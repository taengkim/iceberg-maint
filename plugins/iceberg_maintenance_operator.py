"""
IcebergMaintenanceOperator
==========================
Custom Airflow operator for managing Iceberg tables on S3.

Inherits from SparkKubernetesOperator to submit PySpark maintenance jobs via the
Kubeflow Spark Operator (sparkoperator.k8s.io/v1beta2) running on Kubernetes.

The dt partition to maintain is resolved at execution time:
    dt = data_interval_start - timedelta(days=days_back)

Airflow version : 3.0.6
Spark version   : 4.1.1
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence

import yaml
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class IcebergMaintenanceOperator(SparkKubernetesOperator):
    """
    Airflow operator that submits an Iceberg maintenance PySpark job to the
    Kubeflow Spark Operator on Kubernetes.

    The operator dynamically builds a ``SparkApplication`` CRD manifest based on
    the supplied table information and the DAG's data interval, then delegates
    execution to :class:`SparkKubernetesOperator`.

    Parameters
    ----------
    table_catalog : str
        Iceberg catalog name configured in SparkSession
        (e.g. ``"glue_catalog"``).
    table_database : str
        Database / schema that contains the target table.
    table_name : str
        Name of the Iceberg table to maintain.
    days_back : int
        Number of days to subtract from ``data_interval_start`` when
        computing the ``dt`` partition. Default is ``0`` (current interval).
    maintenance_type : str
        Type of maintenance to perform. Accepted values:

        * ``"all"``                – runs all operations in order
        * ``"rewrite_data_files"`` – compact small files for the partition
        * ``"expire_snapshots"``   – remove old snapshot metadata/data
        * ``"remove_orphan_files"``– delete unreferenced files
        * ``"rewrite_manifests"``  – rewrite manifest files for performance
    spark_image : str
        Docker image used for Spark driver and executor pods.
    spark_main_file : str
        Path to the PySpark entry-point script
        (e.g. ``"s3a://bucket/pyspark/iceberg_maintenance.py"``).
    spark_service_account : str
        Kubernetes ServiceAccount name for Spark driver pod. Default ``"spark"``.
    spark_driver_cores : int
        CPU cores requested for the driver pod. Default ``1``.
    spark_driver_memory : str
        Memory requested for the driver pod (e.g. ``"2g"``). Default ``"1g"``.
    spark_executor_instances : int
        Number of executor pods to launch. Default ``2``.
    spark_executor_cores : int
        CPU cores requested per executor pod. Default ``2``.
    spark_executor_memory : str
        Memory requested per executor pod (e.g. ``"4g"``). Default ``"2g"``.
    spark_conf : dict[str, str] | None
        Extra Spark configuration key-value pairs merged into the
        ``SparkApplication`` spec (e.g. catalog settings, S3 endpoint).
    **kwargs
        Additional keyword arguments forwarded to :class:`SparkKubernetesOperator`
        (e.g. ``namespace``, ``kubernetes_conn_id``, ``api_group``, ``api_version``).
    """

    # Extend parent template_fields so Jinja2 works on these attributes too.
    template_fields: Sequence[str] = (
        *SparkKubernetesOperator.template_fields,
        "table_catalog",
        "table_database",
        "table_name",
        "days_back",
        "maintenance_type",
        "spark_image",
    )

    VALID_MAINTENANCE_TYPES = frozenset(
        {
            "all",
            "expire_snapshots",
            "remove_orphan_files",
            "rewrite_data_files",
            "rewrite_manifests",
        }
    )

    def __init__(
        self,
        *,
        table_catalog: str,
        table_database: str,
        table_name: str,
        days_back: int = 0,
        maintenance_type: str = "all",
        spark_image: str,
        spark_main_file: str,
        spark_service_account: str = "spark",
        spark_driver_cores: int = 1,
        spark_driver_memory: str = "1g",
        spark_executor_instances: int = 2,
        spark_executor_cores: int = 2,
        spark_executor_memory: str = "2g",
        spark_conf: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> None:
        if maintenance_type not in self.VALID_MAINTENANCE_TYPES:
            raise ValueError(
                f"Invalid maintenance_type '{maintenance_type}'. "
                f"Must be one of: {sorted(self.VALID_MAINTENANCE_TYPES)}"
            )

        self.table_catalog = table_catalog
        self.table_database = table_database
        self.table_name = table_name
        self.days_back = days_back
        self.maintenance_type = maintenance_type
        self.spark_image = spark_image
        self.spark_main_file = spark_main_file
        self.spark_service_account = spark_service_account
        self.spark_driver_cores = spark_driver_cores
        self.spark_driver_memory = spark_driver_memory
        self.spark_executor_instances = spark_executor_instances
        self.spark_executor_cores = spark_executor_cores
        self.spark_executor_memory = spark_executor_memory
        self.spark_conf = spark_conf or {}

        # application_file is required by SparkKubernetesOperator.__init__.
        # We supply a placeholder and replace it with the generated YAML in execute().
        super().__init__(application_file="{}", **kwargs)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _k8s_safe_name(value: str, max_len: int = 52) -> str:
        """Return a Kubernetes-safe resource name segment.

        Converts to lowercase, replaces ``_`` and ``.`` with ``-``,
        strips any remaining non-alphanumeric characters, and trims to
        *max_len* characters.
        """
        value = value.lower().replace("_", "-").replace(".", "-")
        value = re.sub(r"[^a-z0-9-]", "", value)
        return value.strip("-")[:max_len]

    def _build_spark_application(self, dt_partition: str) -> str:
        """Build a ``SparkApplication`` YAML manifest string.

        The generated manifest is compatible with
        **Kubeflow Spark Operator** (``sparkoperator.k8s.io/v1beta2``).

        Parameters
        ----------
        dt_partition:
            The ``dt`` partition value (``YYYY-MM-DD``) to pass as an
            argument to the PySpark job.

        Returns
        -------
        str
            YAML string representing the ``SparkApplication`` resource.
        """
        safe_table = self._k8s_safe_name(self.table_name)
        safe_dt = dt_partition.replace("-", "")
        # Full name ≤ 63 chars (K8s label value / pod name constraint)
        app_name = f"iceberg-maint-{safe_table}-{safe_dt}"[:63].rstrip("-")

        # Base Spark config: enable Iceberg extensions
        spark_conf: dict[str, str] = {
            "spark.sql.extensions": (
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
            ),
        }
        spark_conf.update(self.spark_conf)

        manifest: dict[str, Any] = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": app_name,
                "namespace": self.namespace,
                "labels": {
                    "app": "iceberg-maintenance",
                    "table": self._k8s_safe_name(self.table_name, max_len=63),
                    "dt": dt_partition,
                },
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": self.spark_image,
                "imagePullPolicy": "IfNotPresent",
                "mainApplicationFile": self.spark_main_file,
                "sparkVersion": "4.1.1",
                "arguments": [
                    "--catalog", self.table_catalog,
                    "--database", self.table_database,
                    "--table", self.table_name,
                    "--dt-partition", dt_partition,
                    "--maintenance-type", self.maintenance_type,
                ],
                "sparkConf": spark_conf,
                "driver": {
                    "cores": self.spark_driver_cores,
                    "memory": self.spark_driver_memory,
                    "serviceAccount": self.spark_service_account,
                    "labels": {"version": "4.1.1"},
                },
                "executor": {
                    "cores": self.spark_executor_cores,
                    "instances": self.spark_executor_instances,
                    "memory": self.spark_executor_memory,
                    "labels": {"version": "4.1.1"},
                },
                "restartPolicy": {
                    "type": "Never",
                },
            },
        }

        return yaml.dump(manifest, default_flow_style=False, allow_unicode=True)

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def execute(self, context: Context) -> Any:
        """Compute the target dt partition, build the SparkApplication manifest,
        then delegate to :meth:`SparkKubernetesOperator.execute`.
        """
        data_interval_start = context["data_interval_start"]
        dt_partition = (
            data_interval_start - timedelta(days=int(self.days_back))
        ).strftime("%Y-%m-%d")

        self.log.info(
            "Iceberg maintenance — table: %s.%s.%s | dt: %s | type: %s",
            self.table_catalog,
            self.table_database,
            self.table_name,
            dt_partition,
            self.maintenance_type,
        )

        # Override application_file with the dynamically generated manifest.
        self.application_file = self._build_spark_application(dt_partition)
        self.log.debug("SparkApplication manifest:\n%s", self.application_file)

        return super().execute(context)

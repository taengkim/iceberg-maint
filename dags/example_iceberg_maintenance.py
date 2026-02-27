"""
Example DAG: Iceberg Table Maintenance

This DAG demonstrates how to use IcebergMaintenanceOperator to run
PySpark-based maintenance jobs on Iceberg tables stored in S3.

Partition logic:
  dt_partition = data_interval_start - timedelta(days=days_back)

Airflow version : 3.0.6
Spark version   : 4.1.1
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.sdk import DAG

from iceberg_maintenance_operator import IcebergMaintenanceOperator

# ---------------------------------------------------------------------------
# Common Spark resource settings (override per-task as needed)
# ---------------------------------------------------------------------------
SPARK_IMAGE = "your-registry/spark-iceberg:4.1.1"
SPARK_MAIN_FILE = "s3a://your-bucket/pyspark/iceberg_maintenance.py"
SPARK_NAMESPACE = "spark"
K8S_CONN_ID = "kubernetes_default"

SPARK_COMMON = dict(
    spark_image=SPARK_IMAGE,
    spark_main_file=SPARK_MAIN_FILE,
    namespace=SPARK_NAMESPACE,
    kubernetes_conn_id=K8S_CONN_ID,
    spark_driver_cores=1,
    spark_driver_memory="2g",
    spark_executor_instances=2,
    spark_executor_cores=2,
    spark_executor_memory="4g",
    # Optional: extra Spark configurations (e.g., S3 endpoint for MinIO)
    spark_conf={
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": "s3a://your-bucket/warehouse",
        "spark.hadoop.fs.s3a.aws.credentials.provider": (
            "com.amazonaws.auth.WebIdentityTokenCredentialsProvider"
        ),
    },
)

# ---------------------------------------------------------------------------
# Example 1: Single-table daily maintenance (full pipeline)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_iceberg_maintenance_single",
    description="Daily Iceberg maintenance for a single table",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["iceberg", "maintenance", "s3"],
) as single_dag:

    # Maintain the partition that is 7 days before data_interval_start.
    # Useful for handling late-arriving data before the partition is finalized.
    maintain_orders = IcebergMaintenanceOperator(
        task_id="maintain_orders",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="orders",
        days_back=7,          # dt = data_interval_start - 7 days
        maintenance_type="all",  # runs all: rewrite → expire → orphan → manifests
        **SPARK_COMMON,
    )


# ---------------------------------------------------------------------------
# Example 2: Multi-table maintenance with different strategies
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_iceberg_maintenance_multi",
    description="Daily Iceberg maintenance for multiple tables",
    schedule="0 2 * * *",  # every day at 02:00
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=3),
    },
    tags=["iceberg", "maintenance", "s3"],
) as multi_dag:

    # --- orders: full maintenance, 7 days back ---
    orders = IcebergMaintenanceOperator(
        task_id="maintain_orders",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="orders",
        days_back=7,
        maintenance_type="all",
        **SPARK_COMMON,
    )

    # --- events: compaction only, process yesterday's partition ---
    events = IcebergMaintenanceOperator(
        task_id="compact_events",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="events",
        days_back=1,
        maintenance_type="rewrite_data_files",
        spark_executor_instances=4,   # more executors for large table
        spark_executor_memory="8g",
        **{k: v for k, v in SPARK_COMMON.items()
           if k not in ("spark_executor_instances", "spark_executor_memory")},
    )

    # --- user_sessions: snapshot expiry only, no partition offset ---
    user_sessions = IcebergMaintenanceOperator(
        task_id="expire_user_sessions_snapshots",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="user_sessions",
        days_back=0,
        maintenance_type="expire_snapshots",
        **SPARK_COMMON,
    )

    # --- orphan file cleanup runs after both compaction tasks ---
    orders >> events >> user_sessions

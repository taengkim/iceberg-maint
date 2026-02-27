"""
Example DAG: Iceberg Table Maintenance

This DAG demonstrates how to use IcebergMaintenanceOperator to run
PySpark-based maintenance jobs on Iceberg tables stored in S3.

Partition logic:
  partition_value = (data_interval_start - timedelta(days=days_back))
                    .strftime(partition_format)

Supported partition formats:
  "yyyy-mm-dd"  →  2024-03-08
  "yyyymmdd"    →  20240308

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
# Example 1: Single-table daily maintenance
#   - Partition column : "dt"  (default)
#   - Partition format : "yyyy-mm-dd"  →  e.g. 2024-03-08  (default)
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_iceberg_maintenance_single",
    description="Daily Iceberg maintenance for a single table (dt, yyyy-mm-dd)",
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

    # Maintain the partition 7 days before data_interval_start.
    # partition_col="dt", partition_format="yyyy-mm-dd" are defaults,
    # shown explicitly here for clarity.
    maintain_orders = IcebergMaintenanceOperator(
        task_id="maintain_orders",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="orders",
        days_back=7,                       # partition_value = data_interval_start - 7d
        partition_col="dt",                # column name in the Iceberg table
        partition_format="yyyy-mm-dd",     # value format: 2024-03-08
        maintenance_type="all",            # rewrite → expire → orphan → manifests
        **SPARK_COMMON,
    )


# ---------------------------------------------------------------------------
# Example 2: Table using yyyymmdd partition format and custom column name
#   - Partition column : "date_key"
#   - Partition format : "yyyymmdd"  →  e.g. 20240308
# ---------------------------------------------------------------------------
with DAG(
    dag_id="example_iceberg_maintenance_yyyymmdd",
    description="Daily Iceberg maintenance for a table with yyyymmdd partition",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["iceberg", "maintenance", "s3"],
) as yyyymmdd_dag:

    maintain_events = IcebergMaintenanceOperator(
        task_id="maintain_events",
        table_catalog="glue_catalog",
        table_database="raw",
        table_name="events",
        days_back=1,
        partition_col="date_key",          # custom partition column name
        partition_format="yyyymmdd",       # value format: 20240308
        maintenance_type="rewrite_data_files",
        **SPARK_COMMON,
    )


# ---------------------------------------------------------------------------
# Example 3: Multi-table maintenance with mixed formats and strategies
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

    # --- orders: full maintenance, dt column, yyyy-mm-dd, 7 days back ---
    orders = IcebergMaintenanceOperator(
        task_id="maintain_orders",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="orders",
        days_back=7,
        partition_col="dt",
        partition_format="yyyy-mm-dd",
        maintenance_type="all",
        **SPARK_COMMON,
    )

    # --- events: compaction only, date_key column, yyyymmdd, yesterday ---
    events = IcebergMaintenanceOperator(
        task_id="compact_events",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="events",
        days_back=1,
        partition_col="date_key",
        partition_format="yyyymmdd",
        maintenance_type="rewrite_data_files",
        spark_executor_instances=4,        # more executors for large table
        spark_executor_memory="8g",
        **{k: v for k, v in SPARK_COMMON.items()
           if k not in ("spark_executor_instances", "spark_executor_memory")},
    )

    # --- user_sessions: snapshot expiry only, default column/format ---
    user_sessions = IcebergMaintenanceOperator(
        task_id="expire_user_sessions_snapshots",
        table_catalog="glue_catalog",
        table_database="analytics",
        table_name="user_sessions",
        days_back=0,
        partition_col="dt",
        partition_format="yyyy-mm-dd",
        maintenance_type="expire_snapshots",
        **SPARK_COMMON,
    )

    orders >> events >> user_sessions

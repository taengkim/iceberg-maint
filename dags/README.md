# DAGs

This directory contains Airflow DAG definitions for Iceberg table maintenance on S3.

## Overview

Iceberg table maintenance tasks are scheduled as Airflow DAGs. Each DAG uses the
`IcebergMaintenanceOperator` (defined in `plugins/`) which submits a PySpark job
to the Kubeflow Spark Operator on Kubernetes.

## Partition Management via Data Interval

The `dt` partition to be maintained is derived from the DAG's **data interval**:

```
dt_partition = data_interval_start - timedelta(days=days_back)
```

| Parameter | Description |
|-----------|-------------|
| `data_interval_start` | Airflow-managed start of the scheduling window |
| `days_back` | Offset (days) to look back from `data_interval_start` |

**Example:** If the DAG runs for the interval starting `2024-03-15` and `days_back=7`,
the maintained partition will be `dt='2024-03-08'`.

## DAG Files

| File | Description |
|------|-------------|
| `example_iceberg_maintenance.py` | Example DAG showing single and multi-table maintenance |

## Operator Parameters

When calling `IcebergMaintenanceOperator` from a DAG, the required inputs are:

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_catalog` | `str` | Iceberg catalog name (e.g., `glue_catalog`) |
| `table_database` | `str` | Database/schema name |
| `table_name` | `str` | Table name |
| `days_back` | `int` | Days to subtract from `data_interval_start` (default: `0`) |
| `maintenance_type` | `str` | One of `all`, `expire_snapshots`, `remove_orphan_files`, `rewrite_data_files`, `rewrite_manifests` |
| `spark_image` | `str` | Docker image for the Spark driver/executor |
| `spark_main_file` | `str` | S3 path or local path to the PySpark script |

## Example Usage

```python
from iceberg_maintenance_operator import IcebergMaintenanceOperator

maintain_table = IcebergMaintenanceOperator(
    task_id="maintain_orders",
    table_catalog="glue_catalog",
    table_database="analytics",
    table_name="orders",
    days_back=7,
    maintenance_type="all",
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
)
```

## Prerequisites

- Airflow 3.0.6+
- `apache-airflow-providers-cncf-kubernetes` installed
- Kubeflow Spark Operator deployed in Kubernetes
- `IcebergMaintenanceOperator` placed in the `plugins/` directory

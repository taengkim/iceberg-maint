# Plugins

This directory contains custom Airflow operators and hooks.

## IcebergMaintenanceOperator

**File:** `iceberg_maintenance_operator.py`

A custom Airflow operator for running Iceberg table maintenance jobs on S3-backed
tables. It inherits from `SparkKubernetesOperator` and submits a `SparkApplication`
CRD to the **Kubeflow Spark Operator** running on Kubernetes.

### Inheritance Chain

```
BaseOperator
  └── SparkKubernetesOperator  (airflow.providers.cncf.kubernetes)
        └── IcebergMaintenanceOperator
```

### How It Works

1. `execute()` is called by Airflow with the task context.
2. The `dt` partition is computed: `data_interval_start − timedelta(days=days_back)`.
3. A `SparkApplication` YAML manifest is built dynamically with table info and partition args.
4. The YAML is passed to `SparkKubernetesOperator.execute()` which creates the CRD in K8s.
5. The operator watches the `SparkApplication` until it reaches a terminal state.

### SparkApplication CRD (Kubeflow)

The operator generates a `SparkApplication` manifest compatible with
**Kubeflow Spark Operator** (`apiVersion: sparkoperator.k8s.io/v1beta2`).

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_catalog` | `str` | required | Iceberg catalog name |
| `table_database` | `str` | required | Database/schema |
| `table_name` | `str` | required | Table name |
| `days_back` | `int` | `0` | Days to subtract from `data_interval_start` |
| `maintenance_type` | `str` | `"all"` | `all` / `expire_snapshots` / `remove_orphan_files` / `rewrite_data_files` / `rewrite_manifests` |
| `spark_image` | `str` | required | Docker image for Spark pods |
| `spark_main_file` | `str` | required | S3/local path to the PySpark script |
| `spark_service_account` | `str` | `"spark"` | K8s service account for Spark pods |
| `spark_driver_cores` | `int` | `1` | CPU cores for driver pod |
| `spark_driver_memory` | `str` | `"1g"` | Memory for driver pod |
| `spark_executor_instances` | `int` | `2` | Number of executor pods |
| `spark_executor_cores` | `int` | `2` | CPU cores per executor |
| `spark_executor_memory` | `str` | `"2g"` | Memory per executor |
| `spark_conf` | `dict` | `{}` | Extra Spark configuration key-value pairs |

All `SparkKubernetesOperator` parameters (e.g., `namespace`, `kubernetes_conn_id`,
`api_group`, `api_version`) are also accepted via `**kwargs`.

### Template Fields

The following fields support Jinja2 templating:

```
table_catalog, table_database, table_name, days_back, maintenance_type, spark_image
```

(All inherited template fields from `SparkKubernetesOperator` are also supported.)

### Installation

Place this file in Airflow's `plugins/` directory. Airflow automatically discovers
and imports modules from this directory.

### Dependencies

```
apache-airflow>=3.0.6
apache-airflow-providers-cncf-kubernetes
pyyaml
```

### Kubeflow Spark Operator Requirements

- Kubeflow Spark Operator deployed with CRD `SparkApplication` (`sparkoperator.k8s.io/v1beta2`)
- A Kubernetes ServiceAccount (default: `spark`) with RBAC permissions for Spark pods
- S3 access configured via IRSA, Workload Identity, or node-level IAM role

<!-- Language / 언어 -->
<div align="right">
  <a href="#english">English</a> &nbsp;|&nbsp; <a href="#korean">한국어</a>
</div>

---

<a name="english"></a>

# DAGs

This directory contains Airflow DAG definitions for Iceberg table maintenance on S3.

## Overview

Each DAG uses `IcebergMaintenanceOperator` (defined in `plugins/`) which submits
a PySpark job to the **Kubeflow Spark Operator** on Kubernetes.

## Partition Management via Data Interval

The partition value is derived from the DAG's **data interval**:

```
partition_value = (data_interval_start − timedelta(days=days_back))
                  .strftime(partition_format)
```

| Parameter | Description |
|-----------|-------------|
| `data_interval_start` | Airflow-managed start of the scheduling window |
| `days_back` | Days to subtract from `data_interval_start` |
| `partition_col` | Partition column name in the Iceberg table (default: `"dt"`) |
| `partition_format` | Value format — `"yyyy-mm-dd"` or `"yyyymmdd"` (default: `"yyyy-mm-dd"`) |

**Example:** DAG interval starts `2024-03-15`, `days_back=7`, `partition_format="yyyy-mm-dd"`
→ maintained partition: `dt='2024-03-08'`

## DAG Files

| File | Description |
|------|-------------|
| `example_iceberg_maintenance.py` | Example DAGs — single table, yyyymmdd format, and multi-table |

## Operator Parameters

| Parameter | Type | Description |
|-----------|------|-------------|
| `table_catalog` | `str` | Iceberg catalog name (e.g. `glue_catalog`) |
| `table_database` | `str` | Database/schema name |
| `table_name` | `str` | Table name |
| `days_back` | `int` | Days to subtract from `data_interval_start` (default: `0`) |
| `partition_col` | `str` | Partition column name (default: `"dt"`) |
| `partition_format` | `str` | `"yyyy-mm-dd"` or `"yyyymmdd"` (default: `"yyyy-mm-dd"`) |
| `maintenance_type` | `str` | `all` / `expire_snapshots` / `remove_orphan_files` / `rewrite_data_files` / `rewrite_manifests` |
| `spark_image` | `str` | Docker image for Spark driver/executor pods |
| `spark_main_file` | `str` | S3 or local path to the PySpark script |

## Example Usage

```python
from iceberg_maintenance_operator import IcebergMaintenanceOperator

# yyyy-mm-dd format (default)
maintain_orders = IcebergMaintenanceOperator(
    task_id="maintain_orders",
    table_catalog="glue_catalog",
    table_database="analytics",
    table_name="orders",
    days_back=7,
    partition_col="dt",
    partition_format="yyyy-mm-dd",    # → 2024-03-08
    maintenance_type="all",
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
)

# yyyymmdd format with custom column name
maintain_events = IcebergMaintenanceOperator(
    task_id="maintain_events",
    table_catalog="glue_catalog",
    table_database="raw",
    table_name="events",
    days_back=1,
    partition_col="date_key",
    partition_format="yyyymmdd",      # → 20240314
    maintenance_type="rewrite_data_files",
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

---

<a name="korean"></a>

# DAGs

S3 위의 Iceberg 테이블 유지보수를 위한 Airflow DAG 정의가 들어 있는 디렉터리입니다.

## 개요

각 DAG는 `plugins/`의 `IcebergMaintenanceOperator`를 사용하며,
Kubernetes 위의 **Kubeflow Spark Operator**를 통해 PySpark Job을 실행합니다.

## Data Interval 기반 파티션 관리

파티션 값은 DAG의 **Data Interval** 로부터 자동 계산됩니다.

```
partition_value = (data_interval_start − timedelta(days=days_back))
                  .strftime(partition_format)
```

| 파라미터 | 설명 |
|----------|------|
| `data_interval_start` | Airflow가 관리하는 스케줄 윈도우의 시작 시각 |
| `days_back` | `data_interval_start` 에서 뺄 일수 |
| `partition_col` | Iceberg 테이블의 파티션 컬럼명 (기본값: `"dt"`) |
| `partition_format` | 값 포맷 — `"yyyy-mm-dd"` 또는 `"yyyymmdd"` (기본값: `"yyyy-mm-dd"`) |

**예시:** DAG 인터벌 시작 `2024-03-15`, `days_back=7`, `partition_format="yyyy-mm-dd"`
→ 처리 파티션: `dt='2024-03-08'`

## DAG 파일 목록

| 파일 | 설명 |
|------|------|
| `example_iceberg_maintenance.py` | 예제 DAG — 단일 테이블, yyyymmdd 포맷, 멀티 테이블 3가지 |

## 오퍼레이터 파라미터

| 파라미터 | 타입 | 설명 |
|----------|------|------|
| `table_catalog` | `str` | Iceberg 카탈로그명 (예: `glue_catalog`) |
| `table_database` | `str` | 데이터베이스/스키마명 |
| `table_name` | `str` | 테이블명 |
| `days_back` | `int` | `data_interval_start` 기준 몇 일 전 파티션 (기본값: `0`) |
| `partition_col` | `str` | 파티션 컬럼명 (기본값: `"dt"`) |
| `partition_format` | `str` | `"yyyy-mm-dd"` 또는 `"yyyymmdd"` (기본값: `"yyyy-mm-dd"`) |
| `maintenance_type` | `str` | `all` / `expire_snapshots` / `remove_orphan_files` / `rewrite_data_files` / `rewrite_manifests` |
| `spark_image` | `str` | Spark 드라이버/익스큐터 Pod 이미지 |
| `spark_main_file` | `str` | PySpark 스크립트의 S3 또는 로컬 경로 |

## 사용 예시

```python
from iceberg_maintenance_operator import IcebergMaintenanceOperator

# yyyy-mm-dd 포맷 (기본값)
maintain_orders = IcebergMaintenanceOperator(
    task_id="maintain_orders",
    table_catalog="glue_catalog",
    table_database="analytics",
    table_name="orders",
    days_back=7,
    partition_col="dt",
    partition_format="yyyy-mm-dd",    # → 2024-03-08
    maintenance_type="all",
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
)

# yyyymmdd 포맷 + 커스텀 컬럼명
maintain_events = IcebergMaintenanceOperator(
    task_id="maintain_events",
    table_catalog="glue_catalog",
    table_database="raw",
    table_name="events",
    days_back=1,
    partition_col="date_key",
    partition_format="yyyymmdd",      # → 20240314
    maintenance_type="rewrite_data_files",
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
)
```

## 사전 요구사항

- Airflow 3.0.6+
- `apache-airflow-providers-cncf-kubernetes` 패키지 설치
- Kubernetes에 Kubeflow Spark Operator 배포
- `plugins/` 디렉터리에 `IcebergMaintenanceOperator` 배치

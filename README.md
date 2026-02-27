<!-- Language / 언어 -->
<div align="right">
  <a href="#english">English</a> &nbsp;|&nbsp; <a href="#korean">한국어</a>
</div>

---

<a name="english"></a>

# iceberg-maint

A project for automating **Iceberg table maintenance** on S3 using Airflow.
A custom operator triggers PySpark maintenance jobs via the **Kubeflow Spark Operator** on Kubernetes.

## Tech Stack

| Component | Version |
|-----------|---------|
| Apache Airflow | 3.0.6 |
| Apache Spark | 4.1.1 |
| Apache Iceberg | 1.8.0 |
| Kubeflow Spark Operator | `sparkoperator.k8s.io/v1beta2` |

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│  Airflow (3.0.6)                                        │
│                                                         │
│  DAG                                                    │
│   └── IcebergMaintenanceOperator                        │
│         ├── data_interval_start - days_back             │
│         │     → compute partition_value                 │
│         └── Build SparkApplication YAML dynamically    │
│               └── SparkKubernetesOperator.execute()     │
└──────────────────────────┬──────────────────────────────┘
                           │  kubectl apply (CRD)
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Kubernetes                                             │
│                                                         │
│  Kubeflow Spark Operator                                │
│   └── SparkApplication (sparkoperator.k8s.io/v1beta2)  │
│         ├── Driver Pod                                  │
│         └── Executor Pod(s)                             │
│               └── iceberg_maintenance.py                │
│                     ├── rewrite_data_files              │
│                     ├── expire_snapshots                │
│                     ├── remove_orphan_files             │
│                     └── rewrite_manifests               │
└──────────────────────────┬──────────────────────────────┘
                           │  s3a://
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Amazon S3                                              │
│   └── Iceberg Table (data files, metadata, manifests)  │
└─────────────────────────────────────────────────────────┘
```

## Directory Structure

```
iceberg-maint/
├── dags/                          # Airflow DAG definitions
│   ├── README.md
│   └── example_iceberg_maintenance.py
│
├── plugins/                       # Custom Airflow operator
│   ├── README.md
│   └── iceberg_maintenance_operator.py
│
├── pyspark/                       # PySpark maintenance job
│   ├── README.md
│   └── iceberg_maintenance.py
│
└── image/                         # Docker image for Spark pods
    ├── README.md
    ├── Dockerfile
    └── requirements.txt
```

## Partition Management

The partition value is automatically computed from the Airflow **Data Interval**:

```
partition_value = (data_interval_start − timedelta(days=days_back))
                  .strftime(partition_format)
```

### Supported Partition Formats

| `partition_format` | Example output |
|--------------------|----------------|
| `"yyyy-mm-dd"` *(default)* | `2024-03-08` |
| `"yyyymmdd"` | `20240308` |

The partition column name is configured via `partition_col` (default: `"dt"`).

### Examples

| `data_interval_start` | `days_back` | `partition_col` | `partition_format` | Result |
|---|---|---|---|---|
| 2024-03-15 | `0` | `dt` | `yyyy-mm-dd` | `dt='2024-03-15'` |
| 2024-03-15 | `7` | `dt` | `yyyy-mm-dd` | `dt='2024-03-08'` |
| 2024-03-15 | `1` | `date_key` | `yyyymmdd` | `date_key='20240314'` |

## Maintenance Operations

| `maintenance_type` | Description | Order (when `all`) |
|---|---|---|
| `rewrite_data_files` | Compact small files for the target partition | 1 |
| `expire_snapshots` | Remove old snapshot metadata/data | 2 |
| `remove_orphan_files` | Delete unreferenced S3 files | 3 |
| `rewrite_manifests` | Rewrite manifest files for query performance | 4 |
| `all` | Run all four operations in order | — |

## Quick Start

### 1. Build and Push Docker Image

```bash
docker build \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .

docker push your-registry/spark-iceberg:4.1.1
```

### 2. Upload PySpark Script to S3

```bash
aws s3 cp pyspark/iceberg_maintenance.py \
  s3://your-bucket/pyspark/iceberg_maintenance.py
```

### 3. Deploy Airflow Plugin

Place `plugins/iceberg_maintenance_operator.py` in Airflow's `plugins/` directory.
Airflow will auto-import it.

### 4. Deploy DAG

Refer to `dags/example_iceberg_maintenance.py` and place your DAG file
in Airflow's `dags/` directory.

## Operator Parameter Reference

```python
IcebergMaintenanceOperator(
    task_id="...",

    # Table info (required)
    table_catalog="glue_catalog",
    table_database="analytics",
    table_name="orders",

    # Partition settings
    days_back=7,                      # Offset from data_interval_start
    partition_col="dt",               # Partition column name (default: "dt")
    partition_format="yyyy-mm-dd",    # "yyyy-mm-dd" | "yyyymmdd"

    # Maintenance type
    maintenance_type="all",           # all | rewrite_data_files | expire_snapshots
                                      #      | remove_orphan_files | rewrite_manifests

    # Spark image / script (required)
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",

    # Kubernetes settings
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
    spark_service_account="spark",

    # Spark resources
    spark_driver_cores=1,
    spark_driver_memory="2g",
    spark_executor_instances=2,
    spark_executor_cores=2,
    spark_executor_memory="4g",

    # Extra Spark config (catalog, S3 endpoint, etc.)
    spark_conf={
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": "s3a://your-bucket/warehouse",
    },
)
```

## Prerequisites

### Airflow
- Airflow 3.0.6+
- `apache-airflow-providers-cncf-kubernetes` installed

### Kubernetes
- Kubeflow Spark Operator with `SparkApplication` CRD (`sparkoperator.k8s.io/v1beta2`)
- `spark` ServiceAccount with RBAC (see `image/README.md`)
- S3 access via IRSA, Workload Identity, or node IAM role

### Airflow Connection

```bash
airflow connections add kubernetes_default \
  --conn-type kubernetes \
  --conn-extra '{"in_cluster": true}'
```

## Data Flow

```
Airflow Scheduler
  │
  ├─ data_interval_start = 2024-03-15 00:00:00
  │
  └─ IcebergMaintenanceOperator.execute(context)
       │
       ├─ partition_value = "2024-03-08"  (days_back=7, yyyy-mm-dd)
       │                 OR "20240308"    (days_back=7, yyyymmdd)
       │
       ├─ Build SparkApplication YAML
       │    arguments:
       │      --catalog         glue_catalog
       │      --database        analytics
       │      --table           orders
       │      --partition-col   dt
       │      --partition-value 2024-03-08
       │      --maintenance-type all
       │
       └─ Create SparkApplication CRD in K8s → Spark Job runs
            │
            ├─ rewrite_data_files  WHERE dt = '2024-03-08'
            ├─ expire_snapshots    older_than = now() - 7 days
            ├─ remove_orphan_files older_than = now() - 3 days
            └─ rewrite_manifests
```

---

<a name="korean"></a>

# iceberg-maint

S3에 저장된 **Iceberg 테이블**의 유지보수 작업을 Airflow에서 자동화하기 위한 프로젝트입니다.
커스텀 오퍼레이터가 Kubernetes 위의 **Kubeflow Spark Operator**를 통해 PySpark 유지보수 Job을 실행합니다.

## 기술 스택

| 구성 요소 | 버전 |
|-----------|------|
| Apache Airflow | 3.0.6 |
| Apache Spark | 4.1.1 |
| Apache Iceberg | 1.8.0 |
| Kubeflow Spark Operator | `sparkoperator.k8s.io/v1beta2` |

## 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│  Airflow (3.0.6)                                        │
│                                                         │
│  DAG                                                    │
│   └── IcebergMaintenanceOperator                        │
│         ├── data_interval_start - days_back             │
│         │     → partition_value 계산                    │
│         └── SparkApplication YAML 동적 생성             │
│               └── SparkKubernetesOperator.execute()     │
└──────────────────────────┬──────────────────────────────┘
                           │  kubectl apply (CRD)
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Kubernetes                                             │
│                                                         │
│  Kubeflow Spark Operator                                │
│   └── SparkApplication (sparkoperator.k8s.io/v1beta2)  │
│         ├── Driver Pod                                  │
│         └── Executor Pod(s)                             │
│               └── iceberg_maintenance.py                │
│                     ├── rewrite_data_files              │
│                     ├── expire_snapshots                │
│                     ├── remove_orphan_files             │
│                     └── rewrite_manifests               │
└──────────────────────────┬──────────────────────────────┘
                           │  s3a://
                           ▼
┌─────────────────────────────────────────────────────────┐
│  Amazon S3                                              │
│   └── Iceberg Table (data files, metadata, manifests)  │
└─────────────────────────────────────────────────────────┘
```

## 폴더 구조

```
iceberg-maint/
├── dags/                          # Airflow DAG 정의
│   ├── README.md
│   └── example_iceberg_maintenance.py
│
├── plugins/                       # Airflow 커스텀 오퍼레이터
│   ├── README.md
│   └── iceberg_maintenance_operator.py
│
├── pyspark/                       # Spark Job 스크립트
│   ├── README.md
│   └── iceberg_maintenance.py
│
└── image/                         # Docker 이미지 빌드
    ├── README.md
    ├── Dockerfile
    └── requirements.txt
```

## 파티션 관리 방식

파티션 값은 Airflow **Data Interval** 을 기준으로 자동 계산됩니다.

```
partition_value = (data_interval_start − timedelta(days=days_back))
                  .strftime(partition_format)
```

### 지원 파티션 형식

| `partition_format` | 출력 예시 |
|--------------------|-----------|
| `"yyyy-mm-dd"` *(기본값)* | `2024-03-08` |
| `"yyyymmdd"` | `20240308` |

파티션 컬럼명은 `partition_col` 파라미터로 지정합니다 (기본값: `"dt"`).

### 예시

| `data_interval_start` | `days_back` | `partition_col` | `partition_format` | 처리 파티션 |
|---|---|---|---|---|
| 2024-03-15 | `0` | `dt` | `yyyy-mm-dd` | `dt='2024-03-15'` |
| 2024-03-15 | `7` | `dt` | `yyyy-mm-dd` | `dt='2024-03-08'` |
| 2024-03-15 | `1` | `date_key` | `yyyymmdd` | `date_key='20240314'` |

## 유지보수 작업 종류

| `maintenance_type` | 설명 | 실행 순서 (`all` 기준) |
|---|---|---|
| `rewrite_data_files` | 소파일 컴팩션 (지정 파티션 한정) | 1 |
| `expire_snapshots` | 오래된 스냅샷 삭제 | 2 |
| `remove_orphan_files` | 참조되지 않는 S3 파일 삭제 | 3 |
| `rewrite_manifests` | 쿼리 성능 향상을 위한 매니페스트 재작성 | 4 |
| `all` | 위 4가지 순서대로 모두 실행 | — |

## 빠른 시작

### 1. Docker 이미지 빌드 및 푸시

```bash
docker build \
  -t your-registry/spark-iceberg:4.1.1 \
  -f image/Dockerfile \
  .

docker push your-registry/spark-iceberg:4.1.1
```

### 2. PySpark 스크립트를 S3에 업로드

```bash
aws s3 cp pyspark/iceberg_maintenance.py \
  s3://your-bucket/pyspark/iceberg_maintenance.py
```

### 3. Airflow 플러그인 배포

`plugins/iceberg_maintenance_operator.py` 를 Airflow의 `plugins/` 디렉터리에 배치합니다.
Airflow가 자동으로 임포트합니다.

### 4. DAG 배포

`dags/example_iceberg_maintenance.py` 를 참고하여 DAG 파일을 작성하고
Airflow의 `dags/` 디렉터리에 배치합니다.

## 오퍼레이터 파라미터 레퍼런스

```python
IcebergMaintenanceOperator(
    task_id="...",

    # 테이블 정보 (필수)
    table_catalog="glue_catalog",
    table_database="analytics",
    table_name="orders",

    # 파티션 설정
    days_back=7,                      # data_interval_start 기준 몇 일 전 파티션
    partition_col="dt",               # 파티션 컬럼명 (기본값: "dt")
    partition_format="yyyy-mm-dd",    # "yyyy-mm-dd" | "yyyymmdd"

    # 유지보수 작업
    maintenance_type="all",           # all | rewrite_data_files | expire_snapshots
                                      #      | remove_orphan_files | rewrite_manifests

    # Spark 이미지 / 스크립트 (필수)
    spark_image="your-registry/spark-iceberg:4.1.1",
    spark_main_file="s3a://your-bucket/pyspark/iceberg_maintenance.py",

    # Kubernetes 설정
    namespace="spark",
    kubernetes_conn_id="kubernetes_default",
    spark_service_account="spark",

    # Spark 리소스
    spark_driver_cores=1,
    spark_driver_memory="2g",
    spark_executor_instances=2,
    spark_executor_cores=2,
    spark_executor_memory="4g",

    # 추가 Spark 설정 (카탈로그, S3 엔드포인트 등)
    spark_conf={
        "spark.sql.catalog.glue_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.glue_catalog.catalog-impl":
            "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.glue_catalog.warehouse": "s3a://your-bucket/warehouse",
    },
)
```

## 사전 요구사항

### Airflow
- Airflow 3.0.6+
- `apache-airflow-providers-cncf-kubernetes` 패키지 설치

### Kubernetes
- Kubeflow Spark Operator 설치 (`sparkoperator.k8s.io/v1beta2` CRD)
- `spark` ServiceAccount 및 RBAC 설정 (`image/README.md` 참고)
- S3 접근 권한 (IRSA, Workload Identity, 또는 노드 IAM 역할)

### Airflow Connection

```bash
airflow connections add kubernetes_default \
  --conn-type kubernetes \
  --conn-extra '{"in_cluster": true}'
```

## 데이터 흐름 상세

```
Airflow Scheduler
  │
  ├─ data_interval_start = 2024-03-15 00:00:00
  │
  └─ IcebergMaintenanceOperator.execute(context)
       │
       ├─ partition_value = "2024-03-08"  (days_back=7, yyyy-mm-dd)
       │                 OR "20240308"    (days_back=7, yyyymmdd)
       │
       ├─ SparkApplication YAML 생성
       │    arguments:
       │      --catalog         glue_catalog
       │      --database        analytics
       │      --table           orders
       │      --partition-col   dt
       │      --partition-value 2024-03-08
       │      --maintenance-type all
       │
       └─ K8s에 SparkApplication CRD 생성 → Spark Job 실행
            │
            ├─ rewrite_data_files  WHERE dt = '2024-03-08'
            ├─ expire_snapshots    older_than = now() - 7일
            ├─ remove_orphan_files older_than = now() - 3일
            └─ rewrite_manifests
```

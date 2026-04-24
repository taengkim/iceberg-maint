<!-- Language / 언어 -->
<div align="right">
  <a href="#english">English</a> &nbsp;|&nbsp; <a href="#korean">한국어</a>
</div>

---

<a name="english"></a>

# Plugins

This directory contains custom Airflow operators.

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
2. The partition value is computed:
   `(data_interval_start − timedelta(days=days_back)).strftime(partition_format)`
3. A `SparkApplication` YAML manifest is built dynamically with table info and partition args.
4. The YAML is passed to `SparkKubernetesOperator.execute()` which creates the CRD in K8s.
5. The operator watches the `SparkApplication` until it reaches a terminal state.

### Supported Partition Formats

| `partition_format` | strftime format | Example |
|--------------------|-----------------|---------|
| `"yyyy-mm-dd"` *(default)* | `%Y-%m-%d` | `2024-03-08` |
| `"yyyymmdd"` | `%Y%m%d` | `20240308` |
| `"yyyymmddhhMMss"` | `%Y%m%d%H%M%S` | `20240308153045` |

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `table_catalog` | `str` | required | Iceberg catalog name |
| `table_database` | `str` | required | Database/schema |
| `table_name` | `str` | required | Table name |
| `days_back` | `int` | `0` | Days to subtract from `data_interval_start` |
| `partition_col` | `str` | `"dt"` | Partition column name in the Iceberg table |
| `partition_format` | `str` | `"yyyy-mm-dd"` | Partition value format: `"yyyy-mm-dd"`, `"yyyymmdd"`, or `"yyyymmddhhMMss"` |
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

All `SparkKubernetesOperator` parameters (e.g. `namespace`, `kubernetes_conn_id`) are
accepted via `**kwargs`.

### Template Fields

The following fields support Jinja2 templating:

```
table_catalog, table_database, table_name,
days_back, partition_col, partition_format,
maintenance_type, spark_image
```

(All inherited template fields from `SparkKubernetesOperator` are also supported.)

### Installation

Place this file in Airflow's `plugins/` directory. Airflow auto-imports it.

### Dependencies

```
apache-airflow>=3.0.6
apache-airflow-providers-cncf-kubernetes
pyyaml
```

### Kubeflow Spark Operator Requirements

- Kubeflow Spark Operator deployed with CRD `SparkApplication` (`sparkoperator.k8s.io/v1beta2`)
- A Kubernetes ServiceAccount (default: `spark`) with RBAC permissions
- S3 access via IRSA, Workload Identity, or node-level IAM role

---

<a name="korean"></a>

# Plugins

Airflow 커스텀 오퍼레이터가 들어 있는 디렉터리입니다.

## IcebergMaintenanceOperator

**파일:** `iceberg_maintenance_operator.py`

S3 기반 Iceberg 테이블의 유지보수 작업을 실행하는 커스텀 Airflow 오퍼레이터입니다.
`SparkKubernetesOperator`를 상속받아, Kubernetes 위의 **Kubeflow Spark Operator**에
`SparkApplication` CRD를 제출합니다.

### 상속 구조

```
BaseOperator
  └── SparkKubernetesOperator  (airflow.providers.cncf.kubernetes)
        └── IcebergMaintenanceOperator
```

### 동작 방식

1. Airflow가 task context와 함께 `execute()`를 호출합니다.
2. 파티션 값을 계산합니다:
   `(data_interval_start − timedelta(days=days_back)).strftime(partition_format)`
3. 테이블 정보와 파티션 인자를 포함한 `SparkApplication` YAML을 동적으로 생성합니다.
4. YAML을 `SparkKubernetesOperator.execute()`에 전달하여 K8s에 CRD를 생성합니다.
5. `SparkApplication`이 종료 상태에 도달할 때까지 감시합니다.

### 지원 파티션 형식

| `partition_format` | strftime 형식 | 예시 |
|--------------------|---------------|------|
| `"yyyy-mm-dd"` *(기본값)* | `%Y-%m-%d` | `2024-03-08` |
| `"yyyymmdd"` | `%Y%m%d` | `20240308` |
| `"yyyymmddhhMMss"` | `%Y%m%d%H%M%S` | `20240308153045` |

### 파라미터

| 파라미터 | 타입 | 기본값 | 설명 |
|----------|------|--------|------|
| `table_catalog` | `str` | 필수 | Iceberg 카탈로그명 |
| `table_database` | `str` | 필수 | 데이터베이스/스키마 |
| `table_name` | `str` | 필수 | 테이블명 |
| `days_back` | `int` | `0` | `data_interval_start` 에서 뺄 일수 |
| `partition_col` | `str` | `"dt"` | Iceberg 테이블의 파티션 컬럼명 |
| `partition_format` | `str` | `"yyyy-mm-dd"` | 파티션 값 포맷: `"yyyy-mm-dd"`, `"yyyymmdd"`, 또는 `"yyyymmddhhMMss"` |
| `maintenance_type` | `str` | `"all"` | `all` / `expire_snapshots` / `remove_orphan_files` / `rewrite_data_files` / `rewrite_manifests` |
| `spark_image` | `str` | 필수 | Spark Pod용 Docker 이미지 |
| `spark_main_file` | `str` | 필수 | PySpark 스크립트의 S3/로컬 경로 |
| `spark_service_account` | `str` | `"spark"` | Spark Pod의 K8s ServiceAccount |
| `spark_driver_cores` | `int` | `1` | 드라이버 Pod CPU 코어 수 |
| `spark_driver_memory` | `str` | `"1g"` | 드라이버 Pod 메모리 |
| `spark_executor_instances` | `int` | `2` | 익스큐터 Pod 수 |
| `spark_executor_cores` | `int` | `2` | 익스큐터 Pod당 CPU 코어 수 |
| `spark_executor_memory` | `str` | `"2g"` | 익스큐터 Pod당 메모리 |
| `spark_conf` | `dict` | `{}` | 추가 Spark 설정 키-값 |

`SparkKubernetesOperator`의 파라미터 (예: `namespace`, `kubernetes_conn_id`)도
`**kwargs`를 통해 모두 전달 가능합니다.

### 템플릿 필드 (Jinja2 지원)

```
table_catalog, table_database, table_name,
days_back, partition_col, partition_format,
maintenance_type, spark_image
```

(`SparkKubernetesOperator`의 상속 템플릿 필드도 모두 지원합니다.)

### 설치

이 파일을 Airflow의 `plugins/` 디렉터리에 배치하면 Airflow가 자동으로 임포트합니다.

### 의존성

```
apache-airflow>=3.0.6
apache-airflow-providers-cncf-kubernetes
pyyaml
```

### Kubeflow Spark Operator 요구사항

- `SparkApplication` CRD 포함 Kubeflow Spark Operator 배포 (`sparkoperator.k8s.io/v1beta2`)
- RBAC 권한이 설정된 Kubernetes ServiceAccount (기본값: `spark`)
- S3 접근 권한 (IRSA, Workload Identity, 또는 노드 IAM 역할)

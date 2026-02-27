<!-- Language / 언어 -->
<div align="right">
  <a href="#english">English</a> &nbsp;|&nbsp; <a href="#korean">한국어</a>
</div>

---

<a name="english"></a>

# PySpark

This directory contains PySpark scripts executed by the Spark Operator on Kubernetes.

## Scripts

| File | Description |
|------|-------------|
| `iceberg_maintenance.py` | Entry-point for Iceberg table maintenance |

## iceberg_maintenance.py

Performs one or more Iceberg maintenance operations against a specific partition.

### Operations (execution order when `maintenance_type=all`)

| Step | Operation | Description |
|------|-----------|-------------|
| 1 | `rewrite_data_files` | Compact small files within the target partition |
| 2 | `expire_snapshots` | Remove snapshots older than `--snapshot-retention-days` |
| 3 | `remove_orphan_files` | Delete S3 files not referenced by any snapshot |
| 4 | `rewrite_manifests` | Rewrite manifest files for faster query planning |

### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | yes | — | Spark catalog name |
| `--database` | yes | — | Iceberg database/schema |
| `--table` | yes | — | Iceberg table name |
| `--partition-col` | no | `dt` | Partition column name (e.g. `dt`, `date_key`) |
| `--partition-value` | yes | — | Formatted partition value (e.g. `2024-03-08` or `20240308`) |
| `--maintenance-type` | no | `all` | `all` / `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` / `rewrite_manifests` |
| `--snapshot-retention-days` | no | `7` | Expire snapshots older than N days |
| `--snapshot-retain-last` | no | `5` | Always keep at least N most recent snapshots |

> `--partition-value` is pre-formatted by `IcebergMaintenanceOperator` using
> the `partition_format` setting before the job is submitted.

### SparkSession Configuration

The job relies on **externally configured** SparkSession properties injected via
`spark_conf` in the `SparkApplication` manifest.

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.<catalog>.warehouse=s3a://your-bucket/warehouse

# S3 / MinIO (optional)
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
```

### Local Testing

```bash
# yyyy-mm-dd partition
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
  pyspark/iceberg_maintenance.py \
    --catalog local \
    --database mydb \
    --table orders \
    --partition-col dt \
    --partition-value 2024-03-08 \
    --maintenance-type all

# yyyymmdd partition with custom column name
spark-submit \
  ... \
  pyspark/iceberg_maintenance.py \
    --catalog local \
    --database mydb \
    --table events \
    --partition-col date_key \
    --partition-value 20240308 \
    --maintenance-type rewrite_data_files
```

## Deployment

Upload the script to S3 so it can be referenced by the `SparkApplication` manifest:

```bash
aws s3 cp pyspark/iceberg_maintenance.py s3://your-bucket/pyspark/iceberg_maintenance.py
```

Alternatively, bake it into the Docker image (see `image/`).

## Runtime Dependencies

These are provided by the Spark image (see `image/Dockerfile`):

- Apache Spark 4.1.1
- `iceberg-spark-runtime` JAR matching Spark 4.1
- `hadoop-aws` + AWS SDK JARs for S3 access

---

<a name="korean"></a>

# PySpark

Kubernetes의 Spark Operator가 실행하는 PySpark 스크립트가 들어 있는 디렉터리입니다.

## 스크립트 목록

| 파일 | 설명 |
|------|------|
| `iceberg_maintenance.py` | Iceberg 테이블 유지보수 메인 스크립트 |

## iceberg_maintenance.py

지정한 파티션에 대해 하나 이상의 Iceberg 유지보수 작업을 수행합니다.

### 작업 목록 (`maintenance_type=all` 기준 실행 순서)

| 순서 | 작업 | 설명 |
|------|------|------|
| 1 | `rewrite_data_files` | 대상 파티션의 소파일 컴팩션 |
| 2 | `expire_snapshots` | `--snapshot-retention-days` 기준 오래된 스냅샷 삭제 |
| 3 | `remove_orphan_files` | 어떤 스냅샷에도 참조되지 않는 S3 파일 삭제 |
| 4 | `rewrite_manifests` | 빠른 쿼리 플래닝을 위한 매니페스트 파일 재작성 |

### 인자 (Arguments)

| 인자 | 필수 | 기본값 | 설명 |
|------|------|--------|------|
| `--catalog` | 예 | — | Spark 카탈로그명 |
| `--database` | 예 | — | Iceberg 데이터베이스/스키마 |
| `--table` | 예 | — | Iceberg 테이블명 |
| `--partition-col` | 아니오 | `dt` | 파티션 컬럼명 (예: `dt`, `date_key`) |
| `--partition-value` | 예 | — | 포맷된 파티션 값 (예: `2024-03-08` 또는 `20240308`) |
| `--maintenance-type` | 아니오 | `all` | `all` / `rewrite_data_files` / `expire_snapshots` / `remove_orphan_files` / `rewrite_manifests` |
| `--snapshot-retention-days` | 아니오 | `7` | 이 일수보다 오래된 스냅샷 만료 |
| `--snapshot-retain-last` | 아니오 | `5` | 최소 유지할 최신 스냅샷 수 |

> `--partition-value`는 `IcebergMaintenanceOperator`가 Job 제출 전에 `partition_format`
> 설정에 따라 미리 포맷하여 전달합니다.

### SparkSession 설정

`SparkApplication` 매니페스트의 `spark_conf`를 통해 외부에서 주입되는 SparkSession 속성에 의존합니다.

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<catalog>.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.<catalog>.warehouse=s3a://your-bucket/warehouse

# S3 / MinIO (선택)
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.path.style.access=true
```

### 로컬 테스트

```bash
# yyyy-mm-dd 파티션
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
  pyspark/iceberg_maintenance.py \
    --catalog local \
    --database mydb \
    --table orders \
    --partition-col dt \
    --partition-value 2024-03-08 \
    --maintenance-type all

# yyyymmdd 파티션 + 커스텀 컬럼명
spark-submit \
  ... \
  pyspark/iceberg_maintenance.py \
    --catalog local \
    --database mydb \
    --table events \
    --partition-col date_key \
    --partition-value 20240308 \
    --maintenance-type rewrite_data_files
```

## 배포

스크립트를 S3에 업로드하여 `SparkApplication` 매니페스트에서 참조할 수 있게 합니다.

```bash
aws s3 cp pyspark/iceberg_maintenance.py s3://your-bucket/pyspark/iceberg_maintenance.py
```

또는 Docker 이미지에 빌드 시 포함시킵니다 (`image/` 참고).

## 런타임 의존성

Spark 이미지에서 제공됩니다 (`image/Dockerfile` 참고):

- Apache Spark 4.1.1
- Spark 4.1과 호환되는 `iceberg-spark-runtime` JAR
- S3 접근을 위한 `hadoop-aws` + AWS SDK JAR

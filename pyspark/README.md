# PySpark

This directory contains PySpark scripts executed by the Spark Operator on Kubernetes.

## Scripts

| File | Description |
|------|-------------|
| `iceberg_maintenance.py` | Main entry-point for Iceberg table maintenance |

## iceberg_maintenance.py

Performs one or more of the following Iceberg maintenance operations against a
specific `dt` partition and table.

### Operations (in execution order when `maintenance_type=all`)

| Step | Operation | Description |
|------|-----------|-------------|
| 1 | `rewrite_data_files` | Compact small files within the target `dt` partition |
| 2 | `expire_snapshots` | Remove snapshots older than `--snapshot-retention-days` |
| 3 | `remove_orphan_files` | Delete S3 files not referenced by any snapshot |
| 4 | `rewrite_manifests` | Rewrite manifest files for faster query planning |

### Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | yes | — | Spark catalog name |
| `--database` | yes | — | Iceberg database/schema |
| `--table` | yes | — | Iceberg table name |
| `--dt-partition` | yes | — | Partition value (`YYYY-MM-DD`) |
| `--maintenance-type` | no | `all` | One of `all`, `rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`, `rewrite_manifests` |
| `--snapshot-retention-days` | no | `7` | Snapshots older than this (days) are expired |
| `--snapshot-retain-last` | no | `5` | Always keep at least this many snapshots |

### SparkSession Configuration

The job relies on **externally configured** SparkSession properties passed via
`spark_conf` in the `SparkApplication` manifest (managed by `IcebergMaintenanceOperator`).
Key properties expected:

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
spark-submit \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=/tmp/iceberg-warehouse \
  pyspark/iceberg_maintenance.py \
    --catalog local \
    --database mydb \
    --table orders \
    --dt-partition 2024-03-08 \
    --maintenance-type all
```

## Deployment

The script should be uploaded to S3 so it can be referenced by the
`SparkApplication` manifest:

```bash
aws s3 cp pyspark/iceberg_maintenance.py s3://your-bucket/pyspark/iceberg_maintenance.py
```

Alternatively, bake it into the Docker image (see `image/`).

## Dependencies (Runtime)

These are provided by the Spark image (see `image/Dockerfile`):

- Apache Spark 4.1.1
- `iceberg-spark-runtime` JAR matching Spark 4.1.1
- `hadoop-aws` + AWS SDK JARs for S3 access

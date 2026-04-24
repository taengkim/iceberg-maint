"""
Iceberg Table Maintenance — PySpark Job
========================================
Performs maintenance operations on an Iceberg table stored in S3.

Operations (when --maintenance-type=all, executed in order):
  1. rewrite_data_files  – compact small files within the target partition
  2. expire_snapshots    – remove snapshots older than retention threshold
  3. remove_orphan_files – delete S3 files not referenced by any snapshot
  4. rewrite_manifests   – rewrite manifest files for faster planning

The partition column name (--partition-col) and formatted partition value
(--partition-value) are injected by IcebergMaintenanceOperator.

Supported partition value formats (resolved by the operator before submission):
  yyyy-mm-dd      e.g. 2024-03-08
  yyyymmdd        e.g. 20240308
  yyyymmddhhMMss  e.g. 20240308153045

Spark version : 4.1.1
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Iceberg table maintenance PySpark job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--catalog", required=True, help="Spark/Iceberg catalog name")
    parser.add_argument("--database", required=True, help="Iceberg database/schema")
    parser.add_argument("--table", required=True, help="Iceberg table name")
    parser.add_argument(
        "--partition-col",
        dest="partition_col",
        default="dt",
        help="Partition column name in the Iceberg table (e.g. dt, date_key)",
    )
    parser.add_argument(
        "--partition-value",
        required=True,
        dest="partition_value",
        help="Formatted partition value (e.g. 2024-03-08 or 20240308)",
    )
    parser.add_argument(
        "--maintenance-type",
        dest="maintenance_type",
        default="all",
        choices=[
            "all",
            "rewrite_data_files",
            "expire_snapshots",
            "remove_orphan_files",
            "rewrite_manifests",
        ],
        help="Maintenance operation to execute",
    )
    parser.add_argument(
        "--snapshot-retention-days",
        dest="snapshot_retention_days",
        type=int,
        default=7,
        help="Expire snapshots older than this many days",
    )
    parser.add_argument(
        "--snapshot-retain-last",
        dest="snapshot_retain_last",
        type=int,
        default=5,
        help="Always retain at least this many recent snapshots",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# SparkSession
# ---------------------------------------------------------------------------

def get_spark() -> SparkSession:
    """Return (or create) the SparkSession.

    Catalog and S3 configuration are expected to be supplied externally via
    ``spark_conf`` in the SparkApplication manifest (managed by the operator).
    """
    return SparkSession.builder.appName("iceberg-maintenance").getOrCreate()


# ---------------------------------------------------------------------------
# Maintenance operations
# ---------------------------------------------------------------------------

def rewrite_data_files(
    spark: SparkSession,
    full_table: str,
    catalog: str,
    partition_col: str,
    partition_value: str,
) -> None:
    """Compact small data files within the target partition.

    Uses Iceberg's ``rewrite_data_files`` stored procedure with a partition
    filter to limit work to the requested partition value.

    Parameters
    ----------
    partition_col:
        Name of the partition column (e.g. ``"dt"``, ``"date_key"``).
    partition_value:
        Formatted partition value (e.g. ``"2024-03-08"`` or ``"20240308"``).
    """
    print(
        f"[rewrite_data_files] Compacting {partition_col}='{partition_value}'"
        f" in {full_table}"
    )
    result = spark.sql(
        f"""
        CALL {catalog}.system.rewrite_data_files(
            table  => '{full_table}',
            where  => '{partition_col} = \\'{partition_value}\\'',
            options => map(
                'target-file-size-bytes', '134217728',
                'min-input-files',        '2'
            )
        )
        """
    )
    result.show(truncate=False)
    print("[rewrite_data_files] Done.")


def expire_snapshots(
    spark: SparkSession,
    full_table: str,
    catalog: str,
    retention_days: int,
    retain_last: int,
) -> None:
    """Remove snapshots (and their data files) older than the retention window.

    Parameters
    ----------
    retention_days:
        Snapshots with ``committed_at`` older than this many days are expired.
    retain_last:
        Always keep at least this many recent snapshots regardless of age.
    """
    older_than = (
        datetime.now(tz=timezone.utc) - timedelta(days=retention_days)
    ).strftime("%Y-%m-%d %H:%M:%S")

    print(
        f"[expire_snapshots] Expiring snapshots older than {older_than} "
        f"(retain_last={retain_last}) for {full_table}"
    )
    result = spark.sql(
        f"""
        CALL {catalog}.system.expire_snapshots(
            table        => '{full_table}',
            older_than   => TIMESTAMP '{older_than}',
            retain_last  => {retain_last}
        )
        """
    )
    result.show(truncate=False)
    print("[expire_snapshots] Done.")


def remove_orphan_files(
    spark: SparkSession,
    full_table: str,
    catalog: str,
) -> None:
    """Delete S3 files that exist on storage but are not referenced by any snapshot.

    Safe to run after ``expire_snapshots`` to reclaim storage from previously
    expired data.
    """
    # Orphan threshold: files older than 3 days are considered orphaned.
    # This prevents deleting files that are still being written by concurrent jobs.
    older_than = (
        datetime.now(tz=timezone.utc) - timedelta(days=3)
    ).strftime("%Y-%m-%d %H:%M:%S")

    print(f"[remove_orphan_files] Scanning {full_table} (older_than={older_than})")
    result = spark.sql(
        f"""
        CALL {catalog}.system.remove_orphan_files(
            table      => '{full_table}',
            older_than => TIMESTAMP '{older_than}'
        )
        """
    )
    result.show(truncate=False)
    print("[remove_orphan_files] Done.")


def rewrite_manifests(
    spark: SparkSession,
    full_table: str,
    catalog: str,
) -> None:
    """Rewrite Iceberg manifest files to improve query planning performance."""
    print(f"[rewrite_manifests] Rewriting manifests for {full_table}")
    result = spark.sql(
        f"""
        CALL {catalog}.system.rewrite_manifests(
            table => '{full_table}'
        )
        """
    )
    result.show(truncate=False)
    print("[rewrite_manifests] Done.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)

    catalog = args.catalog
    database = args.database
    table = args.table
    partition_col = args.partition_col
    partition_value = args.partition_value
    maintenance_type = args.maintenance_type
    full_table = f"{catalog}.{database}.{table}"

    print("=" * 60)
    print("Iceberg Maintenance Job")
    print(f"  table           : {full_table}")
    print(f"  partition_col   : {partition_col}")
    print(f"  partition_value : {partition_value}")
    print(f"  maintenance_type: {maintenance_type}")
    print("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---- 1. Compact the target partition first ----
        if maintenance_type in ("all", "rewrite_data_files"):
            rewrite_data_files(spark, full_table, catalog, partition_col, partition_value)

        # ---- 2. Expire old snapshots ----
        if maintenance_type in ("all", "expire_snapshots"):
            expire_snapshots(
                spark,
                full_table,
                catalog,
                retention_days=args.snapshot_retention_days,
                retain_last=args.snapshot_retain_last,
            )

        # ---- 3. Remove orphan files (after snapshots are expired) ----
        if maintenance_type in ("all", "remove_orphan_files"):
            remove_orphan_files(spark, full_table, catalog)

        # ---- 4. Rewrite manifests ----
        if maintenance_type in ("all", "rewrite_manifests"):
            rewrite_manifests(spark, full_table, catalog)

    except Exception as exc:
        print(f"[ERROR] Maintenance failed: {exc}", file=sys.stderr)
        raise
    finally:
        spark.stop()

    print("=" * 60)
    print("Iceberg maintenance completed successfully.")
    print("=" * 60)


if __name__ == "__main__":
    main()

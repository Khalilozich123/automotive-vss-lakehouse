"""Iceberg table maintenance: compaction, snapshot expiry and orphan-file cleanup.

Streaming appends and per-batch MERGEs leave many small files and a long snapshot history.
Left alone, both make reads slower and storage larger. Run daily:

    uv run python -m covesa_lakehouse.ops.maintenance                 # every lakehouse table
    uv run python -m covesa_lakehouse.ops.maintenance --table silver.telemetry_signals
"""

from __future__ import annotations

import argparse
import sys
from datetime import UTC, datetime, timedelta

from pyspark.sql import SparkSession

from covesa_lakehouse.common.spark_session import CATALOG_NAME, LakehouseConfig, get_spark

NAMESPACES = ("bronze", "silver", "gold", "ops")
SNAPSHOT_RETENTION = timedelta(days=7)
RETAIN_LAST = 10
TARGET_FILE_BYTES = 128 * 1024 * 1024


def tables(spark: SparkSession) -> list[str]:
    found = []
    for ns in NAMESPACES:
        if spark.sql(f"SHOW NAMESPACES IN {CATALOG_NAME} LIKE '{ns}'").count():
            found += [
                f"{ns}.{r.tableName}"
                for r in spark.sql(f"SHOW TABLES IN {CATALOG_NAME}.{ns}").collect()
            ]
    return found


def maintain(spark: SparkSession, table: str) -> dict[str, object]:
    cutoff = (datetime.now(UTC) - SNAPSHOT_RETENTION).strftime("%Y-%m-%d %H:%M:%S")
    files_before = spark.table(f"{CATALOG_NAME}.{table}.files").count()
    rewrite = spark.sql(
        f"CALL {CATALOG_NAME}.system.rewrite_data_files(table => '{table}', "
        f"options => map('min-input-files', '5', 'target-file-size-bytes', '{TARGET_FILE_BYTES}'))"
    ).first()
    expire = spark.sql(
        f"CALL {CATALOG_NAME}.system.expire_snapshots(table => '{table}', "
        f"older_than => TIMESTAMP '{cutoff}', retain_last => {RETAIN_LAST})"
    ).first()
    files_after = spark.table(f"{CATALOG_NAME}.{table}.files").count()
    return {
        "table": table,
        "data_files": f"{files_before} -> {files_after}",
        "rewritten": rewrite.rewritten_data_files_count if rewrite else 0,
        "deleted_files": expire.deleted_data_files_count if expire else 0,
    }


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--table", action="append", help="namespace.table (default: all tables)")
    args = p.parse_args(argv)
    spark = get_spark(LakehouseConfig.from_env(app_name="iceberg-maintenance"))
    for table in args.table or tables(spark):
        print(maintain(spark, table), file=sys.stderr)
    spark.stop()


if __name__ == "__main__":
    main()

"""Bronze: generic, schema-agnostic, append-only ingestion of the raw landing zone.

One job for every platform and every encoding. It never parses sensor values. It only:

* takes ``platform_id`` and the VIN from the MQTT topic (corrupt payloads cannot be parsed, but
  must still be attributed), falling back to the envelope for plain envelope-per-line files;
* pseudonymizes the VIN (keyed HMAC-SHA256) in its column *and inside the stored payload*;
* lifts the envelope metadata Silver needs for routing and dedup (``hardware_rev``, ``msg_id``)
  when the payload is valid JSON, and leaves them null otherwise;
* stamps lineage: ``ingestion_id``, ``ingested_at``, ``received_at``, source system and file.

Exactly-once into Bronze: Structured Streaming's file source records processed files in the
checkpoint, and the Iceberg sink commits each micro-batch atomically and idempotently. Re-running
the job therefore only picks up new files.

    uv run python -m covesa_lakehouse.bronze.ingest_raw                 # drain new files, stop
    uv run python -m covesa_lakehouse.bronze.ingest_raw --continuous    # keep streaming
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_col, key_from_env
from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark

BRONZE_TABLE = "lakehouse.bronze.telemetry_raw"
LANDING_URI = "s3a://landing/telemetry/"
CHECKPOINT_URI = "s3a://checkpoints/bronze/telemetry_raw"

LINE_SCHEMA = "topic STRING, payload STRING, received_at STRING"
TOPIC_PATTERN = r"^vehicles/([^/]+)/([^/]+)/telemetry$"
SOURCE_PATTERN = r"/telemetry/([^/]+)/"

DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    ingestion_id    STRING    COMMENT 'Unique id of this Bronze row (UUID)',
    vin             STRING    COMMENT 'HMAC-SHA256 pseudonym of the VIN (null if unattributable)',
    vin_key_id      STRING    COMMENT 'Id of the pseudonymization key that produced vin',
    platform_id     STRING    COMMENT 'Source platform; routes the row to its contract in Silver',
    hardware_rev    STRING    COMMENT 'TCU/ECU hardware and firmware revision (from envelope)',
    msg_id          STRING    COMMENT 'Envelope message id (from envelope); dedup key in Silver',
    received_at     TIMESTAMP COMMENT 'When the landing writer received the message (UTC)',
    ingested_at     TIMESTAMP COMMENT 'When this row was committed to Bronze (UTC)',
    source_system   STRING    COMMENT 'Landing source: mqtt | batch | ...',
    source_file     STRING    COMMENT 'Landing object the row was read from (lineage)',
    payload_sha256  STRING    COMMENT 'SHA-256 of raw_payload, for audits and replay checks',
    raw_payload     STRING    COMMENT 'Verbatim payload, VIN replaced by its pseudonym'
)
USING iceberg
PARTITIONED BY (days(ingested_at))
COMMENT 'Bronze: every landed telemetry message, append-only, schema-agnostic'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '50'
)
"""


def _nullif_empty(column: Column) -> Column:
    return F.when(column == "", None).otherwise(column)


def to_bronze(lines: DataFrame, key: bytes, key_id: str) -> DataFrame:
    """Map landing lines (``value``, ``source_file``) to Bronze rows. One row per line, always."""
    line = F.from_json(F.col("value"), LINE_SCHEMA)
    wrapped = line.isNotNull() & line["payload"].isNotNull()
    payload = F.when(wrapped, line["payload"]).otherwise(F.col("value"))
    topic = F.when(wrapped, line["topic"])

    vin_raw = F.coalesce(
        _nullif_empty(F.regexp_extract(topic, TOPIC_PATTERN, 2)),
        F.get_json_object(payload, "$.vin"),
    )
    platform_id = F.coalesce(
        _nullif_empty(F.regexp_extract(topic, TOPIC_PATTERN, 1)),
        F.get_json_object(payload, "$.platform_id"),
    )

    staged = lines.select(
        F.col("source_file"),
        payload.alias("payload"),
        vin_raw.alias("vin_raw"),
        platform_id.alias("platform_id"),
        F.when(wrapped, line["received_at"]).alias("received_at_text"),
    ).withColumn("vin", hmac_sha256_col(key, F.col("vin_raw")))

    raw_payload = F.when(
        F.col("vin_raw").isNotNull(), F.expr("replace(payload, vin_raw, vin)")
    ).otherwise(F.col("payload"))

    return staged.select(
        F.expr("uuid()").alias("ingestion_id"),
        F.col("vin"),
        F.when(F.col("vin").isNotNull(), F.lit(key_id)).alias("vin_key_id"),
        F.col("platform_id"),
        F.get_json_object("payload", "$.hardware_rev").alias("hardware_rev"),
        F.get_json_object("payload", "$.msg_id").alias("msg_id"),
        F.to_timestamp("received_at_text").alias("received_at"),
        F.current_timestamp().alias("ingested_at"),
        F.coalesce(
            _nullif_empty(F.regexp_extract("source_file", SOURCE_PATTERN, 1)), F.lit("unknown")
        ).alias("source_system"),
        F.col("source_file"),
        F.sha2(raw_payload, 256).alias("payload_sha256"),
        raw_payload.alias("raw_payload"),
    )


def ensure_table(spark: SparkSession, table: str) -> None:
    namespace = table.rsplit(".", 1)[0]
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")
    spark.sql(DDL.format(table=table))


def ensure_dir(spark: SparkSession, uri: str) -> None:
    """The streaming file source refuses to start on a prefix that does not exist yet."""
    jvm = spark.sparkContext._jvm
    assert jvm is not None
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
    if not fs.exists(path):
        fs.mkdirs(path)


def run(
    spark: SparkSession,
    landing: str = LANDING_URI,
    table: str = BRONZE_TABLE,
    checkpoint: str = CHECKPOINT_URI,
    continuous: bool = False,
    interval: str = "30 seconds",
    max_files_per_trigger: int = 500,
) -> int:
    """Run the ingestion; return the number of rows appended (for ``availableNow`` runs)."""
    key, key_id = key_from_env()
    ensure_table(spark, table)
    ensure_dir(spark, landing)

    lines = (
        spark.readStream.format("text")
        .option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.jsonl*")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(landing)
        .select("value", F.col("_metadata.file_path").alias("source_file"))
    )
    writer = (
        to_bronze(lines, key, key_id)
        .writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .queryName("bronze_telemetry_raw")
    )
    writer = (
        writer.trigger(processingTime=interval) if continuous else writer.trigger(availableNow=True)
    )
    query = writer.toTable(table)
    query.awaitTermination()
    return sum(int(p.get("numInputRows", 0)) for p in query.recentProgress)


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--landing", default=os.environ.get("BRONZE_LANDING_URI", LANDING_URI))
    p.add_argument("--table", default=BRONZE_TABLE)
    p.add_argument("--checkpoint", default=os.environ.get("BRONZE_CHECKPOINT_URI", CHECKPOINT_URI))
    p.add_argument("--continuous", action="store_true", help="keep running (default: drain & stop)")
    p.add_argument("--interval", default="30 seconds", help="micro-batch interval when continuous")
    args = p.parse_args(argv)

    spark = get_spark(LakehouseConfig.from_env(app_name="bronze-ingest-raw"))
    rows = run(spark, args.landing, args.table, args.checkpoint, args.continuous, args.interval)
    total = spark.table(args.table).count()
    print(f"appended {rows:,} rows to {args.table} (now {total:,} rows)", file=sys.stderr)
    spark.stop()


if __name__ == "__main__":
    main()

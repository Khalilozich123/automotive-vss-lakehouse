"""Silver: metadata-driven conformance of Bronze telemetry to COVESA VSS, with a Dead Letter Queue.

There is no per-platform code. At every micro-batch the contract directory is (re)loaded and
flattened into a small *spec table* (one row per platform signal, with its scaling, bounds and
pre-computed bit layout). Decoding is then one generic plan for all platforms:

* JSON platforms:      explode ``signals`` -> join specs on (platform_id, signal_name)
* CAN-frame platforms: explode ``frames``  -> join specs on (platform_id, frame_id), bit-extract

followed by ``raw * scale + offset`` and the plausibility check. Adding a platform adds rows to
the spec table, not code, and takes effect at the next micro-batch.

Outputs (Iceberg, written with MERGE so retries and duplicate deliveries are harmless):

* ``silver.telemetry_signals``    one row per (message, VSS signal); deduplicated by msg_id
* ``silver.telemetry_quarantine`` the Dead Letter Queue, with error codes (see data_quality.py)
* ``silver.dtc_events``           one row per (message, active DTC)

    uv run python -m covesa_lakehouse.silver.conformance_engine               # drain, then stop
    uv run python -m covesa_lakehouse.silver.conformance_engine --continuous  # keep streaming
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery

from covesa_lakehouse.common.config_loader import (
    DEFAULT_CONTRACTS_DIR,
    SPEC_SCHEMA,
    load_contract_dir,
    spec_rows,
)
from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark
from covesa_lakehouse.silver import can_decoder as can
from covesa_lakehouse.silver import data_quality as dq

BRONZE_TABLE = "lakehouse.bronze.telemetry_raw"
CHECKPOINT_URI = "s3a://checkpoints/silver/conformance"

ENVELOPE_SCHEMA = (
    "msg_id STRING, vin STRING, platform_id STRING, hardware_rev STRING, encoding STRING,"
    " ts STRING, seq BIGINT, signals MAP<STRING, STRING>,"
    " frames ARRAY<STRUCT<id: BIGINT, ext: BOOLEAN, data: STRING>>, dtcs ARRAY<STRING>"
)


@dataclass(frozen=True)
class SilverTables:
    signals: str = "lakehouse.silver.telemetry_signals"
    quarantine: str = "lakehouse.silver.telemetry_quarantine"
    dtc_events: str = "lakehouse.silver.dtc_events"


DDL = {
    "signals": """
CREATE TABLE IF NOT EXISTS {table} (
    vin              STRING    COMMENT 'Pseudonymized VIN',
    platform_id      STRING,
    event_ts         TIMESTAMP COMMENT 'Event time reported by the vehicle (UTC)',
    vss_path         STRING    COMMENT 'COVESA VSS leaf, e.g. Vehicle.Speed',
    value            DOUBLE    COMMENT 'Physical value in the VSS unit',
    unit             STRING    COMMENT 'VSS unit of value',
    signal_name      STRING    COMMENT 'Platform-native signal it was decoded from',
    raw_value        BIGINT    COMMENT 'Raw integer before scaling (lineage)',
    msg_id           STRING,
    hardware_rev     STRING,
    contract_version STRING,
    ingestion_id     STRING    COMMENT 'Bronze row this reading came from',
    ingested_at      TIMESTAMP,
    conformed_at     TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_ts))
COMMENT 'Silver: telemetry conformed to COVESA VSS, one row per signal reading'
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
               'write.metadata.delete-after-commit.enabled'='true')
""",
    "quarantine": """
CREATE TABLE IF NOT EXISTS {table} (
    quarantine_key   STRING    COMMENT 'Stable id of the defect (dedup across retries/duplicates)',
    error_code       STRING,
    error_detail     STRING,
    ingestion_id     STRING,
    msg_id           STRING,
    vin              STRING,
    platform_id      STRING,
    event_ts         TIMESTAMP,
    signal_name      STRING    COMMENT 'Null for message-level errors',
    vss_path         STRING,
    raw_value        BIGINT,
    decoded_value    DOUBLE,
    min_val          DOUBLE,
    max_val          DOUBLE,
    contract_version STRING,
    raw_payload      STRING    COMMENT 'Kept for message-level errors, for triage and replay',
    source_file      STRING,
    quarantined_at   TIMESTAMP
) USING iceberg
PARTITIONED BY (days(quarantined_at), error_code)
COMMENT 'Silver Dead Letter Queue: rejected messages and signal readings'
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
               'write.metadata.delete-after-commit.enabled'='true')
""",
    "dtc_events": """
CREATE TABLE IF NOT EXISTS {table} (
    vin          STRING,
    platform_id  STRING,
    event_ts     TIMESTAMP,
    dtc_code     STRING COMMENT 'SAE J2012 code, e.g. P0A80',
    msg_id       STRING,
    ingestion_id STRING,
    conformed_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(event_ts))
COMMENT 'Silver: active Diagnostic Trouble Codes reported per message'
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd',
               'write.metadata.delete-after-commit.enabled'='true')
""",
}


@dataclass
class Conformed:
    signals: DataFrame
    quarantine: DataFrame
    dtc_events: DataFrame


def _quarantine_cols(
    *,
    code: Column,
    detail: Column,
    signal_name: Column,
    vss_path: Column,
    raw_value: Column,
    decoded_value: Column,
    min_val: Column,
    max_val: Column,
    raw_payload: Column,
    now: Column,
) -> list[Column]:
    return [
        dq.quarantine_key(
            F.coalesce(F.col("msg_id"), F.col("payload_sha256")), signal_name, code
        ).alias("quarantine_key"),
        code.alias("error_code"),
        detail.alias("error_detail"),
        F.col("ingestion_id"),
        F.col("msg_id"),
        F.col("vin"),
        F.col("platform_id"),
        F.col("event_ts"),
        signal_name.cast("string").alias("signal_name"),
        vss_path.cast("string").alias("vss_path"),
        raw_value.cast("bigint").alias("raw_value"),
        decoded_value.cast("double").alias("decoded_value"),
        min_val.cast("double").alias("min_val"),
        max_val.cast("double").alias("max_val"),
        F.col("contract_version"),
        raw_payload.cast("string").alias("raw_payload"),
        F.col("source_file"),
        now.alias("quarantined_at"),
    ]


def conform(bronze: DataFrame, specs: DataFrame, now: Column | None = None) -> Conformed:
    """Pure transformation: Bronze rows + contract spec table -> Silver, DLQ and DTC frames."""
    now = now if now is not None else F.current_timestamp()
    envelope = F.from_json("raw_payload", ENVELOPE_SCHEMA)
    platforms = specs.select(
        "platform_id", "contract_version", F.col("encoding").alias("contract_encoding")
    ).distinct()

    messages = (
        bronze.select(
            "ingestion_id",
            "vin",
            "platform_id",
            "hardware_rev",
            "ingested_at",
            "source_file",
            "payload_sha256",
            "raw_payload",
            envelope.alias("env"),
        )
        .withColumn("msg_id", F.col("env.msg_id"))
        .withColumn("event_ts", F.to_timestamp("env.ts"))
        .join(F.broadcast(platforms), "platform_id", "left")
        .withColumn(
            "message_error",
            dq.message_error(
                F.col("raw_payload"),
                F.col("env"),
                F.col("contract_version"),
                F.col("contract_encoding"),
                F.col("event_ts"),
            ),
        )
    )

    rejected = messages.filter(F.col("message_error").isNotNull())
    message_quarantine = rejected.select(
        *_quarantine_cols(
            code=F.col("message_error"),
            detail=dq.message_error_detail(
                F.col("message_error"),
                F.col("env"),
                F.col("platform_id"),
                F.col("contract_encoding"),
            ),
            signal_name=F.lit(None),
            vss_path=F.lit(None),
            raw_value=F.lit(None),
            decoded_value=F.lit(None),
            min_val=F.lit(None),
            max_val=F.lit(None),
            raw_payload=F.col("raw_payload"),
            now=now,
        )
    )

    accepted = messages.filter(F.col("message_error").isNull()).drop("raw_payload")
    reading_cols = [
        "ingestion_id", "vin", "platform_id", "hardware_rev", "ingested_at", "source_file",
        "payload_sha256", "msg_id", "event_ts", "contract_version",
    ]  # fmt: skip
    spec_cols = ["signal_name", "vss_path", "unit", "scale", "offset", "min_val", "max_val"]

    json_readings = (
        accepted.filter(F.col("contract_encoding") == "json")
        .select(*reading_cols, F.explode("env.signals").alias("signal_name", "raw_text"))
        .join(F.broadcast(specs.drop("contract_version")), ["platform_id", "signal_name"])
        .select(
            *reading_cols,
            *spec_cols,
            F.when(dq.is_integer_text(F.col("raw_text")), F.col("raw_text").cast("bigint")).alias(
                "raw_value"
            ),
            F.when(~dq.is_integer_text(F.col("raw_text")), dq.INVALID_RAW_VALUE).alias(
                "read_error"
            ),
            F.when(
                ~dq.is_integer_text(F.col("raw_text")),
                F.concat(F.lit("non-integer raw value: "), F.coalesce("raw_text", F.lit("null"))),
            ).alias("read_detail"),
        )
    )

    frame_ok = can.frame_is_well_formed(F.col("data"), F.col("dlc"))
    can_readings = (
        accepted.filter(F.col("contract_encoding") == "can_frame")
        .select(*reading_cols, F.explode("env.frames").alias("frame"))
        .select("*", F.col("frame.id").alias("frame_id"), F.col("frame.data").alias("data"))
        .join(F.broadcast(specs.drop("contract_version")), ["platform_id", "frame_id"])
        .withColumn("frame_ok", frame_ok)
        .filter(  # multiplexed signals are only present when the selector matches
            ~F.col("frame_ok")
            | F.col("mux_value").isNull()
            | (can.extract_unsigned("data", prefix="mux_") == F.col("mux_value"))
        )
        .select(
            *reading_cols,
            *spec_cols,
            F.when(
                F.col("frame_ok"),
                can.to_signed(can.extract_unsigned("data"), F.col("length"), F.col("is_signed")),
            ).alias("raw_value"),
            F.when(~F.col("frame_ok"), dq.MALFORMED_FRAME).alias("read_error"),
            F.when(
                ~F.col("frame_ok"),
                F.format_string(
                    "frame %d: '%s' is not %d bytes of hex",
                    F.col("frame_id"),
                    F.col("data"),
                    F.col("dlc"),
                ),
            ).alias("read_detail"),
        )
    )

    readings = json_readings.unionByName(can_readings).withColumn(
        "value", can.physical(F.col("raw_value"), F.col("scale"), F.col("offset"))
    )
    plausible = dq.in_range(F.col("value"), F.col("min_val"), F.col("max_val"))
    readings = readings.withColumn(
        "error_code",
        F.coalesce(F.col("read_error"), F.when(~plausible, dq.PHYSICAL_RANGE_VIOLATION)),
    ).withColumn(
        "error_detail",
        F.coalesce(
            F.col("read_detail"),
            dq.range_detail(
                F.col("vss_path"), F.col("value"), F.col("min_val"), F.col("max_val"), F.col("unit")
            ),
        ),
    )

    signals = readings.filter(F.col("error_code").isNull()).select(
        "vin", "platform_id", "event_ts", "vss_path", "value", "unit", "signal_name", "raw_value",
        "msg_id", "hardware_rev", "contract_version", "ingestion_id", "ingested_at",
        now.alias("conformed_at"),
    )  # fmt: skip

    signal_quarantine = readings.filter(F.col("error_code").isNotNull()).select(
        *_quarantine_cols(
            code=F.col("error_code"),
            detail=F.col("error_detail"),
            signal_name=F.col("signal_name"),
            vss_path=F.col("vss_path"),
            raw_value=F.col("raw_value"),
            decoded_value=F.col("value"),
            min_val=F.col("min_val"),
            max_val=F.col("max_val"),
            raw_payload=F.lit(None),
            now=now,
        )
    )

    dtc_events = accepted.select(
        "vin",
        "platform_id",
        "event_ts",
        F.explode("env.dtcs").alias("dtc_code"),
        "msg_id",
        "ingestion_id",
        now.alias("conformed_at"),
    )

    return Conformed(signals, message_quarantine.unionByName(signal_quarantine), dtc_events)


# ---------------------------------------------------------------------- writing (Iceberg MERGE)
def ensure_tables(spark: SparkSession, tables: SilverTables) -> None:
    for kind, table in (
        ("signals", tables.signals),
        ("quarantine", tables.quarantine),
        ("dtc_events", tables.dtc_events),
    ):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
        spark.sql(DDL[kind].format(table=table))


def _latest_snapshot(spark: SparkSession, table: str) -> tuple[int | None, int]:
    rows = spark.sql(
        f"SELECT snapshot_id, summary['added-records'] AS added FROM {table}.snapshots "
        "ORDER BY committed_at DESC LIMIT 1"
    ).collect()
    return (rows[0].snapshot_id, int(rows[0].added or 0)) if rows else (None, 0)


def _merge_new(df: DataFrame, table: str, keys: list[str]) -> int:
    """Insert rows whose keys are not in the table yet (idempotent append); return rows added.

    The count comes from the Iceberg commit summary, so nothing has to be cached and recounted.
    """
    spark = df.sparkSession
    before, _ = _latest_snapshot(spark, table)
    view = f"silver_src_{table.rsplit('.', 1)[1]}"
    df.dropDuplicates(keys).createOrReplaceTempView(view)
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    spark.sql(f"MERGE INTO {table} t USING {view} s ON {on} WHEN NOT MATCHED THEN INSERT *")
    after, added = _latest_snapshot(spark, table)
    return added if after != before else 0


@dataclass
class BatchStats:
    batches: int = 0
    bronze_rows: int = 0
    signals: int = 0  # rows inserted (after deduplication)
    quarantined: int = 0
    dtc_events: int = 0
    contract_errors: dict[str, str] = field(default_factory=dict)


def process_batch(
    batch: DataFrame, contracts_dir: Path, tables: SilverTables, stats: BatchStats
) -> None:
    spark = batch.sparkSession
    loaded = load_contract_dir(contracts_dir)  # hot reload: picks up new/changed contracts
    if loaded.errors != stats.contract_errors:
        for name, reason in loaded.errors.items():
            print(f"contract {name} rejected: {reason}", file=sys.stderr)
        stats.contract_errors = loaded.errors
    specs = spark.createDataFrame(spec_rows(loaded.contracts), SPEC_SCHEMA)

    # Deliberately not cached: each MERGE streams its slice of Bronze from Iceberg. Caching the
    # batch materialized ~125k raw payloads per block and ran executors out of heap on backfills.
    out = conform(batch, specs)
    # event_ts is in the keys so Iceberg can prune partitions while matching duplicates.
    stats.signals += _merge_new(out.signals, tables.signals, ["msg_id", "vss_path", "event_ts"])
    stats.quarantined += _merge_new(out.quarantine, tables.quarantine, ["quarantine_key"])
    stats.dtc_events += _merge_new(
        out.dtc_events, tables.dtc_events, ["msg_id", "dtc_code", "event_ts"]
    )
    stats.batches += 1
    stats.bronze_rows += batch.count()


def start(
    spark: SparkSession,
    bronze_table: str = BRONZE_TABLE,
    tables: SilverTables | None = None,
    checkpoint: str = CHECKPOINT_URI,
    contracts_dir: Path = DEFAULT_CONTRACTS_DIR,
    continuous: bool = False,
    interval: str = "30 seconds",
    max_rows_per_batch: int = 500_000,
) -> tuple[StreamingQuery, BatchStats]:
    """Start the conformance stream; the returned stats are updated after every micro-batch."""
    tables = tables or SilverTables()
    ensure_tables(spark, tables)
    stats = BatchStats()
    writer = (
        spark.readStream.format("iceberg")
        # Bounded micro-batches: a large backfill is worked off in steps, not in one huge batch.
        .option("streaming-max-rows-per-micro-batch", max_rows_per_batch)
        # Bronze is append-only for this stream. A delete there (retention, GDPR erasure, removing
        # a bad load) is a deliberate purge with nothing new for Silver; without these options the
        # first one would halt the stream for good.
        .option("streaming-skip-delete-snapshots", "true")
        .option("streaming-skip-overwrite-snapshots", "true")
        .load(bronze_table)
        .writeStream.foreachBatch(lambda df, _id: process_batch(df, contracts_dir, tables, stats))
        .option("checkpointLocation", checkpoint)
        .queryName("silver_conformance")
    )
    writer = (
        writer.trigger(processingTime=interval) if continuous else writer.trigger(availableNow=True)
    )
    return writer.start(), stats


def run(
    spark: SparkSession,
    bronze_table: str = BRONZE_TABLE,
    tables: SilverTables | None = None,
    checkpoint: str = CHECKPOINT_URI,
    contracts_dir: Path = DEFAULT_CONTRACTS_DIR,
    continuous: bool = False,
    interval: str = "30 seconds",
) -> BatchStats:
    query, stats = start(
        spark, bronze_table, tables, checkpoint, contracts_dir, continuous, interval
    )
    query.awaitTermination()
    return stats


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--bronze-table", default=BRONZE_TABLE)
    p.add_argument(
        "--contracts", type=Path, default=Path(os.environ.get("CONTRACTS_DIR", "contracts"))
    )
    p.add_argument("--checkpoint", default=os.environ.get("SILVER_CHECKPOINT_URI", CHECKPOINT_URI))
    p.add_argument("--continuous", action="store_true", help="keep running (default: drain & stop)")
    p.add_argument("--interval", default="30 seconds")
    args = p.parse_args(argv)

    spark = get_spark(LakehouseConfig.from_env(app_name="silver-conformance"))
    stats = run(
        spark,
        args.bronze_table,
        None,
        args.checkpoint,
        args.contracts,
        args.continuous,
        args.interval,
    )
    print(
        f"{stats.batches} batch(es): {stats.bronze_rows:,} bronze rows -> {stats.signals:,} signal "
        f"readings, {stats.quarantined:,} quarantined, {stats.dtc_events:,} DTC events",
        file=sys.stderr,
    )
    spark.stop()


if __name__ == "__main__":
    main()

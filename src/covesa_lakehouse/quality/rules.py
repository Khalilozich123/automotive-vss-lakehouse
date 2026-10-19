"""Data-quality detectors that need more than a single SQL aggregate.

* **Stuck sensors.** A frozen sensor keeps sending one value while the vehicle moves, so every
  range check passes. A run of identical readings is flagged when it is long in absolute terms
  (``min_run``) *and* relative to how long that signal normally stays constant while moving
  on that platform (``ratio`` x the fleet p99 run length). This keeps naturally plateauing signals,
  such as coolant at thermostat temperature, out of the alerts.
  Runs at exactly 0 are ignored, because a sensor frozen at 0 is indistinguishable from a
  parked vehicle.

* **Schema drift.** JSON signal names and CAN frame ids that a platform sends but its contract
  does not map, e.g. after a firmware update. They are silently ignored by Silver, so they are
  reported here instead: an onboarding to-do, not an error.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

SPEED = "Vehicle.Speed"
MOVING_KPH = 5.0
RUN_COLUMNS = [
    "vin",
    "platform_id",
    "vss_path",
    "signal_name",
    "value",
    "run_length",
    "typical_run",
    "run_start",
    "run_end",
    "avg_speed_kph",
]


def stuck_runs(signals: DataFrame, min_run: int = 20, ratio: float = 2.0) -> DataFrame:
    """Runs of identical non-zero readings while moving that are abnormally long."""
    speed = signals.filter(F.col("vss_path") == SPEED).select(
        "vin", "event_ts", F.col("value").alias("_speed")
    )
    readings = signals.filter(F.col("vss_path") != SPEED).join(speed, ["vin", "event_ts"], "left")
    readings = readings.unionByName(
        signals.filter(F.col("vss_path") == SPEED).withColumn("_speed", F.col("value"))
    )
    w = Window.partitionBy("vin", "vss_path").orderBy("event_ts")
    prev = F.lag("value").over(w)
    changed = prev.isNull() | (F.col("value") != prev)
    runs = (
        readings.withColumn("_new", changed.cast("int"))
        .withColumn("run", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0)))
        .groupBy("vin", "platform_id", "vss_path", "signal_name", "run")
        .agg(
            F.count(F.lit(1)).alias("run_length"),
            F.first("value").alias("value"),
            F.min("event_ts").alias("run_start"),
            F.max("event_ts").alias("run_end"),
            F.avg("_speed").alias("avg_speed_kph"),
        )
    )
    moving = runs.filter((F.col("avg_speed_kph") > MOVING_KPH) & (F.col("value") != 0))
    # The tail, not the median: plateauing signals (coolant at thermostat temperature) have
    # long natural runs, so their bar rises automatically. Stuck runs are too rare (~0.05 % of
    # runs) to move the 99th percentile.
    typical = moving.groupBy("platform_id", "vss_path").agg(
        F.percentile_approx("run_length", 0.99).alias("typical_run")
    )
    threshold = F.greatest(F.lit(min_run), F.col("typical_run") * ratio)
    return (
        moving.join(typical, ["platform_id", "vss_path"])
        .filter(F.col("run_length") >= threshold)
        .select(*RUN_COLUMNS)
    )


DRIFT_ENVELOPE = "signals MAP<STRING, STRING>, frames ARRAY<STRUCT<id: BIGINT>>"


def unmapped_items(bronze: DataFrame, specs: DataFrame) -> DataFrame:
    """Signal names / CAN frame ids present in payloads but absent from the contracts."""
    env = bronze.select(
        "platform_id", "ingested_at", F.from_json("raw_payload", DRIFT_ENVELOPE).alias("e")
    )
    json_keys = env.select(
        "platform_id", "ingested_at", F.explode(F.map_keys("e.signals")).alias("signal_name")
    ).join(
        F.broadcast(specs.select("platform_id", "signal_name")),
        ["platform_id", "signal_name"],
        "left_anti",
    )
    frame_ids = env.select(
        "platform_id", "ingested_at", F.explode("e.frames.id").alias("frame_id")
    ).join(
        F.broadcast(specs.select("platform_id", "frame_id")),
        ["platform_id", "frame_id"],
        "left_anti",
    )
    # Platforms without any contract are already reported by Silver as UNKNOWN_PLATFORM.
    known = specs.select("platform_id").distinct()
    json_items = json_keys.select(
        "platform_id",
        "ingested_at",
        F.lit("json_signal").alias("kind"),
        F.col("signal_name").alias("item"),
    )
    frame_items = frame_ids.select(
        "platform_id",
        "ingested_at",
        F.lit("can_frame").alias("kind"),
        F.format_string("0x%X", "frame_id").alias("item"),
    )
    items = json_items.unionByName(frame_items)
    return (
        items.join(F.broadcast(known), "platform_id")
        .groupBy("platform_id", "kind", "item")
        .agg(F.count(F.lit(1)).alias("messages"), F.min("ingested_at").alias("first_seen"))
    )

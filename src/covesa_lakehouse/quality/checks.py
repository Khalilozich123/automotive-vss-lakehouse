"""Data-quality gates between the medallion layers.

Each check computes one number for a day window and compares it with a threshold:

* ``error``: the gate fails, and the pipeline stops before bad data reaches the next layer;
* ``warn``: recorded and reported, but the pipeline continues.

Every result is appended to ``lakehouse.ops.dq_results``, so quality is trended over time and
not only enforced. Detected stuck-sensor runs and unmapped signals are stored in their own
tables for triage.

    uv run python -m covesa_lakehouse.quality.checks --layer silver --last-days 1
"""

from __future__ import annotations

import argparse
import operator
import os
import sys
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.common.config_loader import SPEC_SCHEMA, load_contract_dir, spec_rows
from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark
from covesa_lakehouse.quality.rules import stuck_runs, unmapped_items

Layer = Literal["bronze", "silver", "gold"]
Severity = Literal["error", "warn"]

RESULTS_TABLE = "lakehouse.ops.dq_results"
STUCK_TABLE = "lakehouse.ops.dq_stuck_runs"
DRIFT_TABLE = "lakehouse.ops.dq_unmapped_signals"

RESULTS_SCHEMA = (
    "run_id STRING, checked_at TIMESTAMP, layer STRING, check_name STRING, severity STRING,"
    " observed DOUBLE, op STRING, threshold DOUBLE, passed BOOLEAN, window_start DATE,"
    " window_end DATE, description STRING"
)

OPS = {"<=": operator.le, ">=": operator.ge, "==": operator.eq}
B, S = "lakehouse.bronze.telemetry_raw", "lakehouse.silver"
IN_W = "BETWEEN DATE'{start}' AND DATE'{end}'"


@dataclass(frozen=True)
class Check:
    name: str
    layer: Layer
    severity: Severity
    op: str
    threshold: float
    description: str
    sql: str  # returns one row, one numeric column; {start}/{end} are the window's dates


CHECKS: list[Check] = [
    # ------------------------------------------------------------------ bronze
    Check("bronze_has_data", "bronze", "warn", ">=", 1,
          "rows ingested in the window (none: is the fleet / bridge running?)",
          f"SELECT count(*) FROM {B} WHERE to_date(ingested_at) {IN_W}"),
    Check("bronze_vin_never_raw", "bronze", "error", "==", 0,
          "privacy gate: payloads that still contain a raw 17-character VIN",
          f"""SELECT count(*) FROM {B} WHERE to_date(ingested_at) {IN_W}
              AND raw_payload RLIKE '"vin":"[A-HJ-NPR-Z0-9]{{{{17}}}}"'"""),  # f-string + .format
    Check("bronze_unattributed_share", "bronze", "error", "<=", 0.01,
          "share of rows that cannot be attributed to a platform",
          f"""SELECT coalesce(avg(CASE WHEN platform_id IS NULL THEN 1.0 ELSE 0.0 END), 0)
              FROM {B} WHERE to_date(ingested_at) {IN_W}"""),
    Check("bronze_unique_ingestion_id", "bronze", "error", "==", 0,
          "duplicated ingestion ids (exactly-once ingestion broken)",
          f"""SELECT count(*) FROM (SELECT ingestion_id FROM {B} WHERE to_date(ingested_at) {IN_W}
              GROUP BY ingestion_id HAVING count(*) > 1)"""),
    # ------------------------------------------------------------------ silver
    Check("silver_no_duplicate_readings", "silver", "error", "==", 0,
          "(msg_id, vss_path) present more than once (dedup broken)",
          f"""SELECT count(*) FROM (SELECT msg_id, vss_path FROM {S}.telemetry_signals
              WHERE to_date(event_ts) {IN_W} GROUP BY msg_id, vss_path HAVING count(*) > 1)"""),
    Check("silver_quarantine_rate", "silver", "error", "<=", 0.05,
          "quarantined readings / all readings (a jump means a broken feed or contract)",
          f"""SELECT coalesce(q / nullif(q + s, 0), 0) FROM
              (SELECT count(*) q FROM {S}.telemetry_quarantine
               WHERE to_date(quarantined_at) {IN_W}),
              (SELECT count(*) s FROM {S}.telemetry_signals WHERE to_date(event_ts) {IN_W})"""),
    Check("silver_parse_error_rate", "silver", "warn", "<=", 0.02,
          "unparseable messages / Bronze messages",
          f"""SELECT coalesce(p / nullif(b, 0), 0) FROM
              (SELECT count(*) p FROM {S}.telemetry_quarantine
               WHERE error_code = 'PARSE_ERROR' AND to_date(quarantined_at) {IN_W}),
              (SELECT count(*) b FROM {B} WHERE to_date(ingested_at) {IN_W})"""),
    Check("silver_unknown_platform_messages", "silver", "warn", "==", 0,
          "messages from platforms without a contract (onboarding needed)",
          f"""SELECT count(*) FROM {S}.telemetry_quarantine
              WHERE error_code = 'UNKNOWN_PLATFORM' AND to_date(quarantined_at) {IN_W}"""),
    # ------------------------------------------------------------------ gold
    Check("gold_state_built", "gold", "error", "==", 0,
          "Silver has readings in the window but the vehicle-state pivot does not",
          f"""SELECT CASE WHEN s > 0 AND v = 0 THEN 1 ELSE 0 END FROM
              (SELECT count(*) s FROM {S}.telemetry_signals WHERE to_date(event_ts) {IN_W}),
              (SELECT count(*) v FROM {S}.vehicle_state WHERE to_date(event_ts) {IN_W})"""),
    Check("gold_eco_score_in_range", "gold", "error", "==", 0,
          "eco scores outside [0, 100]",
          f"""SELECT count(*) FROM lakehouse.gold.eco_driving_daily
              WHERE day {IN_W} AND (eco_score_7d < 0 OR eco_score_7d > 100)"""),
    Check("gold_soh_plausible", "gold", "error", "==", 0,
          "session SoH estimates outside [50, 110] %",
          f"""SELECT count(*) FROM lakehouse.gold.battery_charging_sessions
              WHERE to_date(session_start) {IN_W} AND (soh_est_pct < 50 OR soh_est_pct > 110)"""),
    Check("gold_kpis_non_negative", "gold", "error", "==", 0,
          "negative EFC, thermal hours, distances or energy",
          f"""SELECT (SELECT count(*) FROM lakehouse.gold.battery_health_daily WHERE day {IN_W}
                      AND (efc < 0 OR thermal_stress_hours < 0))
                   + (SELECT count(*) FROM lakehouse.gold.eco_driving_trips
                      WHERE to_date(trip_start) {IN_W}
                      AND (distance_km < 0 OR energy_out_kwh < 0))"""),
    Check("gold_vehicles_known", "gold", "error", "==", 0,
          "vehicles in Gold that never appear in Silver (lineage broken)",
          f"""SELECT count(*) FROM (SELECT DISTINCT vin FROM lakehouse.gold.eco_driving_daily
              WHERE day {IN_W}) g LEFT ANTI JOIN (SELECT DISTINCT vin FROM {S}.telemetry_signals
              WHERE to_date(event_ts) {IN_W}) s ON g.vin = s.vin"""),
]  # fmt: skip


@dataclass(frozen=True)
class Result:
    check: Check
    observed: float

    @property
    def passed(self) -> bool:
        return bool(OPS[self.check.op](self.observed, self.check.threshold))


def _detector_checks(spark: SparkSession, start: date, end: date, run_id: str) -> list[Result]:
    """Stuck sensors and schema drift: detections are stored, their counts become checks."""
    window = (F.to_date("event_ts") >= F.lit(start)) & (F.to_date("event_ts") <= F.lit(end))
    signals = spark.table(f"{S}.telemetry_signals").filter(window)
    stuck = stuck_runs(signals).withColumn("run_id", F.lit(run_id))
    stuck.writeTo(STUCK_TABLE).createOrReplace()

    contracts = load_contract_dir(Path(os.environ.get("CONTRACTS_DIR", "contracts"))).contracts
    specs = spark.createDataFrame(spec_rows(contracts), SPEC_SCHEMA)
    bronze = spark.table(B).filter(F.to_date("ingested_at").between(F.lit(start), F.lit(end)))
    unmapped_items(bronze, specs).withColumn("run_id", F.lit(run_id)).writeTo(
        DRIFT_TABLE
    ).createOrReplace()

    stuck_check = Check(
        "silver_stuck_sensor_runs", "silver", "warn", "==", 0,
        f"abnormally long runs of one value while moving (see {STUCK_TABLE})", "",
    )  # fmt: skip
    drift_check = Check(
        "silver_unmapped_signals", "silver", "warn", "==", 0,
        f"signals/frames sent but not in any contract (see {DRIFT_TABLE})", "",
    )  # fmt: skip
    return [
        Result(stuck_check, float(spark.table(STUCK_TABLE).count())),
        Result(drift_check, float(spark.table(DRIFT_TABLE).count())),
    ]


def run(
    spark: SparkSession,
    layers: list[Layer],
    start: date,
    end: date,
    sql_checks: list[Check] | None = None,
) -> list[Result]:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.ops")
    run_id = str(uuid.uuid4())
    results: list[Result] = []
    for check in sql_checks if sql_checks is not None else CHECKS:
        if check.layer in layers:
            row = spark.sql(check.sql.format(start=start, end=end)).first()
            results.append(Result(check, float(row[0] if row and row[0] is not None else 0)))
    if "silver" in layers and sql_checks is None:
        results += _detector_checks(spark, start, end, run_id)

    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (run_id, now, r.check.layer, r.check.name, r.check.severity, r.observed, r.check.op,
         float(r.check.threshold), r.passed, start, end, r.check.description)
        for r in results
    ]  # fmt: skip
    spark.sql(f"CREATE TABLE IF NOT EXISTS {RESULTS_TABLE} ({RESULTS_SCHEMA}) USING iceberg")
    spark.createDataFrame(rows, RESULTS_SCHEMA).writeTo(RESULTS_TABLE).append()
    return results


def report(results: list[Result], out: Callable[[str], object] = print) -> bool:
    """Print a table; return True when no error-severity check failed."""
    ok = True
    for r in results:
        mark = "PASS" if r.passed else ("FAIL" if r.check.severity == "error" else "WARN")
        ok &= r.passed or r.check.severity == "warn"
        out(
            f"{mark:4}  {r.check.name:34} {r.observed:>12.4g} {r.check.op} {r.check.threshold:<8g}"
            f" {r.check.description}"
        )
    return ok


def window_from_args(args: argparse.Namespace) -> tuple[date, date]:
    if args.start and args.end:
        return args.start, args.end
    today = datetime.now(UTC).date()
    return today - timedelta(days=args.last_days), today


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--layer", choices=("bronze", "silver", "gold", "all"), default="all")
    p.add_argument("--from", dest="start", type=date.fromisoformat)
    p.add_argument("--to", dest="end", type=date.fromisoformat)
    p.add_argument("--last-days", type=int, default=1, help="window: today minus N days .. today")
    args = p.parse_args(argv)
    layers: list[Layer] = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]
    start, end = window_from_args(args)

    spark = get_spark(LakehouseConfig.from_env(app_name=f"dq-{args.layer}"))
    print(f"data quality: {', '.join(layers)} for {start} .. {end}", file=sys.stderr)
    ok = report(run(spark, layers, start, end), out=lambda s: print(s, file=sys.stderr))
    spark.stop()
    if not ok:
        sys.exit("data-quality gate failed: at least one error-severity check did not pass")


if __name__ == "__main__":
    main()

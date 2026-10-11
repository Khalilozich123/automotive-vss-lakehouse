"""Build the Gold layer for a range of days: vehicle state pivot, then the three marts.

Every step replaces whole day partitions, so re-running a day is idempotent and late data is
picked up by simply rebuilding the affected days.

    uv run python -m covesa_lakehouse.gold.build                      # every day present in Silver
    uv run python -m covesa_lakehouse.gold.build --from 2026-09-20 --to 2026-09-21
"""

from __future__ import annotations

import argparse
import sys
import time
from collections.abc import Callable
from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark
from covesa_lakehouse.gold import mart_battery_health, mart_eco_driving, mart_predictive_dtc
from covesa_lakehouse.gold.common import write_dim_platform
from covesa_lakehouse.silver import vehicle_state


def silver_day_range(spark: SparkSession) -> tuple[date, date]:
    row = (
        spark.table(vehicle_state.SIGNALS_TABLE)
        .agg(F.to_date(F.min("event_ts")).alias("lo"), F.to_date(F.max("event_ts")).alias("hi"))
        .first()
    )
    if row is None or row.lo is None:
        raise SystemExit("Silver is empty: run Bronze and Silver first")
    return row.lo, row.hi


def build(spark: SparkSession, start: date, end: date) -> dict[str, float]:
    steps: list[tuple[str, Callable[[SparkSession, date, date], None]]] = [
        ("silver.vehicle_state", vehicle_state.build),
        ("gold.battery_health", mart_battery_health.build),
        ("gold.dtc_precursors", mart_predictive_dtc.build),
        ("gold.eco_driving", mart_eco_driving.build),
    ]
    write_dim_platform(spark)
    timings = {}
    for name, step in steps:
        t0 = time.monotonic()
        step(spark, start, end)
        timings[name] = round(time.monotonic() - t0, 1)
        print(f"  {name}: {timings[name]} s", file=sys.stderr)
    return timings


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--from", dest="start", type=date.fromisoformat)
    p.add_argument("--to", dest="end", type=date.fromisoformat)
    args = p.parse_args(argv)

    spark = get_spark(LakehouseConfig.from_env(app_name="gold-build"))
    lo, hi = silver_day_range(spark)
    start, end = args.start or lo, args.end or hi
    print(f"building Gold for {start} .. {end}", file=sys.stderr)
    build(spark, start, end)
    spark.stop()


if __name__ == "__main__":
    main()

"""Building blocks shared by the Gold marts.

Time integration: telemetry is sampled (every few seconds), so energy, charge throughput and
"hours above 45 C" are sums of ``value x dt``. Each sample holds until the next one; a gap longer
than ``MAX_GAP_S`` (vehicle off, lost connectivity) contributes nothing, so outages are never
interpolated into fake energy.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from covesa_lakehouse.common.config_loader import load_contract_dir

MAX_GAP_S = 30.0
DIM_PLATFORM = "lakehouse.gold.dim_platform"


def with_sample_dt(df: DataFrame, ts: str = "event_ts", max_gap_s: float = MAX_GAP_S) -> DataFrame:
    """Add ``dt_s``: seconds until the vehicle's next sample (0 across gaps and at the end)."""
    w = Window.partitionBy("vin").orderBy(ts)
    gap = F.lead(ts).over(w).cast("double") - F.col(ts).cast("double")
    return df.withColumn("dt_s", F.when(gap <= max_gap_s, gap).otherwise(F.lit(0.0)))


def sessionize(
    df: DataFrame, active: Column, max_gap_s: float, ts: str = "event_ts", name: str = "session"
) -> DataFrame:
    """Group consecutive active samples per vehicle; a pause longer than ``max_gap_s`` splits.

    Returns the active rows only, with ``{name}_id`` (vin + start time) and ``{name}_start``.
    """
    w = Window.partitionBy("vin").orderBy(ts)
    rows = df.filter(active)
    prev = F.lag(ts).over(w)
    new = prev.isNull() | ((F.col(ts).cast("double") - prev.cast("double")) > max_gap_s)
    rows = rows.withColumn("_new", new.cast("int")).withColumn(
        "_seq", F.sum("_new").over(w.rowsBetween(Window.unboundedPreceding, 0))
    )
    start = F.min(ts).over(Window.partitionBy("vin", "_seq"))
    return (
        rows.withColumn(f"{name}_start", start)
        .withColumn(
            f"{name}_id",
            F.concat_ws("|", F.col("vin"), F.date_format(f"{name}_start", "yyyyMMdd'T'HHmmss")),
        )
        .drop("_new", "_seq")
    )


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def build_dim_platform(spark: SparkSession, contracts_dir: Path = Path("contracts")) -> DataFrame:
    """Platform reference data straight from the contracts (single source of truth)."""
    rows = [
        (
            c.platform_id,
            c.platform_name,
            c.powertrain,
            c.encoding,
            c.version,
            c.vehicle.battery_kwh,
            c.vehicle.battery_cells_series,
            c.vehicle.nominal_capacity_ah,
            sorted(s.vss_path for s in c.signals.values()),
        )
        for c in load_contract_dir(contracts_dir).contracts.values()
    ]
    return spark.createDataFrame(
        rows,
        "platform_id STRING, platform_name STRING, powertrain STRING, encoding STRING,"
        " contract_version STRING, battery_kwh DOUBLE, battery_cells_series INT,"
        " nominal_capacity_ah DOUBLE, vss_paths ARRAY<STRING>",
    )


def write_dim_platform(spark: SparkSession, contracts_dir: Path = Path("contracts")) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lakehouse.gold")
    build_dim_platform(spark, contracts_dir).writeTo(DIM_PLATFORM).createOrReplace()


def ensure_table(spark: SparkSession, ddl: str, table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {table.rsplit('.', 1)[0]}")
    spark.sql(ddl.format(table=table))


def in_days(column: str, start: date, end: date) -> Column:
    return F.to_date(column).between(F.lit(start), F.lit(end))

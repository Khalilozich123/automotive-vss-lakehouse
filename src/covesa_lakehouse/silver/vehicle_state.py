"""``silver.vehicle_state``: conformed telemetry pivoted to one row per vehicle per timestamp.

Silver signals are long (one row per reading), which suits ingestion and quality control. Fleet
analytics need the signals side by side (current x voltage, speed with acceleration), so the
signals the Gold marts use are pivoted once here instead of in every mart. Columns are named
after their VSS leaf; a platform that does not report a signal simply has nulls in that column.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

SIGNALS_TABLE = "lakehouse.silver.telemetry_signals"
STATE_TABLE = "lakehouse.silver.vehicle_state"

# column -> VSS path (all values are already in the VSS unit)
STATE_COLUMNS: dict[str, str] = {
    "speed_kph": "Vehicle.Speed",
    "odometer_m": "Vehicle.TraveledDistance",
    "accel_long_ms2": "Vehicle.Acceleration.Longitudinal",
    "pack_voltage_v": "Vehicle.Powertrain.TractionBattery.CurrentVoltage",
    "pack_current_a": "Vehicle.Powertrain.TractionBattery.CurrentCurrent",  # + = charging
    "soc_pct": "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current",
    "batt_temp_max_c": "Vehicle.Powertrain.TractionBattery.Temperature.Max",
    "batt_temp_min_c": "Vehicle.Powertrain.TractionBattery.Temperature.Min",
    "cell_v_max": "Vehicle.Powertrain.TractionBattery.CellVoltage.Max",
    "cell_v_min": "Vehicle.Powertrain.TractionBattery.CellVoltage.Min",
    "dc_charge_current_a": "Vehicle.Powertrain.TractionBattery.Charging.ChargeCurrent.DC",
    "is_charging": "Vehicle.Powertrain.TractionBattery.Charging.IsCharging",
    "engine_rpm": "Vehicle.Powertrain.CombustionEngine.Speed",
    "coolant_temp_c": "Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature",
    "lv_voltage_v": "Vehicle.LowVoltageBattery.CurrentVoltage",
}

DDL = f"""
CREATE TABLE IF NOT EXISTS {{table}} (
    vin STRING, platform_id STRING, event_ts TIMESTAMP,
    {", ".join(f"{c} DOUBLE" for c in STATE_COLUMNS)}
) USING iceberg
PARTITIONED BY (days(event_ts))
COMMENT 'Silver: VSS signals pivoted to one row per vehicle and timestamp'
TBLPROPERTIES ('format-version'='2', 'write.parquet.compression-codec'='zstd')
"""


def pivot_signals(signals: DataFrame) -> DataFrame:
    path_to_col = {path: col for col, path in STATE_COLUMNS.items()}
    wide = (
        signals.filter(F.col("vss_path").isin(list(path_to_col)))
        .groupBy("vin", "platform_id", "event_ts")
        .pivot("vss_path", list(path_to_col))
        .agg(F.first("value"))
    )
    return wide.select(
        "vin",
        "platform_id",
        "event_ts",
        *[F.col(f"`{path}`").cast("double").alias(col) for path, col in path_to_col.items()],
    )


def day_filter(column: str, start: date, end: date) -> Column:
    """``start <= column < end + 1 day`` on a timestamp column (inclusive day range)."""
    return (F.col(column) >= F.lit(start).cast("timestamp")) & (
        F.col(column) < F.date_add(F.lit(end), 1).cast("timestamp")
    )


def build(
    spark: SparkSession,
    start: date,
    end: date,
    signals_table: str = SIGNALS_TABLE,
    state_table: str = STATE_TABLE,
) -> None:
    """Rebuild the day partitions ``start..end`` (idempotent: partitions are replaced)."""
    spark.sql(DDL.format(table=state_table))
    signals = spark.table(signals_table).filter(day_filter("event_ts", start, end))
    pivot_signals(signals).writeTo(state_table).overwritePartitions()

"""Gold Mart 1: EV battery State of Health (SoH) and thermal degradation.

``gold.battery_charging_sessions`` (one row per charging session)
    Energy and charge delivered, SoC window, and a **capacity estimate by Coulomb counting**:
    the inverse slope of SoC against cumulative Ah over all of the session's samples (an endpoint
    ratio, Ah / delta SoC, is broken by one skewed reading). Its ratio to the nominal capacity
    (from the platform contract) is the session's SoH estimate. Sessions with a frozen current
    sensor get none. Only sessions that add at least
    ``MIN_SOC_GAIN`` percentage points are used, because short top-ups give noisy ratios.

``gold.battery_health_daily`` (one row per vehicle per day)
    * thermal stress: hours with the hottest cell above 45 C (the spec's degradation proxy)
    * Equivalent Full Cycles: ``(Ah charged + Ah discharged) / 2 / nominal Ah``
    * cell-voltage delta (max - min cell, mV): a widening spread precedes cell failure
    * SoH estimate of the day, and its trailing 7-day average
    * risk level: ``critical`` when a wide cell delta and high temperature coincide

KPIs need different signals, and each is computed wherever its inputs exist. For example, a
platform that reports temperature but no current still gets thermal stress, but no EFC or SoH.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from covesa_lakehouse.gold.common import (
    DIM_PLATFORM,
    ensure_table,
    in_days,
    sessionize,
    with_sample_dt,
)
from covesa_lakehouse.silver.vehicle_state import STATE_TABLE, day_filter

SESSIONS_TABLE = "lakehouse.gold.battery_charging_sessions"
DAILY_TABLE = "lakehouse.gold.battery_health_daily"

THERMAL_STRESS_C = 45.0
CELL_DELTA_WATCH_MV = 100.0
MIN_SOC_GAIN = 10.0
CHARGE_PAUSE_S = 120.0
SOC_SPIKE_PTS = 1.0
FROZEN_CURRENT_SAMPLES = 12  # one exact current value for a minute at 5 s sampling

SESSIONS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    session_id STRING, vin STRING, platform_id STRING,
    session_start TIMESTAMP, session_end TIMESTAMP, duration_min DOUBLE,
    soc_start_pct DOUBLE, soc_end_pct DOUBLE, energy_in_kwh DOUBLE, charge_in_ah DOUBLE,
    max_dc_current_a DOUBLE, max_temp_c DOUBLE,
    capacity_est_ah DOUBLE COMMENT '100 / slope of SoC vs cumulative Ah (Coulomb counting)',
    soh_est_pct DOUBLE COMMENT 'capacity_est_ah / nominal capacity x 100; null if SoC gain < 10 pts'
) USING iceberg PARTITIONED BY (days(session_start))
TBLPROPERTIES ('format-version'='2')
"""

DAILY_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    vin STRING, platform_id STRING, day DATE,
    samples BIGINT, driving_hours DOUBLE, charging_hours DOUBLE,
    energy_out_kwh DOUBLE, energy_in_kwh DOUBLE, charge_ah DOUBLE, discharge_ah DOUBLE,
    efc DOUBLE COMMENT 'Equivalent Full Cycles on this day',
    thermal_stress_hours DOUBLE COMMENT 'hours with Temperature.Max > 45 C',
    max_temp_c DOUBLE, avg_temp_c DOUBLE,
    cell_delta_max_mv DOUBLE, cell_delta_avg_mv DOUBLE,
    charging_sessions BIGINT, soh_est_pct DOUBLE, soh_est_pct_7d DOUBLE,
    risk_level STRING COMMENT 'normal | watch | critical'
) USING iceberg PARTITIONED BY (day)
TBLPROPERTIES ('format-version'='2')
"""


def charging_sessions(state: DataFrame, dim: DataFrame) -> DataFrame:
    charging = sessionize(
        state.filter(F.col("is_charging").isNotNull()),
        F.col("is_charging") >= 0.5,
        CHARGE_PAUSE_S,
        name="session",
    )
    charging = with_sample_dt(charging)
    current_in = F.greatest(F.col("pack_current_a"), F.lit(0.0))
    ah = current_in * F.col("dt_s") / 3600.0
    # Charge delivered before each sample (each sample's own Ah covers the interval after it).
    order = Window.partitionBy("session_id").orderBy("event_ts")
    before = order.rowsBetween(Window.unboundedPreceding, -1)
    charging = charging.withColumn("cum_ah", F.coalesce(F.sum(ah).over(before), F.lit(0.0)))
    # SoC cannot jump and come straight back while charging: a reading that disagrees with
    # both neighbours (a late, clock-skewed message) is left out of the fit and the endpoints.
    soc = F.col("soc_pct")
    spike = F.coalesce(F.abs(soc - F.lag(soc).over(order)) > SOC_SPIKE_PTS, F.lit(True)) & (
        F.coalesce(F.abs(soc - F.lead(soc).over(order)) > SOC_SPIKE_PTS, F.lit(True))
    )
    charging = charging.withColumn("_soc", F.when(~spike, soc))
    clean_ts = F.when(F.col("_soc").isNotNull(), F.col("event_ts"))
    # A frozen current sensor keeps reporting one exact value while the real current tapers;
    # its Ah total is fiction, so such a session gets no capacity estimate.
    amps = F.col("pack_current_a")
    new_run = F.coalesce(amps != F.lag(amps).over(order), F.lit(True)).cast("int")
    charging = charging.withColumn("_run", F.sum(new_run).over(order)).withColumn(
        "_run_len", F.count(F.lit(1)).over(Window.partitionBy("session_id", "_run"))
    )
    sessions = charging.groupBy("session_id", "vin", "platform_id", "session_start").agg(
        F.max("event_ts").alias("session_end"),
        F.min_by("_soc", clean_ts).alias("soc_start_pct"),
        F.max_by("_soc", clean_ts).alias("soc_end_pct"),
        F.sum(current_in * F.col("pack_voltage_v") * F.col("dt_s") / 3.6e6).alias("energy_in_kwh"),
        F.sum(ah).alias("charge_in_ah"),
        # SoC points per Ah over *every* sample of the session: an endpoint ratio
        # (Ah / delta SoC) is wrecked by one skewed reading at either end.
        F.regr_slope("_soc", "cum_ah").alias("_soc_per_ah"),
        F.max("_run_len").alias("_frozen_samples"),
        F.max("dc_charge_current_a").alias("max_dc_current_a"),
        F.max("batt_temp_max_c").alias("max_temp_c"),
    )
    soc_gain = F.col("soc_end_pct") - F.col("soc_start_pct")
    capacity = F.when(
        (soc_gain >= MIN_SOC_GAIN)
        & (F.col("_soc_per_ah") > 0)
        & (F.col("_frozen_samples") < FROZEN_CURRENT_SAMPLES),
        100.0 / F.col("_soc_per_ah"),
    )
    return (
        sessions.join(F.broadcast(dim.select("platform_id", "nominal_capacity_ah")), "platform_id")
        .withColumn(
            "duration_min",
            (F.col("session_end").cast("double") - F.col("session_start").cast("double")) / 60.0,
        )
        .withColumn("capacity_est_ah", capacity)
        .withColumn("soh_est_pct", capacity / F.col("nominal_capacity_ah") * 100.0)
        .drop("nominal_capacity_ah", "_soc_per_ah", "_frozen_samples")
    )


def daily_health(state: DataFrame, sessions: DataFrame, dim: DataFrame) -> DataFrame:
    battery = state.filter(
        F.col("pack_voltage_v").isNotNull() | F.col("batt_temp_max_c").isNotNull()
    )
    s = with_sample_dt(battery).withColumn("day", F.to_date("event_ts"))
    dt_h = F.col("dt_s") / 3600.0
    amps_in = F.greatest(F.col("pack_current_a"), F.lit(0.0))  # VSS: + = into the battery
    amps_out = F.greatest(-F.col("pack_current_a"), F.lit(0.0))
    volts = F.col("pack_voltage_v")
    cell_delta_mv = (F.col("cell_v_max") - F.col("cell_v_min")) * 1000.0
    driving = F.coalesce(F.col("speed_kph"), F.lit(0.0)) > 1.0
    charging = F.coalesce(F.col("is_charging"), F.lit(0.0)) >= 0.5
    hot = F.col("batt_temp_max_c") > THERMAL_STRESS_C

    daily = s.groupBy("vin", "platform_id", "day").agg(
        F.count(F.lit(1)).alias("samples"),
        F.sum(F.when(driving, dt_h).otherwise(0.0)).alias("driving_hours"),
        F.sum(F.when(charging, dt_h).otherwise(0.0)).alias("charging_hours"),
        F.sum(amps_out * volts * dt_h / 1000.0).alias("energy_out_kwh"),
        F.sum(amps_in * volts * dt_h / 1000.0).alias("energy_in_kwh"),
        F.sum(amps_in * dt_h).alias("charge_ah"),
        F.sum(amps_out * dt_h).alias("discharge_ah"),
        F.sum(F.when(hot, dt_h).otherwise(0.0)).alias("thermal_stress_hours"),
        F.max("batt_temp_max_c").alias("max_temp_c"),
        F.avg("batt_temp_max_c").alias("avg_temp_c"),
        F.max(cell_delta_mv).alias("cell_delta_max_mv"),
        F.avg(cell_delta_mv).alias("cell_delta_avg_mv"),
    )

    per_day_sessions = (
        sessions.withColumn("day", F.to_date("session_start"))
        .groupBy("vin", "day")
        .agg(
            F.count(F.lit(1)).alias("charging_sessions"), F.avg("soh_est_pct").alias("soh_est_pct")
        )
    )
    efc = (F.col("charge_ah") + F.col("discharge_ah")) / 2.0 / F.col("nominal_capacity_ah")
    wide_cells = F.col("cell_delta_max_mv") > CELL_DELTA_WATCH_MV
    hot = F.col("thermal_stress_hours") > 0
    return (
        daily.join(per_day_sessions, ["vin", "day"], "left")
        .join(F.broadcast(dim.select("platform_id", "nominal_capacity_ah")), "platform_id", "left")
        .withColumn("efc", F.when(F.col("charge_ah") + F.col("discharge_ah") > 0, efc))
        .withColumn("charging_sessions", F.coalesce("charging_sessions", F.lit(0)).cast("bigint"))
        .withColumn("soh_est_pct_7d", F.lit(None).cast("double"))  # filled by refresh_rolling
        .withColumn(
            "risk_level",
            F.when(F.coalesce(wide_cells & hot, F.lit(False)), "critical")
            .when(F.coalesce(wide_cells | hot, F.lit(False)), "watch")
            .otherwise("normal"),
        )
        .drop("nominal_capacity_ah")
    )


def refresh_rolling(spark: SparkSession, start: date, end: date) -> None:
    """Trailing 7-day SoH, computed from the stored daily rows (so earlier days are reused)."""
    daily = spark.table(DAILY_TABLE).filter(
        F.col("day").between(F.lit(start - timedelta(days=6)), F.lit(end))
    )
    w = (
        Window.partitionBy("vin")
        .orderBy(F.col("day").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    rolled = daily.withColumn("soh_est_pct_7d", F.avg("soh_est_pct").over(w)).filter(
        F.col("day").between(F.lit(start), F.lit(end))
    )
    rolled.select(*spark.table(DAILY_TABLE).columns).writeTo(DAILY_TABLE).overwritePartitions()


def build(spark: SparkSession, start: date, end: date) -> None:
    ensure_table(spark, SESSIONS_DDL, SESSIONS_TABLE)
    ensure_table(spark, DAILY_DDL, DAILY_TABLE)
    dim = spark.table(DIM_PLATFORM).filter(F.col("battery_kwh").isNotNull())
    battery_vins = dim.select("platform_id")
    # One day of look-back so sessions that started before midnight are seen whole.
    state = (
        spark.table(STATE_TABLE)
        .filter(day_filter("event_ts", start - timedelta(days=1), end))
        .join(F.broadcast(battery_vins), "platform_id")
    )
    sessions = charging_sessions(state, dim).filter(in_days("session_start", start, end))
    sessions.writeTo(SESSIONS_TABLE).overwritePartitions()

    in_range = state.filter(in_days("event_ts", start, end))
    daily = daily_health(
        in_range, spark.table(SESSIONS_TABLE).filter(in_days("session_start", start, end)), dim
    )
    daily.select(*spark.table(DAILY_TABLE).columns).writeTo(DAILY_TABLE).overwritePartitions()
    refresh_rolling(spark, start, end)

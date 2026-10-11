"""Gold Mart 3: fleet eco-driving and dynamic energy scoring.

``gold.eco_driving_trips``  one row per trip (moving samples; a stop over 5 min ends the trip)
    distance, duration and speeds; harsh accelerations (> 3.0 m/s^2) and harsh braking
    (< -3.0 m/s^2); for battery-electric data: energy out, energy recovered by regenerative
    braking, net kWh/100 km and the regen recovery ratio.

``gold.eco_driving_daily``  one row per vehicle per day, plus **sliding 7-day** aggregates and
    an eco score (0-100). Ratios over the window are computed from summed numerators and
    denominators (total harsh events / total km), not by averaging daily ratios.

Eco score: 100 minus penalties for harsh acceleration (up to 50 points), harsh braking (up to 30)
and, where energy data exists, consumption above the platform's fleet median (up to 20). Each
penalty saturates at ``HARSH_SATURATION_PER_100KM`` events or +50 % consumption. Without energy
data the two driving penalties are rescaled to 100.

Because every platform reports ``Vehicle.Acceleration.Longitudinal`` under the same VSS name,
harsh driving is scored identically across brands, with no per-platform code.
"""

from __future__ import annotations

from datetime import date, timedelta

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from covesa_lakehouse.gold.common import ensure_table, in_days, sessionize, with_sample_dt
from covesa_lakehouse.silver.vehicle_state import STATE_TABLE, day_filter

TRIPS_TABLE = "lakehouse.gold.eco_driving_trips"
DAILY_TABLE = "lakehouse.gold.eco_driving_daily"

HARSH_ACCEL_MS2 = 3.0
HARSH_BRAKE_MS2 = -3.0
MOVING_KPH = 1.0
TRIP_PAUSE_S = 300.0
# Calibrated on the simulated fleet: rates run 0-8 per 100 km, and the most aggressive drivers
# sit at 6-8; at 5 events per 100 km a penalty is complete.
HARSH_SATURATION_PER_100KM = 5.0
MIN_TRIP_KM = 0.5

TRIPS_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    trip_id STRING, vin STRING, platform_id STRING, trip_start TIMESTAMP, trip_end TIMESTAMP,
    duration_min DOUBLE, distance_km DOUBLE, avg_speed_kph DOUBLE, max_speed_kph DOUBLE,
    accel_samples BIGINT COMMENT 'samples with a longitudinal acceleration reading',
    harsh_accel_events BIGINT, harsh_brake_events BIGINT,
    energy_out_kwh DOUBLE, regen_kwh DOUBLE,
    kwh_per_100km DOUBLE COMMENT 'net traction energy (out - regen) per 100 km',
    regen_ratio DOUBLE COMMENT 'energy recovered / energy used'
) USING iceberg PARTITIONED BY (days(trip_start))
TBLPROPERTIES ('format-version'='2')
"""

DAILY_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    vin STRING, platform_id STRING, day DATE,
    trips BIGINT, distance_km DOUBLE, driving_hours DOUBLE,
    harsh_accel_events BIGINT, harsh_brake_events BIGINT,
    harsh_accel_per_100km DOUBLE, harsh_brake_per_100km DOUBLE,
    energy_out_kwh DOUBLE, regen_kwh DOUBLE, kwh_per_100km DOUBLE, regen_ratio DOUBLE,
    distance_km_7d DOUBLE, harsh_accel_per_100km_7d DOUBLE, harsh_brake_per_100km_7d DOUBLE,
    kwh_per_100km_7d DOUBLE, regen_ratio_7d DOUBLE,
    eco_score_7d DOUBLE COMMENT '0-100, higher is better (trailing 7 days)'
) USING iceberg PARTITIONED BY (day)
TBLPROPERTIES ('format-version'='2')
"""


def trips(state: DataFrame) -> DataFrame:
    moving = sessionize(
        state.filter(F.col("speed_kph").isNotNull()),
        F.col("speed_kph") > MOVING_KPH,
        TRIP_PAUSE_S,
        name="trip",
    )
    s = with_sample_dt(moving)
    current = F.col("pack_current_a")
    power_kw = current * F.col("pack_voltage_v") / 1000.0
    accel = F.col("accel_long_ms2")
    agg = s.groupBy("trip_id", "vin", "platform_id", "trip_start").agg(
        F.max("event_ts").alias("trip_end"),
        (F.max("odometer_m") - F.min("odometer_m")).alias("odo_m"),
        F.sum(F.col("speed_kph") / 3.6 * F.col("dt_s")).alias("integrated_m"),
        F.max("speed_kph").alias("max_speed_kph"),
        F.count(accel).alias("accel_samples"),
        F.count(F.when(accel > HARSH_ACCEL_MS2, 1)).alias("harsh_accel_events"),
        F.count(F.when(accel < HARSH_BRAKE_MS2, 1)).alias("harsh_brake_events"),
        F.sum(F.greatest(-power_kw, F.lit(0.0)) * F.col("dt_s") / 3600.0).alias("energy_out_kwh"),
        F.sum(F.greatest(power_kw, F.lit(0.0)) * F.col("dt_s") / 3600.0).alias("regen_kwh"),
    )
    # Odometer when the platform reports one (exact), otherwise speed integrated over time.
    distance_km = (
        F.coalesce(F.when(F.col("odo_m") > 0, F.col("odo_m")), F.col("integrated_m")) / 1000.0
    )
    duration_min = (F.col("trip_end").cast("double") - F.col("trip_start").cast("double")) / 60.0
    return (
        agg.withColumn("distance_km", distance_km)
        .withColumn("duration_min", duration_min)
        .filter(F.col("distance_km") >= MIN_TRIP_KM)
        .withColumn(
            "avg_speed_kph",
            F.when(
                F.col("duration_min") > 0, F.col("distance_km") / (F.col("duration_min") / 60.0)
            ),
        )
        .withColumn(
            "kwh_per_100km",
            F.when(
                F.col("energy_out_kwh") > 0,
                (F.col("energy_out_kwh") - F.col("regen_kwh")) / F.col("distance_km") * 100.0,
            ),
        )
        .withColumn(
            "regen_ratio",
            F.when(F.col("energy_out_kwh") > 0, F.col("regen_kwh") / F.col("energy_out_kwh")),
        )
        .withColumn(
            "harsh_accel_events",
            F.when(F.col("accel_samples") > 0, F.col("harsh_accel_events")),
        )
        .withColumn(
            "harsh_brake_events",
            F.when(F.col("accel_samples") > 0, F.col("harsh_brake_events")),
        )
        .drop("odo_m", "integrated_m")
    )


def _per_100km(events: Column, km: Column) -> Column:
    return F.when(km > 0, events / km * 100.0)


def eco_score(harsh_accel: Column, harsh_brake: Column, consumption_ratio: Column) -> Column:
    """0-100. ``consumption_ratio`` = kWh/100km divided by the platform median (null if n/a)."""
    sat = HARSH_SATURATION_PER_100KM
    accel_pen = F.least(F.coalesce(harsh_accel, F.lit(0.0)) / sat, F.lit(1.0))
    brake_pen = F.least(F.coalesce(harsh_brake, F.lit(0.0)) / sat, F.lit(1.0))
    energy_pen = F.least(F.greatest(consumption_ratio - 1.0, F.lit(0.0)) / 0.5, F.lit(1.0))
    with_energy = 100 - 50 * accel_pen - 30 * brake_pen - 20 * energy_pen
    driving_only = 100 - (50 * accel_pen + 30 * brake_pen) / 0.8
    score = F.when(consumption_ratio.isNotNull(), with_energy).otherwise(driving_only)
    return F.when(harsh_accel.isNotNull() | consumption_ratio.isNotNull(), F.round(score, 1))


def daily(trip_rows: DataFrame) -> DataFrame:
    return (
        trip_rows.withColumn("day", F.to_date("trip_start"))
        .groupBy("vin", "platform_id", "day")
        .agg(
            F.count(F.lit(1)).alias("trips"),
            F.sum("distance_km").alias("distance_km"),
            (F.sum("duration_min") / 60.0).alias("driving_hours"),
            F.sum("harsh_accel_events").alias("harsh_accel_events"),
            F.sum("harsh_brake_events").alias("harsh_brake_events"),
            F.sum(F.when(F.col("harsh_accel_events").isNotNull(), F.col("distance_km"))).alias(
                "_accel_km"
            ),
            F.sum(F.when(F.col("energy_out_kwh") > 0, F.col("energy_out_kwh"))).alias(
                "energy_out_kwh"
            ),
            F.sum(F.when(F.col("energy_out_kwh") > 0, F.col("regen_kwh"))).alias("regen_kwh"),
            F.sum(F.when(F.col("energy_out_kwh") > 0, F.col("distance_km"))).alias("_energy_km"),
        )
    )


def with_rolling(days_df: DataFrame) -> DataFrame:
    """Sliding 7-day window per vehicle; ratios from summed numerators/denominators."""
    w = (
        Window.partitionBy("vin")
        .orderBy(F.col("day").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )

    def s(c: str) -> Column:
        return F.sum(c).over(w)

    net = F.col("energy_out_kwh") - F.col("regen_kwh")
    out = (
        days_df.withColumn(
            "harsh_accel_per_100km", _per_100km(F.col("harsh_accel_events"), F.col("_accel_km"))
        )
        .withColumn(
            "harsh_brake_per_100km", _per_100km(F.col("harsh_brake_events"), F.col("_accel_km"))
        )
        .withColumn("kwh_per_100km", _per_100km(net, F.col("_energy_km")))
        .withColumn("regen_ratio", F.col("regen_kwh") / F.col("energy_out_kwh"))
        .withColumn("distance_km_7d", s("distance_km"))
        .withColumn("harsh_accel_per_100km_7d", _per_100km(s("harsh_accel_events"), s("_accel_km")))
        .withColumn("harsh_brake_per_100km_7d", _per_100km(s("harsh_brake_events"), s("_accel_km")))
        .withColumn(
            "kwh_per_100km_7d", _per_100km(s("energy_out_kwh") - s("regen_kwh"), s("_energy_km"))
        )
        .withColumn("regen_ratio_7d", s("regen_kwh") / s("energy_out_kwh"))
    )
    median = out.groupBy("platform_id").agg(
        F.percentile_approx("kwh_per_100km_7d", 0.5).alias("_platform_median_kwh")
    )
    return out.join(median, "platform_id", "left").withColumn(
        "eco_score_7d",
        eco_score(
            F.col("harsh_accel_per_100km_7d"),
            F.col("harsh_brake_per_100km_7d"),
            F.col("kwh_per_100km_7d") / F.col("_platform_median_kwh"),
        ),
    )


def build(spark: SparkSession, start: date, end: date) -> None:
    ensure_table(spark, TRIPS_DDL, TRIPS_TABLE)
    ensure_table(spark, DAILY_DDL, DAILY_TABLE)
    # One day of look-back so trips that started before midnight are seen whole.
    state = spark.table(STATE_TABLE).filter(day_filter("event_ts", start - timedelta(days=1), end))
    trip_rows = trips(state).filter(in_days("trip_start", start, end))
    trip_rows.select(*spark.table(TRIPS_TABLE).columns).writeTo(TRIPS_TABLE).overwritePartitions()

    # Rolling windows need the 6 previous days of trips, which are already stored.
    history = spark.table(TRIPS_TABLE).filter(in_days("trip_start", start - timedelta(days=6), end))
    rolled = with_rolling(daily(history)).filter(F.col("day").between(F.lit(start), F.lit(end)))
    rolled.select(*spark.table(DAILY_TABLE).columns).writeTo(DAILY_TABLE).overwritePartitions()

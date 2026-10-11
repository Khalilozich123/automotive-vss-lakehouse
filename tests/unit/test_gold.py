"""Gold KPI arithmetic on small hand-built timelines with exactly known answers."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.gold import mart_battery_health as battery
from covesa_lakehouse.gold import mart_eco_driving as eco
from covesa_lakehouse.gold import mart_predictive_dtc as dtc
from covesa_lakehouse.gold.common import sessionize, with_sample_dt
from covesa_lakehouse.silver.vehicle_state import STATE_COLUMNS

T0 = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)
NOMINAL_AH = 60_000 / (96 * 3.7)  # the Renault contract's pack
STATE_SCHEMA = "vin STRING, platform_id STRING, event_ts TIMESTAMP, " + ", ".join(
    f"{c} DOUBLE" for c in STATE_COLUMNS
)


def _state(spark: SparkSession, rows: list[dict[str, Any]]) -> DataFrame:
    full = [
        tuple([r.get("vin", "V1"), r.get("platform_id", "RENAULT_CMF_EV"), r["ts"]])
        + tuple(r.get(c) for c in STATE_COLUMNS)
        for r in rows
    ]
    return spark.createDataFrame(full, STATE_SCHEMA)


def _dim(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [("RENAULT_CMF_EV", 60.0, NOMINAL_AH)],
        "platform_id STRING, battery_kwh DOUBLE, nominal_capacity_ah DOUBLE",
    )


# ---------------------------------------------------------------------- common
def test_sample_dt_holds_each_sample_and_ignores_gaps(spark: SparkSession) -> None:
    times = [T0, T0 + timedelta(seconds=5), T0 + timedelta(seconds=10), T0 + timedelta(minutes=10)]
    df = with_sample_dt(_state(spark, [{"ts": t} for t in times]))
    assert [r.dt_s for r in df.orderBy("event_ts").collect()] == [5.0, 5.0, 0.0, 0.0]


def test_sessionize_splits_on_long_pauses(spark: SparkSession) -> None:
    times = [T0 + timedelta(seconds=s) for s in (0, 5, 10, 400, 405)]
    rows = _state(spark, [{"ts": t, "speed_kph": 50.0} for t in times])
    ids = [
        r.trip_id
        for r in sessionize(rows, F.lit(True), 300, name="trip").orderBy("event_ts").collect()
    ]
    assert len(set(ids)) == 2 and ids[0] == ids[2] and ids[3] == ids[4]


# ---------------------------------------------------------------------- battery
def test_coulomb_counting_recovers_true_capacity(spark: SparkSession) -> None:
    true_capacity_ah, amps, seconds = 150.0, 100.0, 1800
    rows = [
        {
            "ts": T0 + timedelta(seconds=s),
            "is_charging": 1.0,
            "pack_current_a": amps,
            "pack_voltage_v": 380.0,
            "soc_pct": 30.0 + amps * s / 3600 / true_capacity_ah * 100,
            "dc_charge_current_a": amps,
            "batt_temp_max_c": 30.0,
        }
        for s in range(0, seconds + 1, 5)
    ]
    (session,) = battery.charging_sessions(_state(spark, rows), _dim(spark)).collect()
    assert session.charge_in_ah == pytest.approx(50.0)
    assert session.capacity_est_ah == pytest.approx(true_capacity_ah)
    assert session.soh_est_pct == pytest.approx(true_capacity_ah / NOMINAL_AH * 100)
    assert session.energy_in_kwh == pytest.approx(50.0 * 380 / 1000)
    assert session.duration_min == pytest.approx(30.0)


def test_short_top_ups_give_no_soh_estimate(spark: SparkSession) -> None:
    rows = [
        {"ts": T0 + timedelta(seconds=s), "is_charging": 1.0, "pack_current_a": 50.0,
         "pack_voltage_v": 380.0, "soc_pct": 70.0 + s / 600}  # +3 points only
        for s in range(0, 1801, 5)
    ]  # fmt: skip
    (session,) = battery.charging_sessions(_state(spark, rows), _dim(spark)).collect()
    assert session.soh_est_pct is None


def test_daily_thermal_stress_efc_and_risk(spark: SparkSession) -> None:
    rows = []
    for s in range(0, 3601, 5):  # one hour driving at -100 A, hot for the last 10 minutes
        hot = s >= 3000
        rows.append({
            "ts": T0 + timedelta(seconds=s), "speed_kph": 80.0, "pack_current_a": -100.0,
            "pack_voltage_v": 360.0, "batt_temp_max_c": 50.0 if hot else 35.0,
            "cell_v_max": 3.80 + (0.15 if hot else 0.0), "cell_v_min": 3.78,
        })  # fmt: skip
    empty_sessions = spark.createDataFrame(
        [], "vin STRING, session_start TIMESTAMP, soh_est_pct DOUBLE"
    )
    (day,) = battery.daily_health(_state(spark, rows), empty_sessions, _dim(spark)).collect()
    assert day.discharge_ah == pytest.approx(100.0)
    assert day.efc == pytest.approx(100.0 / 2 / NOMINAL_AH)
    assert day.thermal_stress_hours == pytest.approx(600 / 3600)
    assert day.cell_delta_max_mv == pytest.approx(170.0)
    assert day.risk_level == "critical"
    assert day.charging_sessions == 0


# ---------------------------------------------------------------------- DTC precursors
def test_onsets_need_thirty_minutes_of_silence(spark: SparkSession) -> None:
    seconds = [0, 5, 10, 600, 3000, 3005]  # 600 s gap continues, 2400 s gap is a new onset
    events = spark.createDataFrame(
        [("V1", "P", T0 + timedelta(seconds=s), "P0217") for s in seconds],
        "vin STRING, platform_id STRING, event_ts TIMESTAMP, dtc_code STRING",
    )
    got = [r.onset_ts for r in dtc.onsets(events).orderBy("onset_ts").collect()]
    assert got == [T0.replace(tzinfo=None), (T0 + timedelta(seconds=3000)).replace(tzinfo=None)]


def _signals(spark: SparkSession, rows: list[tuple[str, int, float]]) -> DataFrame:
    onset = T0 + timedelta(hours=2)
    return spark.createDataFrame(
        [("V1", onset - timedelta(seconds=back), path, value) for path, back, value in rows],
        "vin STRING, event_ts TIMESTAMP, vss_path STRING, value DOUBLE",
    )


def test_precursor_features_score_level_shifts_and_dropouts(spark: SparkSession) -> None:
    onset = T0 + timedelta(hours=2)
    rows = []
    for back in range(5, 70 * 60, 5):
        in_window = back <= 600
        coolant = 90.0 + (25.0 if in_window else 0.0) + (back % 3) * 0.5
        rows.append(("Vehicle.Coolant", back, coolant))
        if not in_window:  # the RPM signal stops reporting before the code
            rows.append(("Vehicle.Rpm", back, 2000.0 + (back % 7) * 10))
    onset_rows = spark.createDataFrame(
        [("o1", "V1", "P", "P0217", onset)],
        "onset_id STRING, vin STRING, platform_id STRING, dtc_code STRING, onset_ts TIMESTAMP",
    )
    feats = {
        r.vss_path: r for r in dtc.precursor_features(_signals(spark, rows), onset_rows).collect()
    }
    assert feats["Vehicle.Coolant"].driver == "level" and feats["Vehicle.Coolant"].level_z > 20
    assert feats["Vehicle.Rpm"].driver == "dropout" and feats["Vehicle.Rpm"].completeness == 0.0


# ---------------------------------------------------------------------- eco-driving
def test_trips_harsh_events_and_regen(spark: SparkSession) -> None:
    rows = []
    for s in range(0, 601, 5):  # 10-minute trip at 60 km/h on the odometer
        accel = 3.5 if s in (10, 20) else (-3.5 if s == 300 else 0.2)
        current = 40.0 if s % 60 == 0 else -100.0  # brief regen phases
        rows.append({
            "ts": T0 + timedelta(seconds=s), "speed_kph": 60.0, "odometer_m": 1000.0 * s / 60,
            "accel_long_ms2": accel, "pack_current_a": current, "pack_voltage_v": 360.0,
        })  # fmt: skip
    (trip,) = eco.trips(_state(spark, rows)).collect()
    assert trip.distance_km == pytest.approx(10.0)
    assert trip.harsh_accel_events == 2 and trip.harsh_brake_events == 1
    assert 0 < trip.regen_ratio < 1
    assert trip.kwh_per_100km == pytest.approx((trip.energy_out_kwh - trip.regen_kwh) * 10)


def test_platforms_without_acceleration_get_no_harsh_counts(spark: SparkSession) -> None:
    rows = [{"ts": T0 + timedelta(seconds=s), "speed_kph": 50.0} for s in range(0, 601, 5)]
    (trip,) = eco.trips(_state(spark, rows)).collect()
    assert trip.harsh_accel_events is None and trip.distance_km == pytest.approx(50 / 6, rel=0.02)


@pytest.mark.parametrize(
    ("accel", "brake", "consumption", "expected"),
    [
        (0.0, 0.0, 1.0, 100.0),
        (5.0, 5.0, 1.5, 0.0),  # every penalty saturated
        (2.5, 0.0, None, 68.8),  # half the accel penalty, no energy data: rescaled to 100
        (None, None, None, None),
    ],
)
def test_eco_score(
    spark: SparkSession,
    accel: float | None,
    brake: float | None,
    consumption: float | None,
    expected: float | None,
) -> None:
    df = spark.createDataFrame([(accel, brake, consumption)], "a DOUBLE, b DOUBLE, c DOUBLE")
    (row,) = df.select(eco.eco_score(F.col("a"), F.col("b"), F.col("c")).alias("s")).collect()
    assert row.s == expected

"""Data-quality gates: check definitions, verdict logic, stuck-sensor and drift detectors."""

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession

from covesa_lakehouse.common.config_loader import SPEC_SCHEMA, load_contract_dir, spec_rows
from covesa_lakehouse.quality.checks import CHECKS, Check, Result, report
from covesa_lakehouse.quality.rules import stuck_runs, unmapped_items

T0 = datetime(2026, 9, 15, 8, 0, tzinfo=UTC)


@pytest.mark.parametrize("check", CHECKS, ids=lambda c: c.name)
def test_every_check_renders_and_parses(spark: SparkSession, check: Check) -> None:
    sql = check.sql.format(start=date(2026, 9, 1), end=date(2026, 9, 2))
    spark._jsparkSession.sessionState().sqlParser().parsePlan(sql)  # syntax only, no tables
    assert check.op in ("<=", ">=", "==")


def test_privacy_gate_matches_exactly_a_17_character_vin() -> None:
    (gate,) = [c for c in CHECKS if c.name == "bronze_vin_never_raw"]
    assert "[A-HJ-NPR-Z0-9]{17}" in gate.sql.format(start="x", end="y")


def test_only_failed_errors_fail_the_gate() -> None:
    warn = Check("w", "silver", "warn", "==", 0, "", "")
    err = Check("e", "silver", "error", "<=", 0.05, "", "")
    assert report([Result(warn, 3), Result(err, 0.01)], out=lambda _: None)
    assert not report([Result(err, 0.2)], out=lambda _: None)


# ---------------------------------------------------------------------- stuck sensors
def _signals(spark: SparkSession, rows: list[tuple[str, str, int, float]]) -> DataFrame:
    return spark.createDataFrame(
        [(vin, "P", T0 + timedelta(seconds=5 * i), path, path.split(".")[-1], v)
         for vin, path, i, v in rows],
        "vin STRING, platform_id STRING, event_ts TIMESTAMP, vss_path STRING,"
        " signal_name STRING, value DOUBLE",
    )  # fmt: skip


def test_stuck_sensor_is_flagged_while_a_varying_signal_is_not(spark: SparkSession) -> None:
    rows = []
    for vin in ("V1", "V2", "V3"):
        for i in range(200):
            rows.append((vin, "Vehicle.Speed", i, 60.0 + (i % 7)))  # always moving, varying
            frozen = vin == "V1" and 50 <= i < 120  # V1's rpm freezes for 70 samples
            rows.append((vin, "Vehicle.Rpm", i, 2222.0 if frozen else 2000.0 + i))
    runs = stuck_runs(_signals(spark, rows)).collect()
    assert [(r.vin, r.vss_path, r.run_length) for r in runs] == [("V1", "Vehicle.Rpm", 70)]


def test_long_runs_while_parked_or_at_zero_are_not_flagged(spark: SparkSession) -> None:
    rows = []
    for i in range(200):
        rows.append(("V1", "Vehicle.Speed", i, 0.0))  # parked: speed 0 ...
        rows.append(("V1", "Vehicle.TraveledDistance", i, 123456.0))  # ... odometer constant
    assert stuck_runs(_signals(spark, rows)).count() == 0


# ---------------------------------------------------------------------- schema drift
def test_unmapped_signals_and_frames_are_reported(spark: SparkSession) -> None:
    contracts = load_contract_dir(Path("contracts")).contracts
    specs = spark.createDataFrame(spec_rows(contracts), SPEC_SCHEMA)
    renault = {"signals": {"veh_spd_kph": 1, "bms_new_sensor_after_fw_update": 7}}
    vw_frame = next(iter(contracts["VW_MQB"].frames()))
    vw = {"frames": [{"id": vw_frame, "data": "00"}, {"id": 0x7AB, "data": "00"}]}
    bronze = spark.createDataFrame(
        [
            ("RENAULT_CMF_EV", T0, json.dumps(renault)),
            ("VW_MQB", T0, json.dumps(vw)),
            ("NOT_ONBOARDED", T0, json.dumps({"signals": {"x": 1}})),  # Silver's UNKNOWN_PLATFORM
        ],
        "platform_id STRING, ingested_at TIMESTAMP, raw_payload STRING",
    )
    got = {(r.platform_id, r.kind, r.item) for r in unmapped_items(bronze, specs).collect()}
    assert got == {
        ("RENAULT_CMF_EV", "json_signal", "bms_new_sensor_after_fw_update"),
        ("VW_MQB", "can_frame", "0x7AB"),
    }

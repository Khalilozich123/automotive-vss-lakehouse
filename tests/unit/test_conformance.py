"""Silver conformance engine vs. the pure-Python oracles from Phase 1.

* bit level: Spark CAN extraction == ``can_codec`` (itself verified against cantools)
* message level: Spark conformance == ``reference_decoder`` on the captured sample
* anomaly level: the Dead Letter Queue holds exactly what the simulator's ground truth injected
"""

import json
import random
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.bronze.ingest_raw import to_bronze
from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_hex
from covesa_lakehouse.common import can_codec
from covesa_lakehouse.common.config_loader import (
    SPEC_SCHEMA,
    bit_window,
    load_contract_dir,
    spec_rows,
)
from covesa_lakehouse.common.contract import Contract
from covesa_lakehouse.common.reference_decoder import DecodeError, decode_envelope, parse_envelope
from covesa_lakehouse.silver import can_decoder as can
from covesa_lakehouse.silver import data_quality as dq
from covesa_lakehouse.silver.conformance_engine import Conformed, conform

SAMPLE = Path("tests/fixtures/telemetry_sample.jsonl")
TRUTH = Path("tests/fixtures/telemetry_sample.truth.jsonl")
CONTRACTS: dict[str, Contract] = load_contract_dir(Path("contracts")).contracts


def _specs(spark: SparkSession, contracts: dict[str, Contract] = CONTRACTS) -> DataFrame:
    return spark.createDataFrame(spec_rows(contracts), SPEC_SCHEMA)


def _bronze(spark: SparkSession, lines: list[str]) -> DataFrame:
    df = spark.createDataFrame(
        [(line, "s3a://landing/telemetry/batch/x.jsonl") for line in lines],
        "value STRING, source_file STRING",
    )
    return to_bronze(df, b"k", "k1")


@pytest.fixture(scope="module")
def lines() -> list[str]:
    return SAMPLE.read_text(encoding="utf-8").splitlines()


@pytest.fixture(scope="module")
def conformed(spark: SparkSession, lines: list[str]) -> dict[str, list[dict[str, Any]]]:
    out = conform(_bronze(spark, lines), _specs(spark))
    return {
        "signals": [r.asDict() for r in out.signals.collect()],
        "quarantine": [r.asDict() for r in out.quarantine.collect()],
        "dtcs": [r.asDict() for r in out.dtc_events.collect()],
    }


# ---------------------------------------------------------------------- bit level
def test_spark_bit_extraction_matches_the_codec_for_every_contract_layout(
    spark: SparkSession,
) -> None:
    rng = random.Random(0)
    rows, expected = [], []
    for contract in CONTRACTS.values():
        for spec in contract.signals.values():
            lay = spec.can
            w = bit_window(lay.start_bit, lay.length, lay.byte_order)
            for _ in range(20):
                data = bytes(rng.getrandbits(8) for _ in range(lay.dlc))
                rows.append(
                    (
                        data.hex().upper(),
                        w.first_byte,
                        w.n_bytes,
                        w.little_endian,
                        w.shift,
                        lay.length,
                        lay.is_signed,
                    )
                )
                expected.append(
                    can_codec.extract(
                        data, lay.start_bit, lay.length, lay.byte_order, lay.is_signed
                    )
                )
    schema = (
        "data STRING, first_byte INT, n_bytes INT, little_endian BOOLEAN, shift INT,"
        " length INT, is_signed BOOLEAN"
    )
    df = spark.createDataFrame(rows, schema)
    got = df.select(
        can.to_signed(can.extract_unsigned("data"), F.col("length"), F.col("is_signed")).alias(
            "raw"
        )
    ).collect()
    assert [r.raw for r in got] == expected
    # the contracts include big-endian (Motorola) layouts, so both byte orders were exercised
    assert any(
        c.can.byte_order == "big_endian" for x in CONTRACTS.values() for c in x.signals.values()
    )


def test_malformed_frames_are_detected(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("00FF", 2), ("00F", 2), ("00FFAA", 2), ("ZZZZ", 2), (None, 2)], "data STRING, dlc INT"
    )
    got = [
        r.ok
        for r in df.select(
            can.frame_is_well_formed(F.col("data"), F.col("dlc")).alias("ok")
        ).collect()
    ]
    assert got == [True, False, False, False, False]


# ---------------------------------------------------------------------- message level
def test_spark_matches_the_reference_decoder(
    lines: list[str], conformed: dict[str, list[dict[str, Any]]]
) -> None:
    expected: set[tuple[str, str, float]] = set()
    decodable: set[str] = set()
    for line in lines:
        record = json.loads(line)
        try:
            envelope = parse_envelope(record["payload"])
            decoded = decode_envelope(envelope, CONTRACTS[record["topic"].split("/")[1]])
        except DecodeError:
            continue
        decodable.add(envelope["msg_id"])
        expected |= {(envelope["msg_id"], d.vss_path, round(d.value, 6)) for d in decoded}

    got = {(r["msg_id"], r["vss_path"], round(r["value"], 6)) for r in conformed["signals"]}
    got |= {
        (r["msg_id"], r["vss_path"], round(r["decoded_value"], 6))
        for r in conformed["quarantine"]
        if r["error_code"] == dq.PHYSICAL_RANGE_VIOLATION
    }
    assert {g for g in got if g[0] in decodable} == expected
    assert len(expected) > 10_000


def test_dead_letter_queue_matches_the_injected_anomalies(
    lines: list[str], conformed: dict[str, list[dict[str, Any]]]
) -> None:
    truth = [json.loads(t) for t in TRUTH.read_text().splitlines()]
    anomalies = [t for t in truth if t["event"] == "anomaly"]
    quarantine = conformed["quarantine"]
    codes = Counter(r["error_code"] for r in quarantine)
    assert set(codes) <= {dq.PARSE_ERROR, dq.MALFORMED_FRAME, dq.PHYSICAL_RANGE_VIOLATION}

    # every injected out-of-range value is in the DLQ, on the right signal
    oor = {(t["msg_id"], t["signal"]) for t in anomalies if t["value"] == "out_of_range"}
    flagged = {
        (r["msg_id"], r["signal_name"])
        for r in quarantine
        if r["error_code"] == dq.PHYSICAL_RANGE_VIOLATION
    }
    assert oor == flagged

    # every corrupt payload is rejected: truncated JSON as a message, short CAN frames per signal
    corrupt = {t["msg_id"]: t["kind"] for t in anomalies if t["value"] == "corrupt_payload"}
    parse_errors = [r for r in quarantine if r["error_code"] == dq.PARSE_ERROR]
    assert len(parse_errors) == sum(1 for k in corrupt.values() if k == "truncated_json")
    assert all(r["raw_payload"] and r["signal_name"] is None for r in parse_errors)
    short = {r["msg_id"] for r in quarantine if r["error_code"] == dq.MALFORMED_FRAME}
    assert short == {m for m, k in corrupt.items() if k == "short_frame"}


def test_signal_level_quarantine_keeps_the_rest_of_the_message(
    conformed: dict[str, list[dict[str, Any]]],
) -> None:
    bad = next(r for r in conformed["quarantine"] if r["error_code"] == dq.PHYSICAL_RANGE_VIOLATION)
    siblings = [s for s in conformed["signals"] if s["msg_id"] == bad["msg_id"]]
    assert siblings and all(s["vss_path"] != bad["vss_path"] for s in siblings)
    assert bad["raw_payload"] is None and bad["decoded_value"] is not None


def test_dtc_events_are_extracted(
    lines: list[str], conformed: dict[str, list[dict[str, Any]]]
) -> None:
    expected = Counter()
    for line in lines:
        try:
            envelope = parse_envelope(json.loads(line)["payload"])
        except DecodeError:
            continue
        expected.update(envelope.get("dtcs") or [])
    assert expected and Counter(r["dtc_code"] for r in conformed["dtcs"]) == expected


# ---------------------------------------------------------------------- rules
def _conform_one(
    spark: SparkSession,
    payload: dict[str, Any] | str,
    platform: str = "RENAULT_CMF_EV",
    contracts: dict[str, Contract] = CONTRACTS,
) -> Conformed:
    text = payload if isinstance(payload, str) else json.dumps(payload)
    line = json.dumps({"topic": f"vehicles/{platform}/VIN1/telemetry", "payload": text})
    return conform(_bronze(spark, [line]), _specs(spark, contracts))


def _envelope(**overrides: Any) -> dict[str, Any]:
    env = {
        "msg_id": "m1",
        "vin": "VIN1",
        "platform_id": "RENAULT_CMF_EV",
        "hardware_rev": "HW",
        "encoding": "json",
        "ts": "2026-09-01T07:00:00.000Z",
        "seq": 1,
        "signals": {"veh_spd_kph": 1600, "bms_soc_raw": 500},
        "dtcs": [],
    }
    return env | overrides


def test_valid_message_decodes_to_vss(spark: SparkSession) -> None:
    out = _conform_one(spark, _envelope())
    got = {r.vss_path: r.value for r in out.signals.collect()}
    assert got == {
        "Vehicle.Speed": 100.0,
        "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current": 50.0,
    }
    assert out.quarantine.count() == 0


@pytest.mark.parametrize(
    ("payload", "platform", "code"),
    [
        ("{not json", "RENAULT_CMF_EV", dq.PARSE_ERROR),
        (json.dumps({"vin": "x"}), "RENAULT_CMF_EV", dq.PARSE_ERROR),  # no msg_id
        (json.dumps(_envelope()), "NEW_PLATFORM_X", dq.UNKNOWN_PLATFORM),
        (json.dumps(_envelope(encoding="can_frame")), "RENAULT_CMF_EV", dq.ENCODING_MISMATCH),
        (json.dumps(_envelope(ts="yesterday")), "RENAULT_CMF_EV", dq.INVALID_TIMESTAMP),
    ],
)
def test_message_level_errors_quarantine_the_whole_message(
    spark: SparkSession, payload: str, platform: str, code: str
) -> None:
    out = _conform_one(spark, payload, platform)
    (row,) = out.quarantine.collect()
    stored = payload.replace("VIN1", hmac_sha256_hex(b"k", "VIN1"))  # Bronze pseudonymized it
    assert row.error_code == code and row.signal_name is None and row.raw_payload == stored
    assert out.signals.count() == 0


def test_non_integer_raw_values_are_rejected_per_signal(spark: SparkSession) -> None:
    out = _conform_one(
        spark,
        _envelope(signals={"veh_spd_kph": "fast", "bms_soc_raw": 12.5, "bms_cell_temp_max": 70}),
    )
    assert {(r.signal_name, r.error_code) for r in out.quarantine.collect()} == {
        ("veh_spd_kph", dq.INVALID_RAW_VALUE),
        ("bms_soc_raw", dq.INVALID_RAW_VALUE),
    }
    assert [r.vss_path for r in out.signals.collect()] == [
        "Vehicle.Powertrain.TractionBattery.Temperature.Max"
    ]


def test_multiplexed_can_signals_follow_their_selector(spark: SparkSession) -> None:
    gm = CONTRACTS["GM_BOLT_EV"]
    spec = gm.signals["Temp_A"]
    can_gm = gm.model_copy(update={"encoding": "can_frame"})  # same layout, delivered as frames
    contracts = {"GM_BOLT_EV": can_gm}

    def frame(selector: int, raw: int) -> str:
        buf = bytearray(spec.can.dlc)
        assert spec.can.mux is not None
        can_codec.insert(
            buf,
            selector,
            spec.can.mux.start_bit,
            spec.can.mux.length,
            spec.can.mux.byte_order,
            False,
        )
        can_codec.insert(buf, raw, spec.can.start_bit, spec.can.length, spec.can.byte_order, False)
        return buf.hex().upper()

    env = _envelope(platform_id="GM_BOLT_EV", encoding="can_frame", signals=None)
    env["frames"] = [
        {"id": spec.can.frame_id, "ext": False, "data": frame(1, 200)},
        {"id": spec.can.frame_id, "ext": False, "data": frame(0, 140)},
    ]
    out = _conform_one(spark, env, "GM_BOLT_EV", contracts)
    temps = [r.value for r in out.signals.collect() if r.signal_name == "Temp_A"]
    assert temps == [140 * spec.scale + spec.offset]  # only the selector-0 frame carries Temp_A

"""Bronze mapping on the captured sample (every platform, every anomaly kind)."""

import json
from pathlib import Path
from typing import Any

import pytest
from pyspark.sql import DataFrame, SparkSession

from covesa_lakehouse.bronze.ingest_raw import to_bronze
from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_hex

KEY, KEY_ID = b"test-key", "test-1"
SAMPLE = Path("tests/fixtures/telemetry_sample.jsonl")
SOURCE_FILE = "s3a://landing/telemetry/batch/dt=2026-09-01/hour=07/drop-sample.jsonl"


@pytest.fixture(scope="module")
def lines() -> list[str]:
    return SAMPLE.read_text(encoding="utf-8").splitlines()


@pytest.fixture(scope="module")
def bronze(spark: SparkSession, lines: list[str]) -> list[dict[str, Any]]:
    df = spark.createDataFrame(
        [(line, SOURCE_FILE) for line in lines], "value STRING, source_file STRING"
    )
    return [r.asDict() for r in to_bronze(df, KEY, KEY_ID).collect()]


def _bronze_of(spark: SparkSession, *values: str) -> list[dict[str, Any]]:
    df: DataFrame = spark.createDataFrame(
        [(v, SOURCE_FILE) for v in values], "value STRING, source_file STRING"
    )
    return [r.asDict() for r in to_bronze(df, KEY, KEY_ID).collect()]


def test_one_row_per_line_nothing_dropped(lines: list[str], bronze: list[dict[str, Any]]) -> None:
    assert len(bronze) == len(lines)
    assert len({r["ingestion_id"] for r in bronze}) == len(bronze)


def test_platform_and_vin_come_from_the_topic_even_for_corrupt_payloads(
    lines: list[str], bronze: list[dict[str, Any]]
) -> None:
    expected = []
    for line in lines:
        _, platform, vin, _ = json.loads(line)["topic"].split("/")
        expected.append((platform, hmac_sha256_hex(KEY, vin)))
    assert sorted((r["platform_id"], r["vin"]) for r in bronze) == sorted(expected)
    assert {r["vin_key_id"] for r in bronze} == {KEY_ID}


def test_no_raw_vin_survives_anywhere(lines: list[str], bronze: list[dict[str, Any]]) -> None:
    vins = {json.loads(line)["topic"].split("/")[2] for line in lines}
    blob = "\n".join(str(r) for r in bronze)
    assert not [v for v in vins if v in blob]


def test_envelope_metadata_is_lifted_only_from_valid_json(
    lines: list[str], bronze: list[dict[str, Any]]
) -> None:
    def parses(payload: str) -> bool:
        try:
            json.loads(payload)
        except json.JSONDecodeError:
            return False
        return True

    unparseable = sum(1 for line in lines if not parses(json.loads(line)["payload"]))
    assert unparseable > 0
    assert sum(1 for r in bronze if r["hardware_rev"] is None) == unparseable
    assert all(r["source_system"] == "batch" and r["source_file"] == SOURCE_FILE for r in bronze)


def test_payload_is_verbatim_apart_from_the_pseudonym(
    lines: list[str], bronze: list[dict[str, Any]]
) -> None:
    by_hash = {r["raw_payload"] for r in bronze}
    for line in lines[:200]:
        record = json.loads(line)
        vin = record["topic"].split("/")[2]
        assert record["payload"].replace(vin, hmac_sha256_hex(KEY, vin)) in by_hash


def test_plain_envelope_lines_fall_back_to_envelope_fields(spark: SparkSession) -> None:
    envelope = json.dumps({"vin": "WVWZZZAUZHW000001", "platform_id": "VW_MQB", "msg_id": "m1"})
    (row,) = _bronze_of(spark, envelope)
    assert row["platform_id"] == "VW_MQB"
    assert row["vin"] == hmac_sha256_hex(KEY, "WVWZZZAUZHW000001")
    assert row["msg_id"] == "m1"
    assert "WVWZZZAUZHW000001" not in row["raw_payload"]


def test_unrecognisable_lines_are_still_landed(spark: SparkSession) -> None:
    (row,) = _bronze_of(spark, "\x00\x01 not json at all")
    assert row["raw_payload"] == "\x00\x01 not json at all"
    assert row["vin"] is None and row["platform_id"] is None and row["vin_key_id"] is None
    assert row["payload_sha256"] is not None and row["ingested_at"] is not None

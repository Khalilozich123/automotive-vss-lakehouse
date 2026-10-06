"""Guards the captured sample payloads that later phases (Bronze, Silver) test against."""

import json
from collections import Counter
from pathlib import Path

import pytest

from covesa_lakehouse.common.contract import Contract, load_contracts
from covesa_lakehouse.common.reference_decoder import DecodeError, decode_envelope, parse_envelope

FIXTURES = Path("tests/fixtures")


@pytest.fixture(scope="module")
def records() -> list[dict[str, str]]:
    lines = (FIXTURES / "telemetry_sample.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


@pytest.fixture(scope="module")
def truth() -> list[dict[str, object]]:
    lines = (FIXTURES / "telemetry_sample.truth.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


def test_fixture_covers_every_platform_anomaly_and_fault(
    records: list[dict[str, str]], truth: list[dict[str, object]]
) -> None:
    platforms = {r["topic"].split("/")[1] for r in records}
    assert platforms == set(load_contracts(Path("contracts")))
    anomalies = Counter(t["value"] for t in truth if t["event"] == "anomaly")
    assert len(anomalies) == 6
    assert any(t["event"] == "dtc_raised" for t in truth)


def test_fixture_matches_its_ground_truth(
    records: list[dict[str, str]], truth: list[dict[str, object]]
) -> None:
    contracts: dict[str, Contract] = load_contracts(Path("contracts"))
    by_id = {t["msg_id"]: t for t in truth if t["event"] == "anomaly"}
    corrupt = out_of_range = 0
    for record in records:
        platform = record["topic"].split("/")[1]
        try:
            envelope = parse_envelope(record["payload"])
            decoded = decode_envelope(envelope, contracts[platform])
        except DecodeError:
            corrupt += 1
            continue
        expected = by_id.get(envelope["msg_id"], {}).get("value")
        flagged = [d for d in decoded if not d.in_range]
        if expected == "out_of_range":
            out_of_range += 1
            assert flagged
        elif expected not in ("stuck_value",):
            assert not flagged, envelope["msg_id"]
    assert corrupt == sum(1 for t in by_id.values() if t["value"] == "corrupt_payload")
    assert out_of_range == sum(1 for t in by_id.values() if t["value"] == "out_of_range")

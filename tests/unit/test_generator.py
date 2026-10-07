"""Property tests for the fleet simulator: its output must decode back through the contracts."""

import json
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from covesa_lakehouse.common.contract import Contract, load_contracts
from covesa_lakehouse.common.reference_decoder import DecodeError, decode_envelope, parse_envelope
from covesa_lakehouse.data_generator.generate_telemetry import SimVehicle, build_fleet
from covesa_lakehouse.data_generator.payloads import Message
from covesa_lakehouse.data_generator.vin import check_digit, make_vin

START = datetime(2026, 9, 1, 6, 0, tzinfo=UTC)


@pytest.fixture(scope="module")
def contracts() -> dict[str, Contract]:
    return load_contracts(Path("contracts"))


def _run(
    fleet: list[SimVehicle], seconds: int, interval: float = 1.0
) -> list[tuple[SimVehicle, Message]]:
    out = []
    for t in range(int(seconds / interval)):
        ts = START + timedelta(seconds=t * interval)
        for v in fleet:
            snapshot, dtcs, _ = v.advance(interval)
            out.extend((v, m) for m in v.encoder.encode(snapshot, ts, dtcs))
    return out


@pytest.fixture(scope="module")
def messages(contracts: dict[str, Contract]) -> list[tuple[SimVehicle, Message]]:
    fleet = build_fleet(contracts, len(contracts), seed=1, anomaly_rate=0.2, fault_rate=0.0)
    return _run(fleet, 1800)


def test_clean_messages_decode_within_plausibility_bounds(
    messages: list[tuple[SimVehicle, Message]],
) -> None:
    checked = 0
    for vehicle, message in messages:
        if message.anomaly in ("out_of_range", "corrupt_payload", "stuck_value"):
            continue
        decoded = decode_envelope(parse_envelope(message.payload), vehicle.contract)
        bad = [d for d in decoded if not d.in_range]
        assert not bad, f"{vehicle.contract.platform_id}: {bad}"
        checked += len(decoded)
    assert checked > 50_000


def test_out_of_range_anomalies_are_always_detectable(
    messages: list[tuple[SimVehicle, Message]],
) -> None:
    oor = [(v, m) for v, m in messages if m.anomaly == "out_of_range"]
    assert len(oor) > 100
    for vehicle, message in oor:
        decoded = decode_envelope(parse_envelope(message.payload), vehicle.contract)
        flagged = {d.signal for d in decoded if not d.in_range}
        assert message.anomaly_detail["signal"] in flagged


def test_corrupt_payloads_fail_to_decode(messages: list[tuple[SimVehicle, Message]]) -> None:
    corrupt = [(v, m) for v, m in messages if m.anomaly == "corrupt_payload"]
    assert corrupt
    for vehicle, message in corrupt:
        with pytest.raises(DecodeError):
            decode_envelope(parse_envelope(message.payload), vehicle.contract)


def test_anomaly_rate_and_mix(messages: list[tuple[SimVehicle, Message]]) -> None:
    kinds = Counter(m.anomaly for _, m in messages if m.anomaly)
    share = sum(kinds.values()) / len(messages)
    assert 0.17 < share < 0.25  # configured 0.2 (+ duplicates add a second copy)
    assert set(kinds) == {
        "out_of_range",
        "missing_signal",
        "stuck_value",
        "corrupt_payload",
        "duplicate",
        "late_arrival",
    }


def test_missing_signal_on_can_drops_the_whole_frame(
    messages: list[tuple[SimVehicle, Message]],
) -> None:
    can_missing = [
        (v, m)
        for v, m in messages
        if m.anomaly == "missing_signal" and v.contract.encoding == "can_frame"
    ]
    assert can_missing
    for _, message in can_missing:
        frame_ids = {f["id"] for f in json.loads(message.payload)["frames"]}
        assert message.anomaly_detail["frame_id"] not in frame_ids


def test_simulation_is_reproducible(contracts: dict[str, Contract]) -> None:
    def payloads() -> list[str]:
        fleet = build_fleet(contracts, 4, seed=7, anomaly_rate=0.05, fault_rate=1.0)
        return [m.payload for _, m in _run(fleet, 120)]

    assert payloads() == payloads()


def test_fault_scenarios_raise_their_dtc_after_a_precursor(
    contracts: dict[str, Contract],
) -> None:
    fleet = build_fleet(
        {"RENAULT_CMF_EV": contracts["RENAULT_CMF_EV"]}, 3, seed=3, anomaly_rate=0.0, fault_rate=20
    )
    events = []
    for _ in range(3 * 3600):
        for v in fleet:
            snapshot = v.sim.step(1.0)
            _, ev = v.faults.step(snapshot, 1.0)
            events.extend(ev)
    started = sum(1 for e, _ in events if e == "scenario_started")
    raised = [code for e, code in events if e == "dtc_raised"]
    assert started >= 3
    assert set(raised) <= {"P0A7E", "P0A80", "U0100"}  # only BEV-applicable codes


def test_message_ids_are_unique_across_runs_with_the_same_seed(
    contracts: dict[str, Contract],
) -> None:
    def ids(start: datetime) -> set[str]:
        fleet = build_fleet(contracts, 3, seed=5, anomaly_rate=0.0, fault_rate=0.0)
        out = set()
        for t in range(50):
            for v in fleet:
                snapshot, dtcs, _ = v.advance(1.0)
                out |= {
                    m.msg_id for m in v.encoder.encode(snapshot, start + timedelta(seconds=t), dtcs)
                }
        return out

    assert not ids(START) & ids(START + timedelta(days=1))


def test_publish_rate_does_not_change_the_physics(contracts: dict[str, Contract]) -> None:
    """Publishing every 20 s must sample the same 1 s physics, not take 20 s Euler steps."""

    def speeds(interval: float) -> list[float]:
        fleet = build_fleet(contracts, 2, seed=11, anomaly_rate=0.0, fault_rate=0.0)
        samples = []
        for _ in range(int(3600 / interval)):
            snapshot, _, _ = fleet[0].advance(interval)
            samples.append(snapshot["Vehicle.Speed"])
        return samples

    assert speeds(20.0) == pytest.approx(speeds(1.0)[19::20])


def test_vins_are_structurally_valid() -> None:
    rng = random.Random(0)
    for platform in ("VW_MQB", "RENAULT_CMF_EV", "TESLA_MODEL_S"):
        vin = make_vin(platform, rng)
        assert len(vin) == 17
        assert not set(vin) & set("IOQ")
        assert vin[8] == check_digit(vin)

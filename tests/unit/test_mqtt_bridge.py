import contextlib
import json
from datetime import UTC, datetime

from covesa_lakehouse.bronze.landing import object_key
from covesa_lakehouse.bronze.mqtt_bridge import LandingBuffer

NOW = datetime(2026, 9, 1, 7, 30, 15, 123000, tzinfo=UTC)


class FakeWriter:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    def put(self, key: str, data: bytes) -> None:
        self.objects[key] = data


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _buffer(writer: FakeWriter, clock: FakeClock, **kw: float) -> LandingBuffer:
    return LandingBuffer(writer, "bridge-1", clock=clock, now=lambda: NOW, **kw)  # type: ignore[arg-type]


def test_flushes_when_full_and_returns_ids_to_ack() -> None:
    writer, clock = FakeWriter(), FakeClock()
    buf = _buffer(writer, clock, max_records=3)
    for mid in (11, 12):
        buf.add("vehicles/P/V/telemetry", b'{"a":1}', mid)
    assert not buf.due()
    buf.add("vehicles/P/V/telemetry", b'{"a":1}', 13)
    assert buf.due()
    assert buf.flush() == [11, 12, 13]
    assert len(buf) == 0 and buf.flushed_records == 3
    assert not buf.due()


def test_flushes_partial_batches_by_age() -> None:
    writer, clock = FakeWriter(), FakeClock()
    buf = _buffer(writer, clock, max_records=1000, max_age_s=5.0)
    buf.add("t", b"x", 1)
    clock.t = 4.9
    assert not buf.due()
    clock.t = 5.0
    assert buf.due()


def test_nothing_is_acknowledged_before_it_is_written() -> None:
    class FailingWriter:
        def put(self, key: str, data: bytes) -> None:
            raise OSError("object store unavailable")

    buf = LandingBuffer(FailingWriter(), "b", max_records=1)
    buf.add("t", b"x", 7)
    with contextlib.suppress(OSError):
        buf.flush()
    assert len(buf) == 1  # still buffered; message 7 was never acked, so the broker keeps it


def test_qos0_messages_have_nothing_to_ack() -> None:
    writer, clock = FakeWriter(), FakeClock()
    buf = _buffer(writer, clock)
    buf.add("t", b"x", 0)
    assert buf.flush() == []


def test_landing_object_layout_and_line_format() -> None:
    writer, clock = FakeWriter(), FakeClock()
    buf = _buffer(writer, clock)
    buf.add("vehicles/VW_MQB/WVW1/telemetry", b'{"msg_id":"m"', 1)  # truncated payload kept as-is
    buf.add("vehicles/VW_MQB/WVW1/telemetry", b"\xff\xfe", 2)  # undecodable bytes kept visible
    buf.flush()
    ((key, data),) = writer.objects.items()
    assert key == object_key("mqtt", NOW, "bridge-1", 1)
    assert key == "telemetry/mqtt/dt=2026-09-01/hour=07/part-20260901T073015-bridge-1-000001.jsonl"
    first, second = (json.loads(line) for line in data.decode().splitlines())
    assert first == {
        "topic": "vehicles/VW_MQB/WVW1/telemetry",
        "payload": '{"msg_id":"m"',
        "received_at": "2026-09-01T07:30:15.123Z",
    }
    assert second["payload"] == "��"

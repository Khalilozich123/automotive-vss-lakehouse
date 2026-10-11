"""MQTT -> landing-zone bridge (Spark has no maintained MQTT source).

Delivery guarantee: at-least-once, end to end.

* Persistent MQTT session (``clean_session=False``, fixed client id): the broker queues QoS 1
  messages while the bridge is down.
* Manual acknowledgement: a message is acknowledged (PUBACK) only after the batch that contains
  it has been written to object storage. A crash between write and ack causes redelivery, i.e. a
  duplicate in the landing zone, never a loss. Duplicates are removed in Silver by ``msg_id``.

The bridge does not parse payloads: corrupt messages are landed exactly as received.

    uv run python -m covesa_lakehouse.bronze.mqtt_bridge
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import sys
import time
from collections.abc import Callable
from datetime import UTC, datetime

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.reasoncodes import ReasonCode

from covesa_lakehouse.bronze.landing import ObjectWriter, S3Landing, Source, object_key

TOPIC_FILTER = "vehicles/#"


class LandingBuffer:
    """Accumulates messages and flushes them as one landing object (size- or age-triggered)."""

    def __init__(
        self,
        writer: ObjectWriter,
        writer_id: str,
        max_records: int = 2000,
        max_age_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        source: Source = "mqtt",
    ) -> None:
        self.source: Source = source
        self.writer = writer
        self.writer_id = writer_id
        self.max_records = max_records
        self.max_age_s = max_age_s
        self.clock = clock
        self.now = now
        self.seq = 0
        self.flushed_records = 0
        self._lines: list[str] = []
        self._mids: list[int] = []
        self._first_at: float | None = None

    def __len__(self) -> int:
        return len(self._lines)

    def add(self, topic: str, payload: bytes, mid: int) -> None:
        line = {
            "topic": topic,
            # Payloads are text in this fleet; undecodable bytes are kept visible, not dropped.
            "payload": payload.decode("utf-8", errors="replace"),
            "received_at": self.now().isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }
        self._lines.append(json.dumps(line, separators=(",", ":")))
        if mid:
            self._mids.append(mid)
        if self._first_at is None:
            self._first_at = self.clock()

    def due(self) -> bool:
        if not self._lines:
            return False
        assert self._first_at is not None
        return (
            len(self._lines) >= self.max_records or self.clock() - self._first_at >= self.max_age_s
        )

    def flush(self) -> list[int]:
        """Write the buffered batch; return the message ids that are now safe to acknowledge."""
        if not self._lines:
            return []
        self.seq += 1
        key = object_key(self.source, self.now(), self.writer_id, self.seq)
        self.writer.put(key, ("\n".join(self._lines) + "\n").encode("utf-8"))
        mids = self._mids
        self.flushed_records += len(self._lines)
        self._lines, self._mids, self._first_at = [], [], None
        return mids


def run(args: argparse.Namespace) -> None:
    buffer = LandingBuffer(S3Landing(args.bucket), args.client_id, args.max_records, args.max_age)
    client = mqtt.Client(
        CallbackAPIVersion.VERSION2,
        client_id=args.client_id,
        clean_session=False,
        protocol=mqtt.MQTTv311,
        manual_ack=True,
    )

    def on_connect(
        c: mqtt.Client, _ud: object, _flags: object, reason: ReasonCode, _props: object
    ) -> None:
        print(f"connected to {args.mqtt_host}:{args.mqtt_port} ({reason})", file=sys.stderr)
        c.subscribe(TOPIC_FILTER, qos=1)

    def on_message(_c: mqtt.Client, _ud: object, msg: mqtt.MQTTMessage) -> None:
        buffer.add(msg.topic, msg.payload, msg.mid if msg.qos > 0 else 0)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(args.mqtt_host, args.mqtt_port, keepalive=30)

    stopping = False

    def stop(*_: object) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_report = time.monotonic()
    backoff = 1.0
    while not stopping:
        # Single-threaded loop: callbacks, flushes and acks never race. Unlike loop_forever(),
        # a manual loop() does not reconnect by itself.
        if client.loop(timeout=0.5) != mqtt.MQTT_ERR_SUCCESS:
            print(f"connection lost; reconnecting in {backoff:.0f}s", file=sys.stderr)
            time.sleep(backoff)
            try:
                client.reconnect()
                backoff = 1.0
            except OSError:
                backoff = min(backoff * 2, 30.0)
            continue
        if buffer.due():
            for mid in buffer.flush():
                client.ack(mid, 1)
        if time.monotonic() - last_report > 30:
            print(
                f"landed {buffer.flushed_records:,} records in {buffer.seq} files", file=sys.stderr
            )
            last_report = time.monotonic()

    for mid in buffer.flush():
        client.ack(mid, 1)
    client.loop(timeout=1.0)
    client.disconnect()
    print(f"stopped: landed {buffer.flushed_records:,} records", file=sys.stderr)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--bucket", default=os.environ.get("LANDING_BUCKET", "landing"))
    p.add_argument(
        "--client-id",
        default=os.environ.get("BRIDGE_CLIENT_ID", f"bronze-bridge-{socket.gethostname()}"),
    )
    p.add_argument("--max-records", type=int, default=2000)
    p.add_argument("--max-age", type=float, default=5.0, help="seconds before a partial flush")
    return p.parse_args(argv)


if __name__ == "__main__":
    run(parse_args())

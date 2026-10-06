"""Encode VSS snapshots into platform-native TCU messages, and inject data-quality anomalies.

Message envelope (published as JSON on ``vehicles/<platform_id>/<vin>/telemetry``)::

    {"msg_id", "vin", "platform_id", "hardware_rev", "encoding", "ts", "seq",
     "signals": {"<raw signal>": <raw int>, ...}          # encoding == "json"
     "frames":  [{"id": 253, "ext": false, "data": "<hex>"}, ...]   # encoding == "can_frame"
     "dtcs": ["P0A7E", ...]}

Anomalies are what the Silver layer must catch without stopping the stream. They are injected
after physically valid encoding, and each one is recorded in an optional ground-truth log.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from covesa_lakehouse.common import can_codec
from covesa_lakehouse.common.contract import Contract, SignalSpec

ANOMALY_WEIGHTS = {
    "out_of_range": 0.35,  # physically impossible value -> Silver quarantine (DLQ)
    "missing_signal": 0.20,  # a signal / frame absent from the message
    "stuck_value": 0.15,  # sensor frozen at one raw value for a while
    "corrupt_payload": 0.10,  # truncated JSON / malformed hex frame
    "duplicate": 0.10,  # same message delivered twice (same msg_id)
    "late_arrival": 0.10,  # event time far behind the stream (out-of-order)
}


def to_raw(spec: SignalSpec, value: float) -> int:
    return round((value - spec.offset) / spec.scale)


def to_physical(spec: SignalSpec, raw: int) -> float:
    return raw * spec.scale + spec.offset


def _clip(spec: SignalSpec, raw: int) -> int:
    lo, hi = can_codec.raw_range(spec.can.length, spec.can.is_signed)
    return max(lo, min(hi, raw))


@dataclass
class Message:
    topic: str
    payload: str
    msg_id: str
    anomaly: str | None = None
    anomaly_detail: dict[str, Any] = field(default_factory=dict)


class PayloadEncoder:
    """Per-vehicle encoder: keeps the sequence counter and any stuck-sensor state."""

    def __init__(
        self,
        contract: Contract,
        vin: str,
        hardware_rev: str,
        rng: random.Random,
        anomaly_rate: float,
    ) -> None:
        self.c = contract
        self.vin = vin
        self.hardware_rev = hardware_rev
        self.rng = rng
        self.anomaly_rate = anomaly_rate
        self.seq = 0
        self.topic = f"vehicles/{contract.platform_id}/{vin}/telemetry"
        self._stuck: tuple[str, int, int] | None = None  # (signal, raw, messages left)

    # ------------------------------------------------------------------ encoding
    def _raw_signals(self, snapshot: dict[str, float]) -> dict[str, int]:
        raws: dict[str, int] = {}
        for name, spec in self.c.signals.items():
            if spec.vss_path in snapshot:
                # A real TCU reads these off the bus, so normal values fit the bit field.
                raws[name] = _clip(spec, to_raw(spec, snapshot[spec.vss_path]))
        if self._stuck:
            name, raw, left = self._stuck
            if name in raws:
                raws[name] = raw
            self._stuck = (name, raw, left - 1) if left > 1 else None
        return raws

    def _frames(self, raws: dict[str, int]) -> list[dict[str, Any]]:
        frames: dict[tuple[int, int | None], bytearray] = {}
        meta: dict[tuple[int, int | None], bool] = {}
        for name, raw in raws.items():
            lay = self.c.signals[name].can
            key = (lay.frame_id, lay.mux.value if lay.mux else None)
            buf = frames.setdefault(key, bytearray(lay.dlc))
            meta[key] = lay.is_extended
            if lay.mux:
                can_codec.insert(
                    buf, lay.mux.value, lay.mux.start_bit, lay.mux.length, lay.mux.byte_order, False
                )
            can_codec.insert(buf, raw, lay.start_bit, lay.length, lay.byte_order, lay.is_signed)
        return [
            {"id": frame_id, "ext": meta[(frame_id, mux)], "data": buf.hex().upper()}
            for (frame_id, mux), buf in sorted(frames.items(), key=lambda kv: kv[0][0])
        ]

    def encode(self, snapshot: dict[str, float], ts: datetime, dtcs: list[str]) -> list[Message]:
        self.seq += 1
        msg_id = str(uuid.UUID(int=self.rng.getrandbits(128), version=4))
        raws = self._raw_signals(snapshot)
        anomaly = None
        if self.rng.random() < self.anomaly_rate:
            anomaly = self.rng.choices(
                list(ANOMALY_WEIGHTS), weights=list(ANOMALY_WEIGHTS.values())
            )[0]
        detail: dict[str, Any] = {}

        if anomaly == "out_of_range":
            anomaly, detail = self._out_of_range(raws)
        elif anomaly == "missing_signal" and raws:
            name = self.rng.choice(sorted(raws))
            if self.c.encoding == "json":
                del raws[name]
                detail = {"signal": name}
            else:
                # On a bus a signal cannot vanish on its own: the whole frame is lost.
                frame_id = self.c.signals[name].can.frame_id
                lost = sorted(n for n in raws if self.c.signals[n].can.frame_id == frame_id)
                for n in lost:
                    del raws[n]
                detail = {"frame_id": frame_id, "signals": lost}
        elif anomaly == "stuck_value" and raws:
            name = self.rng.choice(sorted(raws))
            self._stuck = (name, raws[name], self.rng.randint(20, 120))
            detail = {"signal": name, "messages": self._stuck[2]}
        elif anomaly == "late_arrival":
            delay = self.rng.uniform(30, 600)
            ts -= timedelta(seconds=delay)
            detail = {"delay_s": round(delay, 1)}

        envelope: dict[str, Any] = {
            "msg_id": msg_id,
            "vin": self.vin,
            "platform_id": self.c.platform_id,
            "hardware_rev": self.hardware_rev,
            "encoding": self.c.encoding,
            "ts": ts.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "seq": self.seq,
        }
        if self.c.encoding == "json":
            envelope["signals"] = raws
        else:
            envelope["frames"] = self._frames(raws)
        envelope["dtcs"] = dtcs
        payload = json.dumps(envelope, separators=(",", ":"))

        if anomaly == "corrupt_payload":
            payload, detail = self._corrupt(payload, envelope)
        message = Message(self.topic, payload, msg_id, anomaly, detail)
        if anomaly == "duplicate":
            return [message, Message(self.topic, payload, msg_id, anomaly, {"copy": 2})]
        return [message]

    # ------------------------------------------------------------------ anomalies
    def _out_of_range(self, raws: dict[str, int]) -> tuple[str | None, dict[str, Any]]:
        names = sorted(raws)
        self.rng.shuffle(names)
        for name in names:
            spec = self.c.signals[name]
            span = spec.max_val - spec.min_val
            if self.c.encoding == "json":
                # A buggy TCU can send any integer; the bit-field limit does not apply.
                high = self.rng.random() < 0.5
                phys = spec.max_val + span * self.rng.uniform(0.1, 0.6)
                if not high:
                    phys = spec.min_val - span * self.rng.uniform(0.1, 0.6)
                raw = to_raw(spec, phys)
                # Rounding can land back inside a narrow range (e.g. a 0/1 flag): keep nudging.
                step = 1 if high == (spec.scale > 0) else -1
                while spec.min_val <= to_physical(spec, raw) <= spec.max_val:
                    raw += step
            else:
                # On the bus the field is fixed width; use an extreme raw value (e.g. 0xFFF
                # "signal not available") if it decodes outside the plausible range.
                lo, hi = can_codec.raw_range(spec.can.length, spec.can.is_signed)
                candidates = [
                    r for r in (hi, lo) if not spec.min_val <= to_physical(spec, r) <= spec.max_val
                ]
                if not candidates:
                    continue
                raw = self.rng.choice(candidates)
            raws[name] = raw
            return "out_of_range", {
                "signal": name,
                "vss_path": spec.vss_path,
                "physical": round(to_physical(spec, raw), 3),
            }
        return None, {}

    def _corrupt(self, payload: str, envelope: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        if envelope.get("frames") and self.rng.random() < 0.5:
            frame = self.rng.choice(envelope["frames"])
            frame["data"] = frame["data"][: self.rng.randrange(1, len(frame["data"]))]
            return json.dumps(envelope, separators=(",", ":")), {"kind": "short_frame"}
        cut = self.rng.randrange(10, max(11, len(payload) - 1))
        return payload[:cut], {"kind": "truncated_json", "kept_chars": cut}

"""Pure-Python, row-at-a-time decoder for TCU envelopes.

Not used by the pipeline itself: it is the oracle that the Spark conformance engine (Phase 3)
is tested against, and it validates the simulator's output in Phase 1.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from covesa_lakehouse.common import can_codec
from covesa_lakehouse.common.contract import Contract


class DecodeError(ValueError):
    pass


@dataclass(frozen=True)
class DecodedSignal:
    signal: str
    vss_path: str
    value: float
    unit: str
    in_range: bool


def parse_envelope(payload: str) -> dict[str, Any]:
    try:
        envelope = json.loads(payload)
    except json.JSONDecodeError as e:
        raise DecodeError(f"invalid JSON: {e.msg}") from None
    if not isinstance(envelope, dict):
        raise DecodeError("envelope is not a JSON object")
    return envelope


def decode_envelope(envelope: dict[str, Any], contract: Contract) -> list[DecodedSignal]:
    raws: dict[str, int] = {}
    if contract.encoding == "json":
        for name, raw in (envelope.get("signals") or {}).items():
            if name in contract.signals:
                raws[name] = int(raw)
    else:
        frames: dict[int, list[bytes]] = {}
        for frame in envelope.get("frames") or []:
            try:
                frames.setdefault(int(frame["id"]), []).append(bytes.fromhex(frame["data"]))
            except (KeyError, ValueError, TypeError) as e:
                raise DecodeError(f"malformed frame {frame!r}: {e}") from None
        for name, spec in contract.signals.items():
            lay = spec.can
            for data in frames.get(lay.frame_id, []):
                if len(data) != lay.dlc:
                    raise DecodeError(
                        f"frame {lay.frame_id} has {len(data)} bytes, expected {lay.dlc}"
                    )
                if lay.mux:
                    sel = can_codec.extract(
                        data, lay.mux.start_bit, lay.mux.length, lay.mux.byte_order, False
                    )
                    if sel != lay.mux.value:
                        continue
                raws[name] = can_codec.extract(
                    data, lay.start_bit, lay.length, lay.byte_order, lay.is_signed
                )

    decoded = []
    for name, raw in raws.items():
        spec = contract.signals[name]
        value = raw * spec.scale + spec.offset
        decoded.append(
            DecodedSignal(
                name, spec.vss_path, value, spec.unit, spec.min_val <= value <= spec.max_val
            )
        )
    return decoded

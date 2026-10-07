"""Load and validate the contract directory, and flatten it into decoding specs for Spark.

Called at the start of every Silver micro-batch, so a new or changed YAML contract takes effect
without a code change or a restart. A contract that fails validation does not stop the pipeline:
it is reported and skipped, and its platform's messages are quarantined as UNKNOWN_PLATFORM until
the contract is fixed.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from covesa_lakehouse.common.contract import CanLayout, Contract

DEFAULT_CONTRACTS_DIR = Path("contracts")
# The Spark CAN decoder assembles the bytes a signal spans into one 64-bit integer.
MAX_SPAN_BYTES = 7


@dataclass(frozen=True)
class BitWindow:
    """Where a field sits in a frame: the bytes it spans and how to cut it out of them."""

    first_byte: int
    n_bytes: int
    little_endian: bool
    shift: int  # right shift that aligns the field's LSB with bit 0 of the assembled word
    length: int


def bit_window(start_bit: int, length: int, byte_order: str) -> BitWindow:
    if byte_order == "little_endian":
        first, last = start_bit // 8, (start_bit + length - 1) // 8
        return BitWindow(first, last - first + 1, True, start_bit - first * 8, length)
    msb_linear = (start_bit // 8) * 8 + (7 - start_bit % 8)
    lsb_linear = msb_linear + length - 1
    first, last = msb_linear // 8, lsb_linear // 8
    return BitWindow(first, last - first + 1, False, 7 - lsb_linear % 8, length)


@dataclass
class LoadedContracts:
    contracts: dict[str, Contract] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)  # file name -> reason


def _check_layout(name: str, lay: CanLayout) -> None:
    for what, start, length, order in [
        ("signal", lay.start_bit, lay.length, lay.byte_order),
        *(
            [("mux selector", lay.mux.start_bit, lay.mux.length, lay.mux.byte_order)]
            if lay.mux
            else []
        ),
    ]:
        window = bit_window(start, length, order)
        if window.n_bytes > MAX_SPAN_BYTES:
            raise ValueError(f"{name}: {what} spans {window.n_bytes} bytes (max {MAX_SPAN_BYTES})")
        if window.first_byte + window.n_bytes > lay.dlc:
            raise ValueError(f"{name}: {what} overflows the {lay.dlc}-byte frame")


def validate(contract: Contract) -> None:
    """Checks beyond the Pydantic schema: layouts the Spark decoder can actually execute."""
    for name, spec in contract.signals.items():
        _check_layout(name, spec.can)


def load_contract_dir(directory: Path = DEFAULT_CONTRACTS_DIR) -> LoadedContracts:
    loaded = LoadedContracts()
    for path in sorted(directory.glob("*.yaml")):
        try:
            contract = Contract.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
            validate(contract)
        except (ValidationError, ValueError, yaml.YAMLError) as e:
            loaded.errors[path.name] = str(e).splitlines()[0]
            continue
        if contract.platform_id in loaded.contracts:
            loaded.errors[path.name] = f"duplicate platform_id {contract.platform_id}"
            continue
        loaded.contracts[contract.platform_id] = contract
    return loaded


SPEC_SCHEMA = """
    platform_id STRING, contract_version STRING, encoding STRING,
    signal_name STRING, vss_path STRING, unit STRING,
    scale DOUBLE, offset DOUBLE, min_val DOUBLE, max_val DOUBLE,
    frame_id BIGINT, dlc INT, is_signed BOOLEAN, length INT,
    first_byte INT, n_bytes INT, little_endian BOOLEAN, shift INT,
    mux_first_byte INT, mux_n_bytes INT, mux_little_endian BOOLEAN, mux_shift INT,
    mux_length INT, mux_value BIGINT
"""


def spec_rows(contracts: dict[str, Contract]) -> list[tuple[Any, ...]]:
    """One row per (platform, signal) in SPEC_SCHEMA order: the contracts as a join table."""
    rows = []
    for contract in contracts.values():
        for name, spec in contract.signals.items():
            lay = spec.can
            w = bit_window(lay.start_bit, lay.length, lay.byte_order)
            mux: tuple[Any, ...] = (None,) * 6
            if lay.mux:
                m = bit_window(lay.mux.start_bit, lay.mux.length, lay.mux.byte_order)
                mux = (m.first_byte, m.n_bytes, m.little_endian, m.shift, m.length, lay.mux.value)
            rows.append(
                (
                    contract.platform_id,
                    contract.version,
                    contract.encoding,
                    name,
                    spec.vss_path,
                    spec.unit,
                    spec.scale,
                    spec.offset,
                    spec.min_val,
                    spec.max_val,
                    lay.frame_id,
                    lay.dlc,
                    lay.is_signed,
                    lay.length,
                    w.first_byte,
                    w.n_bytes,
                    w.little_endian,
                    w.shift,
                    *mux,
                )
            )
    return rows

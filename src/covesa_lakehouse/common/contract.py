"""Declarative platform contracts: the single source of truth for decoding a vehicle platform.

A contract maps each raw platform signal to a COVESA VSS path with the linear decoding
``physical = raw * scale + offset`` (already expressed in the VSS unit) and physical
plausibility bounds. ``can`` carries the bit layout needed when the platform ships raw
CAN frames instead of decoded JSON.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

Encoding = Literal["json", "can_frame"]
Powertrain = Literal["BEV", "PHEV", "HEV", "ICE"]
ByteOrder = Literal["little_endian", "big_endian"]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class MuxSelector(_Strict):
    """Simple multiplexing: the signal is present only when the selector equals ``value``."""

    signal: str
    start_bit: int = Field(ge=0)
    length: int = Field(ge=1, le=16)
    byte_order: ByteOrder
    value: int = Field(ge=0)


class CanLayout(_Strict):
    message: str
    frame_id: int = Field(ge=0, le=0x1FFFFFFF)
    is_extended: bool = False
    dlc: int = Field(ge=1, le=64)
    start_bit: int = Field(ge=0)
    length: int = Field(ge=1, le=64)
    byte_order: ByteOrder
    is_signed: bool
    mux: MuxSelector | None = None
    # Lineage: the DBC's own scaling before conversion to the VSS unit.
    dbc_scale: float
    dbc_offset: float
    dbc_unit: str = ""


class SignalSpec(_Strict):
    vss_path: str = Field(pattern=r"^Vehicle(\.[A-Za-z0-9]+)+$")
    target_type: Literal["double"] = "double"
    scale: float
    offset: float = 0.0
    unit: str
    min_val: float
    max_val: float
    can: CanLayout
    note: str | None = None

    @model_validator(mode="after")
    def _check(self) -> SignalSpec:
        if self.scale == 0:
            raise ValueError("scale must be non-zero")
        if self.min_val >= self.max_val:
            raise ValueError(f"min_val {self.min_val} must be < max_val {self.max_val}")
        return self


class SourceInfo(_Strict):
    dbc: str
    opendbc_commit: str | None = None
    vss_release: str
    synthetic: bool = False


class VehicleSpec(_Strict):
    """Platform reference data that analytics need (e.g. nominal capacity for SoH and EFC)."""

    battery_kwh: float | None = Field(default=None, gt=0)  # traction battery, nominal
    battery_cells_series: int | None = Field(default=None, gt=0)
    nominal_cell_v: float = 3.7

    @property
    def nominal_capacity_ah(self) -> float | None:
        if self.battery_kwh is None or self.battery_cells_series is None:
            return None
        return self.battery_kwh * 1000.0 / (self.battery_cells_series * self.nominal_cell_v)


class Contract(_Strict):
    platform_id: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    platform_name: str
    version: str = Field(pattern=r"^\d+\.\d+\.\d+$")
    encoding: Encoding
    powertrain: Powertrain
    hardware_revs: list[str] = Field(min_length=1)
    vehicle: VehicleSpec = VehicleSpec()
    source: SourceInfo
    signals: dict[str, SignalSpec] = Field(min_length=1)

    @model_validator(mode="after")
    def _battery_matches_powertrain(self) -> Contract:
        has_battery = self.vehicle.battery_kwh is not None
        if has_battery != (self.powertrain != "ICE"):
            raise ValueError(f"{self.powertrain} platform: battery_kwh must be set iff not ICE")
        return self

    @model_validator(mode="after")
    def _unique_targets(self) -> Contract:
        seen: dict[str, str] = {}
        for name, spec in self.signals.items():
            if spec.vss_path in seen:
                raise ValueError(
                    f"signals {seen[spec.vss_path]!r} and {name!r} both map to {spec.vss_path}"
                )
            seen[spec.vss_path] = name
        return self

    def frames(self) -> dict[int, list[tuple[str, SignalSpec]]]:
        """Signals grouped by CAN frame id (only meaningful for ``can_frame`` encoding)."""
        grouped: dict[int, list[tuple[str, SignalSpec]]] = {}
        for name, spec in self.signals.items():
            grouped.setdefault(spec.can.frame_id, []).append((name, spec))
        return grouped


def load_contract(path: Path) -> Contract:
    return Contract.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))


def load_contracts(directory: Path) -> dict[str, Contract]:
    contracts: dict[str, Contract] = {}
    for path in sorted(directory.glob("*.yaml")):
        contract = load_contract(path)
        if contract.platform_id in contracts:
            raise ValueError(f"duplicate platform_id {contract.platform_id} in {path}")
        contracts[contract.platform_id] = contract
    return contracts

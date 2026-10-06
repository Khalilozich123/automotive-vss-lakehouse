"""Fault scenarios: a precursor drift in the telemetry, followed by a Diagnostic Trouble Code.

Lifecycle per scenario: PRECURSOR (signal drifts, no DTC yet) -> ACTIVE (DTC reported in every
message) -> cleared. The predictive-DTC mart (Phase 4) looks for exactly these antecedents in the
10 minutes before a code appears.
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from covesa_lakehouse.common.contract import Powertrain
from covesa_lakehouse.data_generator.vehicle_model import Mode, VehicleSim

_ANY: tuple[Powertrain, ...] = ("BEV", "PHEV", "HEV", "ICE")
_BATTERY: tuple[Powertrain, ...] = ("BEV", "PHEV", "HEV")
_ENGINE: tuple[Powertrain, ...] = ("ICE", "HEV", "PHEV")

# effect(sim, snapshot, phase, progress 0..1, rng)
Effect = Callable[[VehicleSim, dict[str, float], str, float, random.Random], None]


def _battery_overtemp(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, _: random.Random
) -> None:
    sim.s.extra["pack_temp_offset"] = 1.5 if phase == "precursor" else 0.3  # C per minute


def _cell_imbalance(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, _: random.Random
) -> None:
    sim.s.extra["cell_spread_mv"] = 160.0 * x if phase == "precursor" else 160.0


def _coolant_overtemp(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, _: random.Random
) -> None:
    sim.s.extra["coolant_offset"] = 3.0 if phase == "precursor" else 0.2  # C per minute


def _low_system_voltage(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, _: random.Random
) -> None:
    key = "Vehicle.LowVoltageBattery.CurrentVoltage"
    if key in out:
        out[key] -= 2.8 * x if phase == "precursor" else 3.0


def _misfire(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, _: random.Random
) -> None:
    sim.s.extra["rpm_jitter"] = 50.0 + 250.0 * x if phase == "precursor" else 300.0


def _ecm_comm_loss(
    sim: VehicleSim, out: dict[str, float], phase: str, x: float, rng: random.Random
) -> None:
    drop = 0.6 * x if phase == "precursor" else 0.9
    for key in [k for k in out if k.startswith("Vehicle.Powertrain.")]:
        if rng.random() < drop:
            del out[key]


@dataclass(frozen=True)
class ScenarioType:
    name: str
    dtc: str
    powertrains: tuple[Powertrain, ...]
    precursor_s: tuple[float, float]
    active_s: tuple[float, float]
    effect: Effect
    extras: tuple[str, ...] = ()


#             name                  dtc      applies to  precursor_s  active_s     effect
SCENARIOS = (
    ScenarioType("battery_overtemp",   "P0A7E", _BATTERY, (480, 900), (300, 900),
                 _battery_overtemp, ("pack_temp_offset",)),
    ScenarioType("cell_imbalance",     "P0A80", _BATTERY, (480, 900), (600, 1800),
                 _cell_imbalance, ("cell_spread_mv",)),
    ScenarioType("coolant_overtemp",   "P0217", _ENGINE,  (480, 720), (300, 900),
                 _coolant_overtemp, ("coolant_offset",)),
    ScenarioType("low_system_voltage", "P0562", _ENGINE,  (300, 720), (300, 900),
                 _low_system_voltage),
    ScenarioType("misfire",            "P0300", _ENGINE,  (300, 720), (300, 900),
                 _misfire, ("rpm_jitter",)),
    ScenarioType("ecm_comm_loss",      "U0100", _ANY,     (180, 480), (120, 600),
                 _ecm_comm_loss),
)  # fmt: skip


@dataclass
class ActiveScenario:
    kind: ScenarioType
    precursor_s: float
    active_s: float
    elapsed_s: float = 0.0

    @property
    def phase(self) -> str:
        return "precursor" if self.elapsed_s < self.precursor_s else "active"

    @property
    def done(self) -> bool:
        return self.elapsed_s >= self.precursor_s + self.active_s


class FaultManager:
    """At most one scenario per vehicle; new ones start at ``rate_per_hour`` while driving."""

    def __init__(self, sim: VehicleSim, rng: random.Random, rate_per_hour: float) -> None:
        self.sim = sim
        self.rng = rng
        self.rate_per_hour = rate_per_hour
        self.kinds = [k for k in SCENARIOS if sim.p.powertrain in k.powertrains]
        self.current: ActiveScenario | None = None

    def step(self, out: dict[str, float], dt: float) -> tuple[list[str], list[tuple[str, str]]]:
        """Apply the active scenario to ``out``; return (active DTCs, lifecycle events)."""
        events: list[tuple[str, str]] = []
        if self.current is None:
            started = self.rng.random() < self.rate_per_hour * dt / 3600.0
            if self.kinds and self.sim.s.mode is Mode.DRIVING and started:
                kind = self.rng.choice(self.kinds)
                self.current = ActiveScenario(
                    kind, self.rng.uniform(*kind.precursor_s), self.rng.uniform(*kind.active_s)
                )
                events.append(("scenario_started", kind.name))
            else:
                return [], events

        sc = self.current
        before = sc.phase
        sc.elapsed_s += dt
        if sc.done:
            for extra in sc.kind.extras:
                self.sim.s.extra.pop(extra, None)
            events.append(("dtc_cleared", sc.kind.dtc))
            self.current = None
            return [], events
        if before == "precursor" and sc.phase == "active":
            events.append(("dtc_raised", sc.kind.dtc))
        progress = min(1.0, sc.elapsed_s / sc.precursor_s)
        sc.kind.effect(self.sim, out, sc.phase, progress, self.rng)
        return ([sc.kind.dtc] if sc.phase == "active" else []), events

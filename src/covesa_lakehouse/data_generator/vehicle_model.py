"""Physics-lite vehicle simulator that speaks COVESA VSS.

Each tick produces a snapshot ``{vss_path: physical value in the VSS unit}``. Platform contracts
then encode that snapshot into platform-specific raw values (see ``payloads.py``), i.e. the
generator runs the Silver conformance step in reverse, which gives Phase 3 a known ground truth.

The model is deliberately simple but internally consistent, so Gold KPIs have real structure:
road-load power drives battery current, I^2R heating drives pack temperature, aggressive drivers
produce harsh accelerations, worn batteries (low SoH) show larger cell-voltage spread, etc.
"""

from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from enum import Enum

from covesa_lakehouse.common.contract import Powertrain, VehicleSpec

G = 9.80665
AIR_DENSITY = 1.2
WHEEL_RADIUS_M = 0.31
WHEELBASE_M = 2.7
STEERING_RATIO = 15.0
EV_GEAR_RATIO = 9.7
ICE_GEARS = (3.6, 2.1, 1.4, 1.0, 0.8, 0.65)
ICE_UPSHIFT_KPH = (0, 15, 30, 50, 70, 90)
ICE_FINAL_DRIVE = 3.4
IDLE_RPM = 780.0
NOMINAL_CELL_V = 3.7
MAX_DISCHARGE_W = {"BEV": 150_000.0, "PHEV": 90_000.0, "HEV": 60_000.0, "ICE": 0.0}


class Mode(Enum):
    DRIVING = "driving"
    PARKED = "parked"
    CHARGING = "charging"


@dataclass(frozen=True)
class VehicleProfile:
    powertrain: Powertrain
    mass_kg: float
    cda_m2: float
    crr: float
    battery_kwh: float  # 0 for ICE
    cells_series: int
    state_of_health: float  # 0..1, lowers capacity, raises resistance and cell spread
    aggressiveness: float  # 0 (calm) .. 1 (aggressive driver)
    ambient_c: float

    @classmethod
    def random(
        cls, powertrain: Powertrain, rng: random.Random, vehicle: VehicleSpec | None = None
    ) -> VehicleProfile:
        """Random vehicle of a powertrain type; the battery comes from the contract when known."""
        base = {
            "BEV": (1750.0, 0.62, 60.0, 96),
            "PHEV": (1900.0, 0.72, 18.0, 96),
            "HEV": (1400.0, 0.58, 1.3, 56),
            "ICE": (1450.0, 0.66, 0.0, 0),
        }[powertrain]
        mass, cda, kwh, cells = base
        if vehicle and vehicle.battery_kwh and vehicle.battery_cells_series:
            kwh, cells = vehicle.battery_kwh, vehicle.battery_cells_series
        return cls(
            powertrain=powertrain,
            mass_kg=mass * rng.uniform(0.9, 1.15),
            cda_m2=cda * rng.uniform(0.95, 1.05),
            crr=rng.uniform(0.009, 0.013),
            battery_kwh=kwh,
            cells_series=cells,
            state_of_health=rng.uniform(0.78, 1.0),
            aggressiveness=min(1.0, max(0.0, rng.betavariate(2.0, 3.5))),
            ambient_c=rng.uniform(-2.0, 32.0),
        )

    @property
    def has_battery(self) -> bool:
        return self.powertrain in ("BEV", "PHEV", "HEV")

    @property
    def has_engine(self) -> bool:
        return self.powertrain in ("ICE", "HEV", "PHEV")

    @property
    def capacity_ah(self) -> float:
        return self.battery_kwh * 1000.0 / (self.cells_series * NOMINAL_CELL_V)


@dataclass
class _Trip:
    remaining_s: float
    target_kph: float = 0.0
    segment_left_s: float = 0.0
    road: str = "urban"
    stop_left_s: float = 0.0


@dataclass
class VehicleState:
    mode: Mode = Mode.PARKED
    mode_left_s: float = 0.0
    speed_ms: float = 0.0
    accel_ms2: float = 0.0
    odometer_m: float = 0.0
    curvature: float = 0.0
    soc: float = 0.8
    pack_temp_c: float = 20.0
    coolant_c: float = 20.0
    oil_c: float = 20.0
    fuel_pct: float = 70.0
    ambient_c: float = 20.0
    charge_current_a: float = 0.0
    trip: _Trip | None = None
    extra: dict[str, float] = field(default_factory=dict)


class VehicleSim:
    def __init__(self, profile: VehicleProfile, rng: random.Random) -> None:
        self.p = profile
        self.rng = rng
        self.s = VehicleState(
            odometer_m=rng.uniform(2_000, 180_000) * 1000.0,
            soc=rng.uniform(0.45, 0.95),
            fuel_pct=rng.uniform(25, 95),
            ambient_c=profile.ambient_c,
        )
        self.s.pack_temp_c = self.s.coolant_c = self.s.oil_c = profile.ambient_c
        self.s.mode_left_s = rng.uniform(0, 300)  # stagger fleet start-up

    # ------------------------------------------------------------------ driving logic
    def _pick_segment(self, trip: _Trip) -> None:
        roll = self.rng.random()
        trip.road = "urban" if roll < 0.5 else "rural" if roll < 0.8 else "highway"
        mean = {"urban": 42.0, "rural": 82.0, "highway": 122.0}[trip.road]
        trip.target_kph = max(15.0, self.rng.gauss(mean, 8.0))
        trip.segment_left_s = self.rng.uniform(60, 300)

    def _driver_accel(self, trip: _Trip, dt: float) -> float:
        aggr = self.p.aggressiveness
        if trip.remaining_s <= 0:
            target = 0.0
        else:
            trip.segment_left_s -= dt
            if trip.segment_left_s <= 0:
                self._pick_segment(trip)
            if trip.stop_left_s > 0:
                trip.stop_left_s -= dt
                target = 0.0
            else:
                target = trip.target_kph
                if trip.road == "urban" and self.rng.random() < dt / 90.0:
                    trip.stop_left_s = self.rng.uniform(15, 45)  # traffic light
        tau = 7.0 - 4.0 * aggr
        accel = (target / 3.6 - self.s.speed_ms) / tau
        accel = min(accel, 1.6 + 2.6 * aggr)  # aggressive drivers exceed 3 m/s^2
        accel = max(accel, -(2.2 + 3.5 * aggr))
        return accel + self.rng.gauss(0.0, 0.12)

    def _advance_mode(self, dt: float) -> None:
        s = self.s
        s.mode_left_s -= dt
        if s.mode is Mode.DRIVING:
            assert s.trip is not None
            s.trip.remaining_s -= dt
            if s.trip.remaining_s <= 0 and s.speed_ms < 0.1:
                s.speed_ms = 0.0
                s.trip = None
                wants_charge = self.p.powertrain in ("BEV", "PHEV") and (
                    s.soc < 0.35 or (s.soc < 0.7 and self.rng.random() < 0.15)
                )
                if wants_charge:
                    s.mode, s.mode_left_s = Mode.CHARGING, 7200.0
                else:
                    s.mode, s.mode_left_s = Mode.PARKED, self.rng.uniform(300, 3600)
                    if self.p.has_engine and s.fuel_pct < 12:
                        s.fuel_pct = self.rng.uniform(85, 100)  # refuel
        elif s.mode is Mode.CHARGING:
            if s.soc >= 0.8 or s.mode_left_s <= 0:
                s.mode, s.mode_left_s = Mode.PARKED, self.rng.uniform(120, 900)
        elif s.mode_left_s <= 0:
            s.mode = Mode.DRIVING
            s.trip = _Trip(remaining_s=self.rng.uniform(600, 2700))
            self._pick_segment(s.trip)

    # ------------------------------------------------------------------ physics
    def step(self, dt: float) -> dict[str, float]:
        p, s, rng = self.p, self.s, self.rng
        self._advance_mode(dt)
        driving = s.mode is Mode.DRIVING

        if driving:
            assert s.trip is not None
            s.accel_ms2 = self._driver_accel(s.trip, dt)
            s.speed_ms = max(0.0, s.speed_ms + s.accel_ms2 * dt)
            if s.speed_ms == 0.0:
                s.accel_ms2 = 0.0
        else:
            s.speed_ms, s.accel_ms2 = 0.0, 0.0
        v = s.speed_ms
        s.odometer_m += v * dt

        # Road load: inertia + aero drag + rolling resistance.
        force = p.mass_kg * s.accel_ms2 + 0.5 * AIR_DENSITY * p.cda_m2 * v * v
        force += p.mass_kg * G * p.crr if v > 0 else 0.0
        wheel_power = force * v

        # Lateral dynamics from a mean-reverting road curvature.
        s.curvature += (-s.curvature * 0.05 + rng.gauss(0, 0.0015)) * dt
        kappa_max = min(0.15, 6.0 / max(v * v, 1.0))  # keep lateral accel under ~6 m/s^2
        s.curvature = max(-kappa_max, min(kappa_max, s.curvature))
        steer_deg = math.degrees(math.atan(s.curvature * WHEELBASE_M)) * STEERING_RATIO

        s.ambient_c += rng.gauss(0, 0.002) * dt
        out: dict[str, float] = {
            "Vehicle.Speed": v * 3.6,
            "Vehicle.TraveledDistance": s.odometer_m,
            "Vehicle.Acceleration.Longitudinal": s.accel_ms2,
            "Vehicle.Acceleration.Lateral": v * v * s.curvature if driving else 0.0,
            "Vehicle.AngularVelocity.Yaw": math.degrees(v * s.curvature) if driving else 0.0,
            "Vehicle.Chassis.SteeringWheel.Angle": steer_deg if driving else 0.0,
            "Vehicle.Chassis.Accelerator.PedalPosition": (
                min(100.0, max(0.0, 8.0 + 22.0 * s.accel_ms2 + 0.25 * v * 3.6))
                if driving and s.accel_ms2 > -0.2
                else 0.0
            ),
            "Vehicle.Chassis.Brake.PedalPosition": (
                min(100.0, -s.accel_ms2 * 22.0) if s.accel_ms2 < -0.3 else 0.0
            ),
            "Vehicle.Exterior.AirTemperature": s.ambient_c,
            "Vehicle.LowVoltageBattery.CurrentVoltage": (
                rng.gauss(14.1, 0.05) if s.mode is not Mode.PARKED else rng.gauss(12.6, 0.03)
            ),
        }

        engine_power = 0.0
        if p.has_battery:
            engine_power = self._battery(out, wheel_power, dt)
        else:
            engine_power = max(0.0, wheel_power) / 0.9 + 1500.0 if driving else 0.0
        if p.has_engine:
            self._engine(out, engine_power, driving, dt)
        if p.powertrain in ("BEV", "PHEV"):
            motor_rpm = v / WHEEL_RADIUS_M * EV_GEAR_RATIO * 60 / (2 * math.pi)
            motor_torque = force * WHEEL_RADIUS_M / EV_GEAR_RATIO if driving else 0.0
            for axle in ("Front", "Rear"):
                out[f"Vehicle.Powertrain.ElectricMotor.{axle}.Speed"] = motor_rpm
                out[f"Vehicle.Powertrain.ElectricMotor.{axle}.Torque"] = max(
                    -400.0, min(450.0, motor_torque)
                )
        return out

    def _battery(self, out: dict[str, float], wheel_power: float, dt: float) -> float:
        """Update the traction battery; return the power the engine must supply (hybrids)."""
        p, s, rng = self.p, self.s, self.rng
        engine_power = 0.0
        aux_w = 350.0 + 60.0 * abs(s.ambient_c - 21.0)  # HVAC load grows away from 21 C

        if s.mode is Mode.CHARGING:
            c_rate = 1.5 if s.soc < 0.55 else max(0.15, 1.5 * (0.8 - s.soc) / 0.25)
            s.charge_current_a = min(250.0, p.capacity_ah * c_rate) * rng.uniform(0.97, 1.0)
            current = -s.charge_current_a  # discharge-positive internally
        else:
            s.charge_current_a = 0.0
            if wheel_power >= 0:
                demand = min(wheel_power / 0.9, MAX_DISCHARGE_W[p.powertrain])
            else:
                demand = max(wheel_power * 0.7, -60_000.0)  # regenerative braking
            if p.powertrain == "HEV" or (p.powertrain == "PHEV" and s.soc < 0.2):
                engine_on = demand > 12_000 or s.speed_ms > 18
                if engine_on:
                    engine_power, demand = max(0.0, demand) * 0.85 + 2_000.0, demand * 0.15
            if s.mode is Mode.PARKED:
                demand, aux_w = 0.0, 0.0
            ocv = self._ocv()
            current = (demand + aux_w) / ocv

        resistance = 0.06 * (1 + 1.5 * (1 - p.state_of_health))
        resistance *= 1 + max(0.0, (15.0 - s.pack_temp_c) * 0.03)  # cold cells
        resistance *= p.cells_series / 96.0
        voltage = self._ocv() - current * resistance
        s.soc -= current * dt / 3600.0 / (p.capacity_ah * p.state_of_health)
        s.soc = min(1.0, max(0.03, s.soc))

        heat_w = current * current * resistance
        coolant_target = s.ambient_c if s.pack_temp_c < 35 else 25.0
        heat_capacity = 2_500.0 * p.battery_kwh  # J/K
        s.pack_temp_c += (heat_w - 90.0 * (s.pack_temp_c - coolant_target)) / heat_capacity * dt
        s.pack_temp_c += s.extra.get("pack_temp_offset", 0.0) * dt / 60.0

        spread_mv = 8.0 + 60.0 * (1 - p.state_of_health) + 0.05 * abs(current)
        spread_mv += s.extra.get("cell_spread_mv", 0.0)
        cell_avg = voltage / p.cells_series
        temp_spread = 1.5 + 3.0 * (1 - p.state_of_health)

        out.update(
            {
                "Vehicle.Powertrain.TractionBattery.CurrentVoltage": voltage,
                "Vehicle.Powertrain.TractionBattery.CurrentCurrent": -current,  # VSS: + = charging
                "Vehicle.Powertrain.TractionBattery.StateOfCharge.Current": s.soc * 100.0,
                "Vehicle.Powertrain.TractionBattery.Temperature.Max": s.pack_temp_c + 1.0,
                "Vehicle.Powertrain.TractionBattery.Temperature.Min": s.pack_temp_c
                - temp_spread
                + rng.gauss(0, 0.2),
                "Vehicle.Powertrain.TractionBattery.CellVoltage.Max": cell_avg + spread_mv * 0.6e-3,
                "Vehicle.Powertrain.TractionBattery.CellVoltage.Min": cell_avg - spread_mv * 0.4e-3,
                "Vehicle.Powertrain.TractionBattery.Charging.ChargeCurrent.DC": s.charge_current_a,
                "Vehicle.Powertrain.TractionBattery.Charging.IsCharging": float(
                    s.mode is Mode.CHARGING
                ),
            }
        )
        return engine_power

    def _ocv(self) -> float:
        return self.p.cells_series * (3.25 + 0.95 * self.s.soc)

    def _engine(self, out: dict[str, float], power_w: float, driving: bool, dt: float) -> None:
        p, s, rng = self.p, self.s, self.rng
        running = driving and (p.powertrain == "ICE" or power_w > 0)
        kph = s.speed_ms * 3.6
        if not running:
            rpm, torque = 0.0, 0.0
        elif p.powertrain == "ICE":
            gear = sum(1 for t in ICE_UPSHIFT_KPH if kph >= t)
            ratio = ICE_GEARS[gear - 1] * ICE_FINAL_DRIVE
            rpm = max(IDLE_RPM, s.speed_ms / WHEEL_RADIUS_M * ratio * 60 / (2 * math.pi))
            torque = power_w / (rpm * 2 * math.pi / 60)
        else:  # hybrid: engine runs near its efficient band, decoupled from wheel speed
            rpm = 1200.0 + min(1.0, power_w / 40_000.0) * 3000.0
            torque = power_w / (rpm * 2 * math.pi / 60)
        rpm += s.extra.get("rpm_jitter", 0.0) * rng.gauss(0, 1) if running else 0.0

        coolant_target = 90.0 if running else s.ambient_c
        rate = 1 / 300.0 if running else 1 / 1800.0
        s.coolant_c += (coolant_target - s.coolant_c) * rate * dt
        s.coolant_c += s.extra.get("coolant_offset", 0.0) * dt / 60.0
        s.oil_c += ((100.0 if running else s.ambient_c) - s.oil_c) * rate * 0.5 * dt
        s.fuel_pct -= max(0.0, power_w) / 0.3 / 34.2e6 / 50.0 * 100.0 * dt  # 50 l tank

        out.update(
            {
                "Vehicle.Powertrain.CombustionEngine.Speed": max(0.0, rpm),
                "Vehicle.Powertrain.CombustionEngine.Torque": max(-50.0, min(400.0, torque)),
                "Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature": s.coolant_c,
                "Vehicle.Powertrain.CombustionEngine.EngineOil.Temperature": s.oil_c,
                "Vehicle.Powertrain.FuelSystem.RelativeLevel": max(0.0, s.fuel_pct),
            }
        )

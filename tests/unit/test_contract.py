from typing import Any

import pytest
from pydantic import ValidationError

from covesa_lakehouse.common.contract import Contract


def _signal(vss_path: str, **overrides: Any) -> dict[str, Any]:
    spec = {
        "vss_path": vss_path,
        "scale": 0.1,
        "unit": "km/h",
        "min_val": 0.0,
        "max_val": 260.0,
        "can": {
            "message": "M",
            "frame_id": 1,
            "dlc": 8,
            "start_bit": 0,
            "length": 16,
            "byte_order": "little_endian",
            "is_signed": False,
            "dbc_scale": 0.1,
            "dbc_offset": 0.0,
        },
    }
    return spec | overrides


def _contract(**signals: dict[str, Any]) -> dict[str, Any]:
    return {
        "platform_id": "TEST_PLATFORM",
        "platform_name": "test",
        "version": "1.0.0",
        "encoding": "json",
        "powertrain": "ICE",
        "hardware_revs": ["HW1"],
        "source": {"dbc": "x.dbc", "vss_release": "v6.1"},
        "signals": signals,
    }


def test_valid_contract_loads() -> None:
    contract = Contract.model_validate(_contract(spd=_signal("Vehicle.Speed")))
    assert contract.signals["spd"].offset == 0.0


def test_two_signals_cannot_target_the_same_vss_path() -> None:
    with pytest.raises(ValidationError, match=r"both map to Vehicle\.Speed"):
        Contract.model_validate(_contract(a=_signal("Vehicle.Speed"), b=_signal("Vehicle.Speed")))


@pytest.mark.parametrize(
    "bad",
    [{"scale": 0}, {"min_val": 10.0, "max_val": 10.0}, {"vss_path": "Speed"}, {"extra": 1}],
)
def test_invalid_signal_specs_are_rejected(bad: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        Contract.model_validate(_contract(spd=_signal("Vehicle.Speed") | bad))


def test_battery_spec_must_match_the_powertrain() -> None:
    ice_with_battery = _contract(spd=_signal("Vehicle.Speed")) | {"vehicle": {"battery_kwh": 60}}
    with pytest.raises(ValidationError, match="battery_kwh"):
        Contract.model_validate(ice_with_battery)
    bev = _contract(spd=_signal("Vehicle.Speed")) | {
        "powertrain": "BEV",
        "vehicle": {"battery_kwh": 60, "battery_cells_series": 96},
    }
    capacity = Contract.model_validate(bev).vehicle.nominal_capacity_ah
    assert capacity == pytest.approx(60_000 / (96 * 3.7))


def test_platform_id_must_be_upper_snake_case() -> None:
    with pytest.raises(ValidationError):
        Contract.model_validate(_contract(spd=_signal("Vehicle.Speed")) | {"platform_id": "vw"})

from pathlib import Path

import pytest
import yaml

from covesa_lakehouse.common.contract import load_contracts
from covesa_lakehouse.common.vss import VssCatalog
from covesa_lakehouse.data_generator.dbc_loader import sanitize_dbc
from covesa_lakehouse.data_generator.dbc_parser import (
    MappingError,
    PlatformMapping,
    build_contract,
    generate,
)
from covesa_lakehouse.data_generator.units import conversion_factor, normalize_unit

SOURCES = Path("sources")


@pytest.fixture(scope="module")
def vss() -> VssCatalog:
    return VssCatalog.load(SOURCES / "vss" / "vss.json")


@pytest.fixture(scope="module")
def bounds() -> dict[str, dict[str, float]]:
    return yaml.safe_load((SOURCES / "mappings" / "_bounds.yaml").read_text())  # type: ignore[no-any-return]


def _mapping(**signal: object) -> PlatformMapping:
    return PlatformMapping.model_validate(
        {
            "platform_id": "TEST_PLATFORM",
            "platform_name": "test",
            "powertrain": "BEV",
            "encoding": "json",
            "dbc": "tesla_powertrain.dbc",
            "hardware_revs": ["HW1"],
            "vehicle": {"battery_kwh": 100, "battery_cells_series": 96},
            "signals": [signal],
        }
    )


def test_committed_contracts_are_up_to_date(tmp_path: Path) -> None:
    """CI guard: contracts/ must be exactly what the parser generates from sources/."""
    assert generate(SOURCES, Path("contracts"), check=True) == 0


def test_every_contract_is_valid_and_targets_real_vss_paths(vss: VssCatalog) -> None:
    contracts = load_contracts(Path("contracts"))
    assert len(contracts) >= 10
    for contract in contracts.values():
        for spec in contract.signals.values():
            assert spec.vss_path in vss
            assert spec.unit == vss.leaf(spec.vss_path).unit


def test_unit_conversion_is_folded_into_scale_and_offset(
    vss: VssCatalog, bounds: dict[str, dict[str, float]]
) -> None:
    mapping = _mapping(dbc="DI_torque2.DI_vehicleSpeed", vss="Vehicle.Speed")
    spec = build_contract(mapping, SOURCES / "dbc", vss, bounds).signals["DI_vehicleSpeed"]
    # DBC: 0.05 mph/bit, -25 mph offset  ->  VSS km/h
    assert spec.scale == pytest.approx(0.05 * 1.609344)
    assert spec.offset == pytest.approx(-25 * 1.609344)
    assert spec.unit == "km/h"
    assert spec.can.dbc_unit == "MPH"


def test_spec_example_path_is_rejected_by_vss(
    vss: VssCatalog, bounds: dict[str, dict[str, float]]
) -> None:
    # The spec's example used TractionBattery.Voltage, which VSS 6.1 does not define.
    mapping = _mapping(
        dbc="DI_torque1.DI_motorRPM", vss="Vehicle.Powertrain.TractionBattery.Voltage"
    )
    with pytest.raises(MappingError, match="not a leaf of VSS"):
        build_contract(mapping, SOURCES / "dbc", vss, bounds)


def test_unknown_signal_suggests_close_matches(
    vss: VssCatalog, bounds: dict[str, dict[str, float]]
) -> None:
    mapping = _mapping(dbc="DI_torque1.DI_motorRPN", vss="Vehicle.Speed")
    with pytest.raises(MappingError, match="DI_motorRPM"):
        build_contract(mapping, SOURCES / "dbc", vss, bounds)


def test_multiplexed_signal_records_its_selector() -> None:
    contracts = load_contracts(Path("contracts"))
    mux = contracts["GM_BOLT_EV"].signals["Temp_A"].can.mux
    assert mux is not None
    assert (mux.signal, mux.start_bit, mux.length, mux.value) == ("Switch", 2, 1, 0)


def test_sanitizer_repairs_known_opendbc_defects() -> None:
    text = (
        'BO_ 4000 BIG: 8 X\n SG_ 0_COUNTER : 0|4@1+ (1,0) [0|15] "" Y\n'
        'CM_ SG_ 4000 0_COUNTER "rolling counter";\n'
    )
    fixed, fixes = sanitize_dbc(text)
    assert "SG_ _0_COUNTER" in fixed
    assert f"BO_ {4000 | 0x80000000} BIG" in fixed
    assert f"CM_ SG_ {4000 | 0x80000000} _0_COUNTER" in fixed
    assert len(fixes) == 2


@pytest.mark.parametrize(
    ("raw", "unit"),
    [
        ("Unit_KiloMeterPerHour", "km/h"),
        ("kph", "km/h"),
        ("1/min", "rpm"),
        ("Degré/s", "degrees/s"),
        ("°C", "Celsius"),
        ("", ""),
        ("furlongs", None),
    ],
)
def test_normalize_unit(raw: str, unit: str | None) -> None:
    assert normalize_unit(raw) == unit


def test_unsupported_conversion_raises() -> None:
    with pytest.raises(ValueError, match="no conversion"):
        conversion_factor("rpm", "km/h")

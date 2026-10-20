"""Dashboard logic without a catalog: the attention queue, formatting and sheet rendering."""

import re
from collections.abc import Iterator
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pytest
import streamlit as st
from pyiceberg.exceptions import NoSuchTableError
from streamlit.testing.v1 import AppTest

from covesa_lakehouse.dashboard import data, model, ui

AS_OF = date(2026, 9, 20)
VIN_A, VIN_B, VIN_C = "a" * 64, "b" * 64, "c" * 64
ROOT = Path(__file__).parents[2]
APP = ROOT / "src/covesa_lakehouse/dashboard/app.py"
# A two-day slice of a real (synthetic, pseudonymized) lakehouse run, 19-20 Sep 2026.
SNAPSHOT = ROOT / "tests/fixtures/dashboard"
BATTERY = "Vehicle.Powertrain.TractionBattery.Temperature.Max"


def _onset(onset_id: str, vin: str, code: str, path: str, **kw: object) -> dict[str, object]:
    row: dict[str, object] = {
        "onset_id": onset_id, "vin": vin, "platform_id": "RENAULT_CMF_EV", "dtc_code": code,
        "onset_ts": pd.Timestamp("2026-09-20 10:00", tz="UTC"), "vss_path": path,
        "driver": "excursion", "w_mean": 58.0, "ref_lo": 20.0, "ref_hi": 50.0,
        "b_trend_pred": 40.0, "completeness": 1.0, "volatility_ratio": 1.0,
        "roughness_ratio": 1.0, "score": 9.0,
    }  # fmt: skip
    row.update(kw)
    return row


TABLES = {
    "gold.dtc_precursor_features": pd.DataFrame([
        _onset("o1", VIN_A, "P0A7E", BATTERY),
        _onset("o1", VIN_A, "P0A7E", "Vehicle.Speed", score=12.0),  # scores higher, not learned
        _onset("o2", VIN_B, "P0A80", "Vehicle.Speed"),  # the learned precursor is not reported
    ]),
    "gold.dtc_precursor_ranking": pd.DataFrame([
        {"dtc_code": "P0A7E", "vss_path": BATTERY, "rank": 1},
        {"dtc_code": "P0A80", "vss_path": "Vehicle.Speed", "rank": 7},
    ]),
    "gold.battery_health_daily": pd.DataFrame([
        {"vin": VIN_A, "platform_id": "RENAULT_CMF_EV", "day": AS_OF, "risk_level": "critical",
         "thermal_stress_hours": 1.5, "max_temp_c": 51.0, "cell_delta_max_mv": 140.0},
    ]),
    "gold.eco_driving_daily": pd.DataFrame([
        {"vin": VIN_C, "platform_id": "VW_MQB", "day": AS_OF, "eco_score_7d": 42.0},
        {"vin": VIN_C, "platform_id": "VW_MQB", "day": date(2026, 9, 13), "eco_score_7d": 55.0},
    ]),
    "ops.dq_results": pd.DataFrame([
        {"checked_at": datetime(2026, 9, 20, 8), "layer": "silver", "check_name": "q_rate",
         "severity": "error", "observed": 0.2, "op": "<=", "threshold": 0.05, "passed": False,
         "description": "quarantine rate"},
        {"checked_at": datetime(2026, 9, 20, 9), "layer": "silver", "check_name": "q_rate",
         "severity": "error", "observed": 0.01, "op": "<=", "threshold": 0.05, "passed": True,
         "description": "quarantine rate"},
        {"checked_at": datetime(2026, 9, 20, 9), "layer": "silver", "check_name": "stuck",
         "severity": "warn", "observed": 3.0, "op": "==", "threshold": 0.0, "passed": False,
         "description": "stuck sensor runs"},
    ]),
}  # fmt: skip


@pytest.fixture(autouse=True)
def fake_lakehouse(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    def table(
        name: str, columns: tuple[str, ...] = ("*",), row_filter: str = "true"
    ) -> pd.DataFrame:
        frame = TABLES.get(name, pd.DataFrame())
        return frame if columns == ("*",) or frame.empty else frame[list(columns)]

    monkeypatch.setattr(model, "table", table)
    monkeypatch.setattr(model, "dtc_titles", lambda: {"P0A7E": "Battery over-temperature (HV)"})
    monkeypatch.setattr(model, "units", lambda: {BATTERY: "Celsius"})
    st.cache_data.clear()
    yield
    st.cache_data.clear()


# ---------------------------------------------------------------------- attention queue
def test_attention_ranks_actions_first_one_row_per_vehicle() -> None:
    att = model.attention(AS_OF)
    states = list(att["state"])
    assert states == sorted(states, key=["action", "watch"].index)
    assert att["key"].is_unique
    a = att[att["vin"] == VIN_A].iloc[0]
    assert a["others"] == 1  # its fault and its battery row collapse to one line


def test_onset_is_shown_through_the_learned_precursor() -> None:
    att = model.attention(AS_OF).set_index("vin")
    fault = att.loc[VIN_A]
    assert fault["kind"] == "fault"
    assert fault["issue"] == "P0A7E Battery over-temperature"
    assert fault["signal"] == "TractionBattery.Temperature.Max: 58.0 °C"
    assert fault["deviation"] == "+8.0 °C"  # above the normal range's upper edge
    assert att.loc[VIN_B, "signal"] == "precursor not reported by this platform"


def test_eco_and_failed_gates_reach_the_queue() -> None:
    att = model.attention(AS_OF)
    eco = att[att["vin"] == VIN_C].iloc[0]
    assert (eco["state"], eco["deviation"]) == ("watch", "-13 pts")
    gates = att[att["kind"] == "quality"]
    # Only the latest result per check counts: q_rate has since passed, the warning remains.
    assert list(gates["signal"]) == ["stuck"] and gates.iloc[0]["state"] == "watch"


def test_roster_state_takes_the_worst_issue_per_vehicle() -> None:
    assert model.roster_state(AS_OF) == {VIN_A: "action", VIN_B: "action", VIN_C: "watch"}


# ---------------------------------------------------------------------- formatting
def test_formatting_helpers() -> None:
    assert model.fmt(3.14159, "Celsius") == "3.1 °C"
    assert model.fmt(2.0, "km/h", signed=True) == "+2.0 km/h"
    assert model.fmt(float("nan"), "V") == "—"
    assert model.leaf(BATTERY) == "TractionBattery.Temperature.Max"
    assert model.leaf("Vehicle.Speed") == "Speed"
    assert model.leaf(model.DERIVED_SPREAD) == "Cell voltage spread (derived)"
    aware = model._utc(pd.Timestamp("2026-09-20 12:00", tz="Europe/Paris"))
    assert aware == pd.Timestamp("2026-09-20 10:00") and aware.tzinfo is None


def test_vehicle_codes_are_stable_per_platform_and_hide_the_pseudonym() -> None:
    vins = pd.DataFrame(
        {"vin": [VIN_B, VIN_A, VIN_C], "platform_id": ["X", "RENAULT_CMF_EV", "RENAULT_CMF_EV"]}
    )
    assert data.vehicle_codes(vins) == {VIN_A: "REN-01", VIN_C: "REN-02", VIN_B: "X-01"}
    assert data.short(VIN_A) == "aaaa…aaaa"


def test_units_come_from_the_contracts() -> None:
    u = data.units(Path("contracts"))
    assert u["Vehicle.Speed"] == "km/h"
    assert u[model.DERIVED_SPREAD] == "mV"


# ---------------------------------------------------------------------- rendering
def test_sheet_table_escapes_values_unless_raw() -> None:
    cols = [ui.Col("Issue", "issue"), ui.Col("Tag", "tag", raw=True), ui.Col("N", "n", num=True)]
    html = ui.sheet_table(
        [{"issue": "<script>", "tag": ui.tag("action"), "n": 3, "done": True}], cols, "done"
    )
    assert "&lt;script&gt;" in html and "<script>" not in html
    assert '<span class="tag action">' in html
    assert '<tr class="void">' in html and '<td class="num">3</td>' in html


def test_tolerance_draws_band_limits_and_labels() -> None:
    assert len(ui.tolerance(70, 100, "70 %", "100 %").layer) == 5  # band + 2 rules + 2 labels
    assert len(ui.tolerance(None, 45, label_hi="45 °C").layer) == 2


# ---------------------------------------------------------------------- pages
class _no_tables:  # stands in for data.catalog, which is called like a function
    def load_table(self, name: str) -> None:
        raise NoSuchTableError(name)


def _page(name: str) -> None:
    # AppTest runs this function's source as a standalone script, so it imports for itself.
    from covesa_lakehouse.dashboard import ui, views  # noqa: PLC0415

    ui.inject_css()
    getattr(views, name)()


@pytest.mark.parametrize("page", ["overview", "battery", "faults", "eco", "quality"])
def test_every_page_renders_on_a_fresh_stack(page: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(data, "catalog", _no_tables)  # no Gold or ops tables yet
    monkeypatch.setattr(model, "table", data.read)
    at = AppTest.from_function(_page, args=(page,), default_timeout=30).run()
    assert not at.exception, at.exception


def test_app_shell_renders_status_chips(monkeypatch: pytest.MonkeyPatch) -> None:
    gates = TABLES["ops.dq_results"]  # gate results recorded, Gold not built yet
    monkeypatch.setattr(data, "catalog", _no_tables)
    monkeypatch.setattr(
        model, "table", lambda name, *a: gates if name == "ops.dq_results" else data.read(name, *a)
    )
    at = AppTest.from_file(str(APP), default_timeout=30).run()
    assert not at.exception, at.exception
    status = " ".join(str(h.proto.body) for h in at.get("html"))
    assert "1 warning" in status and "Synthetic data" in status


def _snapshot(
    name: str, columns: tuple[str, ...] = ("*",), row_filter: str = "true"
) -> pd.DataFrame:
    path = SNAPSHOT / f"{name}.parquet"
    if not path.exists():
        return pd.DataFrame(columns=[] if columns == ("*",) else list(columns))
    frame = pd.read_parquet(path)
    if name == "silver.telemetry_signals":  # the onset close-up asks for one vehicle and signal
        vin, path_ = re.search(r"vin == '([^']+)'", row_filter), re.search(
            r"vss_path == '([^']+)'", row_filter
        )
        frame = (
            frame[(frame["vin"] == vin[1]) & (frame["vss_path"] == path_[1])]
            if vin and path_
            else frame
        )
    return frame if columns == ("*",) else frame[[c for c in columns if c in frame]]


@pytest.mark.parametrize("page", ["overview", "battery", "faults", "eco", "quality"])
def test_every_page_renders_real_data(page: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(model, "table", _snapshot)
    monkeypatch.setattr(model, "units", data.units)
    at = AppTest.from_function(_page, args=(page,), default_timeout=60).run()
    assert not at.exception, at.exception
    assert at.get("html") or at.get("arrow_vega_lite_chart")

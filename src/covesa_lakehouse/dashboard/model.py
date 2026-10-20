"""Dashboard data model: Gold/ops tables shaped into what the views show (cached)."""

from __future__ import annotations

import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st
import yaml

from covesa_lakehouse.common.config_loader import load_contract_dir
from covesa_lakehouse.dashboard import data

TTL = 300
DERIVED_SPREAD = "Derived.TractionBattery.CellVoltageSpread"
STRESS_C = 45.0
CELL_WATCH_MV = 100.0
ECO_WATCH = 50.0
TOP_PRECURSORS = 3  # a code's learned precursors: its top-ranked signals in Mart 2


@st.cache_data(ttl=TTL, show_spinner=False)
def table(name: str, columns: tuple[str, ...] = ("*",), row_filter: str = "true") -> pd.DataFrame:
    return data.read(name, columns, row_filter)


@st.cache_data(ttl=TTL, show_spinner=False)
def units() -> dict[str, str]:
    return data.units()


@st.cache_data(ttl=TTL, show_spinner=False)
def dtc_titles() -> dict[str, str]:
    path = Path(os.environ.get("DTC_CATALOG", "sources/dtc/dtc_catalog.yaml"))
    if not path.exists():
        return {}
    return {code: spec["title"] for code, spec in yaml.safe_load(path.read_text()).items()}


@st.cache_data(ttl=TTL, show_spinner=False)
def contracts_summary() -> dict[str, Any]:
    contracts = load_contract_dir(Path(os.environ.get("CONTRACTS_DIR", "contracts"))).contracts
    versions = sorted({c.version for c in contracts.values()})
    vss = sorted({c.source.vss_release for c in contracts.values()})
    return {"count": len(contracts), "versions": versions, "vss": vss}


def leaf(path: str) -> str:
    """Readable tail of a VSS path: 'TractionBattery.Temperature.Max'."""
    if path == DERIVED_SPREAD:
        return "Cell voltage spread (derived)"
    parts = path.split(".")
    return ".".join(parts[-3:]) if len(parts) > 3 else path.removeprefix("Vehicle.")


def _utc(value: object) -> pd.Timestamp:
    """Naive UTC timestamp: Iceberg returns tz-aware values, dates come back naive."""
    t = pd.Timestamp(value)
    return t.tz_convert("UTC").tz_localize(None) if t.tzinfo else t


def fmt_unit(unit: str) -> str:
    return {"Celsius": "°C", "percent": "%", "degrees/s": "°/s"}.get(unit, unit)


def fmt(value: float, unit: str, signed: bool = False) -> str:
    if pd.isna(value):
        return "—"
    unit = fmt_unit(unit)
    text = f"{value:+.1f}" if signed else f"{value:.1f}"
    return f"{text} {unit}".strip()


# ---------------------------------------------------------------------- fleet
@st.cache_data(ttl=TTL, show_spinner=False)
def fleet() -> pd.DataFrame:
    """Every vehicle seen in Gold, with its display code and pseudonym prefix."""
    frames = [
        table("gold.eco_driving_daily", ("vin", "platform_id")),
        table("gold.battery_health_daily", ("vin", "platform_id")),
        table("gold.dtc_precursor_features", ("vin", "platform_id")),
    ]
    vins = pd.concat(frames).drop_duplicates()
    codes = data.vehicle_codes(vins)
    vins = vins.assign(code=vins["vin"].map(codes), pseudo=vins["vin"].map(data.short))
    return vins.sort_values("code").reset_index(drop=True)


def codes() -> dict[str, str]:
    f = fleet()
    return dict(zip(f["vin"], f["code"], strict=True))


@st.cache_data(ttl=TTL, show_spinner=False)
def days() -> list[date]:
    d = pd.concat(
        [table("gold.eco_driving_daily", ("day",)), table("gold.battery_health_daily", ("day",))]
    )
    return sorted({pd.Timestamp(x).date() for x in d["day"].dropna()})


# ---------------------------------------------------------------------- attention queue
def _onset_issue(r: pd.Series, unit: str) -> tuple[str, str]:
    """(signal reading, deviation) for the change that dominates an onset."""
    if r["driver"] == "dropout":
        return "readings missing", f"{(1 - r['completeness']) * 100:.0f} % lost"
    if r["driver"] == "volatility":
        return "jitter", f"×{max(r['volatility_ratio'] or 1, r['roughness_ratio'] or 1):.1f}"
    reading = fmt(r["w_mean"], unit)
    if r["driver"] == "excursion" and pd.notna(r["ref_hi"]):
        edge = r["ref_hi"] if r["w_mean"] > r["ref_hi"] else r["ref_lo"]
        return reading, fmt(r["w_mean"] - edge, unit, signed=True)
    return reading, fmt(r["w_mean"] - r["b_trend_pred"], unit, signed=True)


@st.cache_data(ttl=TTL, show_spinner=False)
def attention(as_of: date) -> pd.DataFrame:
    """Everything out of tolerance on ``as_of`` and the day before, most severe first."""
    lo, u, titles, code = as_of - timedelta(days=1), units(), dtc_titles(), codes()
    rows: list[dict[str, Any]] = []

    feats = table("gold.dtc_precursor_features")
    if not feats.empty:
        feats = feats[pd.to_datetime(feats["onset_ts"]).dt.date.between(lo, as_of)]
        # Show each onset through the precursor the fleet learned for its code (Mart 2's
        # ranking), not through whichever signal happened to score highest this once.
        ranks = table("gold.dtc_precursor_ranking", ("dtc_code", "vss_path", "rank"))
        feats = feats.merge(ranks, on=["dtc_code", "vss_path"], how="left")
        feats["rank"] = feats["rank"].fillna(999)
        top = feats.sort_values(["rank", "score"], ascending=[True, False]).drop_duplicates(
            "onset_id"
        )
        for _, r in top.iterrows():
            if r["rank"] <= TOP_PRECURSORS:
                reading, deviation = _onset_issue(r, u.get(r["vss_path"], ""))
                signal = f"{leaf(r['vss_path'])}: {reading}"
            else:  # e.g. cell imbalance on a platform whose contract has no cell voltages
                signal, deviation = "precursor not reported by this platform", "—"
            title = titles.get(r["dtc_code"], "").split(" (")[0]
            rows.append({
                "state": "action", "vin": r["vin"], "platform": r["platform_id"],
                "issue": f"{r['dtc_code']} {title}".strip(), "signal": signal, "deviation": deviation,
                "when": _utc(r["onset_ts"]), "kind": "fault",
            })  # fmt: skip

    battery = table("gold.battery_health_daily")
    if not battery.empty:
        b = battery[pd.to_datetime(battery["day"]).dt.date == as_of]
        for _, r in b[b["risk_level"] != "normal"].iterrows():
            hot = r["thermal_stress_hours"] > 0
            rows.append({
                "state": "action" if r["risk_level"] == "critical" else "watch",
                "vin": r["vin"], "platform": r["platform_id"],
                "issue": f"Thermal stress {r['thermal_stress_hours']:.1f} h above 45 °C" if hot
                         else f"Cell voltage delta {r['cell_delta_max_mv']:.0f} mV",
                "signal": f"Temperature.Max: {fmt(r['max_temp_c'], 'Celsius')}" if hot
                          else "CellVoltage.Max − Min",
                "deviation": fmt(r["max_temp_c"] - STRESS_C, "Celsius", True) if hot
                             else f"{r['cell_delta_max_mv'] - CELL_WATCH_MV:+.0f} mV",
                "when": _utc(r["day"]), "kind": "battery",
            })  # fmt: skip

    eco = table("gold.eco_driving_daily", ("vin", "platform_id", "day", "eco_score_7d"))
    if not eco.empty:
        eco = eco.assign(day=pd.to_datetime(eco["day"]).dt.date)
        now = eco[eco["day"] == as_of].set_index("vin")
        before = eco[eco["day"] == as_of - timedelta(days=7)].set_index("vin")["eco_score_7d"]
        for vin, r in now[now["eco_score_7d"] < ECO_WATCH].iterrows():
            delta = r["eco_score_7d"] - before.get(vin, float("nan"))
            rows.append({
                "state": "watch", "vin": vin, "platform": r["platform_id"],
                "issue": f"Eco score {r['eco_score_7d']:.0f} / 100 (7 days)",
                "signal": "harsh driving", "deviation": "—" if pd.isna(delta) else f"{delta:+.0f} pts",
                "when": _utc(as_of), "kind": "eco",
            })  # fmt: skip

    gates = latest_gates()
    for _, r in gates[~gates["passed"]].iterrows():
        rows.append({
            "state": "action" if r["severity"] == "error" else "watch", "vin": None,
            "platform": r["layer"].capitalize(), "issue": r["description"],
            "signal": r["check_name"], "deviation": f"{r['observed']:g} {r['op']} {r['threshold']:g}",
            "when": _utc(r["checked_at"]), "kind": "quality",
        })  # fmt: skip

    out = pd.DataFrame(
        rows, columns=["state", "vin", "platform", "issue", "signal", "deviation", "when", "kind"]
    )
    if out.empty:
        return out
    out["code"] = out["vin"].map(lambda v: code.get(v, "—") if v else "Pipeline")
    out["pseudo"] = out["vin"].map(lambda v: data.short(v) if v else "")
    out["rank"] = out["state"].map({"action": 0, "watch": 1})
    out["measured"] = out["deviation"] != "—"  # issues with a measured deviation lead their state
    out = out.sort_values(["rank", "measured", "when"], ascending=[True, False, False]).reset_index(
        drop=True
    )
    # One row per vehicle (its most severe, most recent issue); pipeline rows stay separate.
    out["key"] = out["vin"].fillna(out["signal"])
    out["others"] = out.groupby("key")["key"].transform("size") - 1
    return out.drop_duplicates("key").reset_index(drop=True)


def roster_state(as_of: date) -> dict[str, str]:
    """Worst state per vehicle on the attention queue."""
    att = attention(as_of)
    state: dict[str, str] = {}
    for _, r in att.dropna(subset=["vin"]).iterrows():
        if state.get(r["vin"]) != "action":
            state[r["vin"]] = r["state"]
    return state


# ---------------------------------------------------------------------- quality
@st.cache_data(ttl=TTL, show_spinner=False)
def latest_gates() -> pd.DataFrame:
    """The most recent result of every check (one run may cover one layer only)."""
    res = table("ops.dq_results")
    if res.empty:
        return res.assign(passed=pd.Series(dtype=bool))
    return (
        res.sort_values("checked_at")
        .drop_duplicates("check_name", keep="last")
        .sort_values(["layer", "check_name"])
    )


@st.cache_data(ttl=TTL, show_spinner=False)
def validation() -> pd.DataFrame:
    return table("ops.validation_results")


# ---------------------------------------------------------------------- traces
@st.cache_data(ttl=TTL, show_spinner=False)
def onset_trace(vin: str, vss_path: str, onset: pd.Timestamp) -> pd.DataFrame:
    """The signal from 70 min before an onset to 10 min after (Silver, or derived from state)."""
    start = (onset - pd.Timedelta(minutes=70)).isoformat()
    end = (onset + pd.Timedelta(minutes=10)).isoformat()
    window = f"vin == '{vin}' and event_ts >= '{start}' and event_ts < '{end}'"
    if vss_path == DERIVED_SPREAD:
        s = table("silver.vehicle_state", ("event_ts", "cell_v_max", "cell_v_min"), window)
        return pd.DataFrame(
            {"event_ts": s["event_ts"], "value": (s["cell_v_max"] - s["cell_v_min"]) * 1000}
        )
    s = table(
        "silver.telemetry_signals", ("event_ts", "value"), f"{window} and vss_path == '{vss_path}'"
    )
    return s.sort_values("event_ts")

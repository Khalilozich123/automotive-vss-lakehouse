"""The five views. Each leads with what needs action, then the detail behind it."""

from __future__ import annotations

from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from covesa_lakehouse.dashboard import data, model, ui
from covesa_lakehouse.dashboard.ui import Col

WARRANTY_SOH = 70.0


def titled_as_of(title: str, key: str) -> date:
    """Page title with the as-of day control on the same row (shared across views)."""
    days = model.days()
    left, right = st.columns([3, 2], vertical_alignment="bottom")
    left.title(title)
    if not days:
        ui.empty("No Gold data yet: run the pipeline (Bronze, Silver, Gold) first.")
        st.stop()
    default = st.session_state.get("as_of", days[-1])
    picked = right.select_slider(
        "As of", options=days, value=default if default in days else days[-1],
        format_func=lambda d: d.strftime("%a %d %b %Y"), key=key,
    )  # fmt: skip
    st.session_state["as_of"] = picked
    return picked


def _sessions() -> pd.DataFrame:
    s = model.table("gold.battery_charging_sessions")
    return s.assign(vehicle=s["vin"].map(model.codes()))


# ---------------------------------------------------------------------- overview
def overview() -> None:
    as_of = titled_as_of("Fleet status", "as_of_overview")
    att = model.attention(as_of)
    left, right = st.columns([3, 2], gap="large")

    with left:
        st.header("Out of tolerance")
        if att.empty:
            ui.empty(f"Every vehicle and every quality gate is in specification on {as_of:%d %b}.")
        else:
            rows = [
                {
                    "ref": f'<span class="call">{i}</span>',
                    "vehicle": ui.vehicle(r["code"], r["pseudo"]),
                    "issue": ui.esc(r["issue"])
                    + (
                        f'<br><span class="pseudo">+{r["others"]} more on this vehicle</span>'
                        if r["others"]
                        else ""
                    ),
                    "signal": r["signal"],
                    "dev": f'<span class="dev {r["state"]}">{ui.esc(r["deviation"])}</span>',
                    "state": ui.tag(r["state"]),
                }
                for i, (_, r) in enumerate(att.head(10).iterrows(), start=1)
            ]
            st.html(ui.sheet_table(rows, [
                Col("Ref", "ref", raw=True, width="2.4rem"), Col("Vehicle", "vehicle", raw=True, width="6.6rem"),
                Col("Issue", "issue", raw=True), Col("Signal", "signal", hide_small=True),
                Col("Deviation", "dev", num=True, raw=True, width="6rem"),
                Col("State", "state", raw=True, width="5.2rem"),
            ], fixed=True))  # fmt: skip
            vehicles = att["vin"].notna().sum()
            more = len(att) - 10
            ui.note(
                f"{vehicles} vehicles out of tolerance between {as_of - pd.Timedelta(days=1):%d %b} and "
                f"{as_of:%d %b}, one row each (most severe issue first)."
                + (f" {more} more rows in the views." if more > 0 else "")
            )

    with right:
        st.header("Verified")
        val = model.validation()
        if val.empty:
            ui.empty("No validation recorded: run covesa_lakehouse.gold.validate.")
        else:
            st.html(ui.sheet_table(
                [{"check": r["label"], "value": f"{r['value']:g} {r['unit']}", "n": f"n = {r['sample_size']}",
                  "verdict": ui.tag("info" if r["passed"] else "action", "PASS" if r["passed"] else "FAIL")}
                 for _, r in val.iterrows()],
                [Col("Against simulator ground truth", "check"), Col("Result", "value", num=True),
                 Col("Sample", "n", num=True, hide_small=True), Col("", "verdict", raw=True)],
            ))  # fmt: skip
        gates = model.latest_gates()
        cs = model.contracts_summary()
        passed = int(gates["passed"].sum()) if not gates.empty else 0
        warned = (
            int((~gates["passed"] & (gates["severity"] == "warn")).sum()) if not gates.empty else 0
        )
        failed = len(gates) - passed - warned
        verdict = (
            f"{passed} pass"
            + (f", {warned} warning" if warned else "")
            + (f", {failed} failed" if failed else "")
        )
        st.html(
            '<div class="revblock">'
            f"<div><b>Standard</b>COVESA VSS {', '.join(v.lstrip('v') for v in cs['vss'])}</div>"
            f"<div><b>Contracts</b>{cs['count']} platforms, v{', '.join(cs['versions'])}</div>"
            f"<div><b>Quality checks</b>{len(gates)}: {verdict}</div>"
            f"<div><b>Vehicles</b>{len(model.fleet())}, pseudonymized</div>"
            f"<div><b>As of</b>{as_of:%d %b %Y}</div><div><b>Data</b>Synthetic (simulator)</div>"
            "</div>"
        )

    st.header("Fleet")
    state = model.roster_state(as_of)
    cells = "".join(
        f'<div class="cell {state.get(r["vin"], "")}"><b>{ui.esc(r["code"])}</b><small>{ui.esc(r["pseudo"][:4])}</small></div>'
        for _, r in model.fleet().iterrows()
    )
    st.html(
        f'<div class="fleet">{cells}</div><div class="legend">'
        f'<span><i style="background:var(--rev-wash);border-color:var(--rev)"></i>Action</span>'
        f'<span><i style="background:var(--watch-wash);border-color:var(--watch)"></i>Watch</span>'
        '<span><i style="background:var(--cell)"></i>In specification</span></div>'
    )

    st.header("Battery state of health per charging session")
    s = _sessions().dropna(subset=["soh_est_pct"])
    if s.empty:
        ui.empty("No charging session long enough (≥ 10 SoC points) to estimate SoH yet.")
    else:
        points = alt.Chart(s).mark_point().encode(
            x=alt.X("session_start:T", title=None),
            y=alt.Y("soh_est_pct:Q", title="SoH estimate (%)", scale=alt.Scale(domain=[60, 105])),
            color=alt.Color("vehicle:N", title=None),
            tooltip=["vehicle", alt.Tooltip("session_start:T", title="session"),
                     alt.Tooltip("soh_est_pct:Q", format=".1f", title="SoH %"),
                     alt.Tooltip("energy_in_kwh:Q", format=".1f", title="kWh in")],
        )  # fmt: skip
        ui.chart(ui.tolerance(WARRANTY_SOH, 100, "warranty floor 70 %", "nominal 100 %") + points)


# ---------------------------------------------------------------------- battery
def battery() -> None:
    st.title("Battery health")
    daily = model.table("gold.battery_health_daily")
    if daily.empty:
        ui.empty("No battery data yet.")
        return
    daily = daily.assign(vehicle=daily["vin"].map(model.codes()), day=pd.to_datetime(daily["day"]))
    s = _sessions()
    ui.kv([
        ("Vehicles with battery data", f"{daily['vin'].nunique()}"),
        ("Charging sessions", f"{len(s)}"),
        ("Thermal stress, fleet", f"{daily['thermal_stress_hours'].sum():.1f} h"),
        ("Critical days", f"{(daily['risk_level'] == 'critical').sum()}"),
    ])  # fmt: skip
    ui.note(
        "SoH and Equivalent Full Cycles need battery current, which only RENAULT_CMF_EV (synthetic "
        "contract) reports. GM_BOLT_EV contributes temperature only."
    )

    st.header("SoH per vehicle (same scale)")
    est = s.dropna(subset=["soh_est_pct"])
    if est.empty:
        ui.empty("No charging session long enough (≥ 10 SoC points) to estimate SoH.")
    else:
        # Small multiples on one fixed scale, so vehicles compare honestly.
        vehicles = sorted(est["vehicle"].dropna().unique())
        for start in range(0, len(vehicles), 4):
            cols = st.columns(4)
            for col, name in zip(cols, vehicles[start : start + 4], strict=False):
                one = est[est["vehicle"] == name]
                base = alt.Chart(one).encode(
                    x=alt.X(
                        "session_start:T", title=None, axis=alt.Axis(format="%d %b", tickCount=4)
                    )
                )
                soh = (
                    ui.tolerance(WARRANTY_SOH, 100, "70 %", "100 %")
                    + base.mark_line(color=ui.DIM).encode(
                        y=alt.Y("soh_est_pct:Q", title=None, scale=alt.Scale(domain=[60, 105]))
                    )
                    + base.mark_point(color=ui.DIM).encode(
                        y="soh_est_pct:Q", tooltip=["session_start:T", "soh_est_pct:Q"]
                    )
                ).properties(title=f"{name} · median {one['soh_est_pct'].median():.1f} %")
                with col:
                    ui.chart(soh, height=170)

    c1, c2 = st.columns(2, gap="large")
    with c1:
        st.header("Hottest cell per day")
        temps = (
            alt.Chart(daily)
            .mark_point()
            .encode(
                x=alt.X("day:T", title=None),
                y=alt.Y(
                    "max_temp_c:Q", title="Temperature.Max (°C)", scale=alt.Scale(domain=[0, 60])
                ),
                color=alt.Color("vehicle:N", title=None),
                tooltip=["vehicle", "day:T", "max_temp_c"],
            )
        )
        ui.chart(
            ui.tolerance(0, model.STRESS_C, None, f"stress threshold {model.STRESS_C:g} °C") + temps
        )
    with c2:
        st.header("Cell voltage delta per day")
        cells = daily.dropna(subset=["cell_delta_max_mv"])
        if cells.empty:
            ui.empty("No platform reports both cell voltage extremes.")
        else:
            delta = (
                alt.Chart(cells)
                .mark_point()
                .encode(
                    x=alt.X("day:T", title=None),
                    y=alt.Y("cell_delta_max_mv:Q", title="max − min cell (mV)"),
                    color=alt.Color("vehicle:N", title=None),
                    tooltip=["vehicle", "day:T", "cell_delta_max_mv"],
                )
            )
            ui.chart(ui.tolerance(0, model.CELL_WATCH_MV, None, "watch 100 mV") + delta)

    st.header("Daily record")
    st.dataframe(
        daily.sort_values(["day", "vehicle"], ascending=[False, True])[
            ["day", "vehicle", "platform_id", "risk_level", "thermal_stress_hours", "max_temp_c",
             "cell_delta_max_mv", "efc", "charging_sessions", "soh_est_pct_7d"]
        ],
        hide_index=True, width="stretch",
        column_config={
            "day": st.column_config.DateColumn("Day", format="DD MMM"),
            "platform_id": "Platform", "risk_level": "Risk",
            "thermal_stress_hours": st.column_config.NumberColumn("Stress h > 45 °C", format="%.2f"),
            "max_temp_c": st.column_config.NumberColumn("Max °C", format="%.1f"),
            "cell_delta_max_mv": st.column_config.NumberColumn("Cell Δ mV", format="%.0f"),
            "efc": st.column_config.NumberColumn("EFC", format="%.2f"),
            "charging_sessions": "Sessions",
            "soh_est_pct_7d": st.column_config.NumberColumn("SoH 7 d %", format="%.1f"),
        },
    )  # fmt: skip


# ---------------------------------------------------------------------- faults
def faults() -> None:
    st.title("Fault precursors")
    ranking = model.table("gold.dtc_precursor_ranking")
    feats = model.table("gold.dtc_precursor_features")
    if ranking.empty or feats.empty:
        ui.empty("No fault onsets yet.")
        return
    titles, u = model.dtc_titles(), model.units()
    onsets = feats.drop_duplicates("onset_id")
    counts = onsets["dtc_code"].value_counts()
    code = st.selectbox(
        "Fault code", sorted(counts.index),
        format_func=lambda c: f"{c} · {titles.get(c, '')} · {counts[c]} onsets",
    )  # fmt: skip

    st.header(f"What moves before {code}")
    top = ranking[ranking["dtc_code"] == code].sort_values("rank").head(6)
    st.html(ui.sheet_table(
        [{"rank": int(r["rank"]), "signal": model.leaf(r["vss_path"]), "score": f"{r['median_score']:.1f} σ",
          "share": f"{r['share_above_3_sigma'] * 100:.0f} %", "driver": r["typical_driver"],
          "n": int(r["onsets"])} for _, r in top.iterrows()],
        [Col("Rank", "rank", num=True), Col("Signal", "signal", path=True), Col("Median change", "score", num=True),
         Col("Onsets above 3 σ", "share", num=True, hide_small=True), Col("Change type", "driver", hide_small=True),
         Col("Onsets", "n", num=True, hide_small=True)],
    ))  # fmt: skip
    ui.note(
        "Change = the strongest of level (vs extrapolated trend), excursion beyond the vehicle's own normal range, increased jitter, or missing readings, in roughly-sigma units. Below 3 σ is noise."
    )

    st.header("One onset, up close")
    of_code = onsets[onsets["dtc_code"] == code].sort_values("onset_ts", ascending=False)
    of_code = of_code.assign(vehicle=of_code["vin"].map(model.codes()))
    labels = {
        r["onset_id"]: f"{r['vehicle']} · {pd.Timestamp(r['onset_ts']):%d %b %H:%M}"
        for _, r in of_code.iterrows()
    }
    pick = st.selectbox("Onset", of_code["onset_id"], format_func=labels.__getitem__)
    row = feats[feats["onset_id"] == pick].sort_values("score", ascending=False).iloc[0]
    trace = model.onset_trace(row["vin"], row["vss_path"], pd.Timestamp(row["onset_ts"]))
    unit = u.get(row["vss_path"], "")
    if trace.empty:
        ui.empty("No readings of this signal around the onset (it dropped out).")
        return
    onset = pd.Timestamp(row["onset_ts"])
    marks = pd.DataFrame(
        [{"t": onset - pd.Timedelta(minutes=10), "t2": onset, "label": "precursor window"}]
    )
    window = alt.Chart(marks).mark_rect(color=ui.WATCH, opacity=0.08).encode(x="t:T", x2="t2:T")
    onset_rule = alt.Chart(marks).mark_rule(color=ui.REV, strokeWidth=1.5).encode(x="t2:T")
    onset_label = (
        alt.Chart(marks)
        .mark_text(color=ui.REV, align="left", dx=4, y=10, fontWeight=700)
        .encode(x="t2:T", text=alt.value(f"{code} raised"))
    )
    line = (
        alt.Chart(trace)
        .mark_line(color=ui.INK)
        .encode(
            x=alt.X("event_ts:T", title=None),
            y=alt.Y("value:Q", title=model.fmt_unit(unit)),
        )
    )
    band = (
        ui.tolerance(row["ref_lo"], row["ref_hi"], "normal p0.5", "normal p99.5")
        if pd.notna(row["ref_lo"])
        else alt.layer()
    )
    ui.chart(band + window + line + onset_rule + onset_label, height=300)
    ui.kv([
        ("Vehicle", f"{model.codes().get(row['vin'], '—')} · {data.short(row['vin'])}"),
        ("Top signal", model.leaf(row["vss_path"])),
        ("Change", f"{row['score']:.1f} σ ({row['driver']})"),
    ])  # fmt: skip


# ---------------------------------------------------------------------- eco-driving
def eco() -> None:
    as_of = titled_as_of("Eco-driving", "as_of_eco")
    daily = model.table("gold.eco_driving_daily")
    if daily.empty:
        ui.empty("No trips yet.")
        return
    daily = daily.assign(
        day=pd.to_datetime(daily["day"]).dt.date, vehicle=daily["vin"].map(model.codes())
    )
    now = (
        daily[daily["day"] == as_of]
        .dropna(subset=["eco_score_7d"])
        .sort_values("eco_score_7d", ascending=False)
    )
    prev = daily[daily["day"] == as_of - pd.Timedelta(days=7)].set_index("vin")["eco_score_7d"]
    best = now["eco_score_7d"].max() if not now.empty else None

    st.header("Classification, trailing 7 days")
    if now.empty:
        ui.empty(f"No vehicle drove on {as_of:%d %b}.")
    else:
        rows = []
        for pos, (_, r) in enumerate(now.iterrows(), start=1):
            delta = r["eco_score_7d"] - prev.get(r["vin"], float("nan"))
            state = (
                "info"
                if r["eco_score_7d"] == best
                else ("watch" if r["eco_score_7d"] < model.ECO_WATCH else "ok")
            )
            rows.append({
                "pos": pos, "vehicle": ui.vehicle(r["vehicle"], data.short(r["vin"])),
                "platform": r["platform_id"], "score": f"{r['eco_score_7d']:.1f}",
                "harsh": "—" if pd.isna(r["harsh_accel_per_100km_7d"]) else f"{r['harsh_accel_per_100km_7d']:.2f}",
                "brake": "—" if pd.isna(r["harsh_brake_per_100km_7d"]) else f"{r['harsh_brake_per_100km_7d']:.2f}",
                "kwh": "—" if pd.isna(r["kwh_per_100km_7d"]) else f"{r['kwh_per_100km_7d']:.1f}",
                "km": f"{r['distance_km_7d']:,.0f}",
                "delta": "—" if pd.isna(delta) else f"{delta:+.1f}",
                "state": ui.tag(state, "FLEET BEST" if state == "info" else None),
            })  # fmt: skip
        st.html(ui.sheet_table(rows, [
            Col("Pos", "pos", num=True), Col("Vehicle", "vehicle", raw=True), Col("Platform", "platform", hide_small=True),
            Col("Eco score", "score", num=True), Col("Harsh accel /100 km", "harsh", num=True),
            Col("Harsh brake /100 km", "brake", num=True, hide_small=True), Col("kWh /100 km", "kwh", num=True, hide_small=True),
            Col("km", "km", num=True, hide_small=True), Col("Δ 7 d", "delta", num=True), Col("", "state", raw=True),
        ]))  # fmt: skip
        ui.note(
            "Harsh driving needs a longitudinal-acceleration signal: 6 of 11 platform contracts map one. Energy needs battery current (RENAULT_CMF_EV only)."
        )

    st.header("Harsh accelerations per 100 km over time")
    hist = daily.dropna(subset=["harsh_accel_per_100km_7d"])
    if not hist.empty:
        q1, q3 = hist["harsh_accel_per_100km_7d"].quantile([0.25, 0.75])
        line = (
            alt.Chart(hist)
            .mark_line()
            .encode(
                x=alt.X("day:T", title=None),
                y=alt.Y("harsh_accel_per_100km_7d:Q", title="per 100 km, 7-day"),
                color=alt.Color("vehicle:N", title=None),
                tooltip=["vehicle", "day:T", "harsh_accel_per_100km_7d"],
            )
        )
        ui.chart(ui.tolerance(q1, q3, "fleet p25", "fleet p75") + line, height=280)


# ---------------------------------------------------------------------- data quality
def _gate_value(check: str, value: float) -> str:
    """Rates as percentages, counts with thousands separators."""
    if check.endswith(("_rate", "_share")):
        return f"{value * 100:.2g} %" if value else "0 %"
    return f"{value:,.0f}" if float(value).is_integer() else f"{value:.3g}"


def quality() -> None:
    st.title("Data quality and pipeline")
    gates = model.latest_gates()
    st.header("Quality gates, latest result per check")
    if gates.empty:
        ui.empty("No quality checks recorded yet: run the Airflow pipeline.")
    else:
        st.html(ui.sheet_table(
            [{"layer": r["layer"].capitalize(), "check": r["check_name"], "desc": r["description"],
              "obs": _gate_value(r["check_name"], r["observed"]),
              "rule": f"{r['op']} {_gate_value(r['check_name'], r['threshold'])}",
              "verdict": ui.tag("info", "PASS") if r["passed"] else ui.tag("action" if r["severity"] == "error" else "watch", "FAIL" if r["severity"] == "error" else "WARN"),
              "when": f"{pd.Timestamp(r['checked_at']):%d %b %H:%M}"} for _, r in gates.iterrows()],
            [Col("Layer", "layer", hide_small=True), Col("Check", "check", hide_small=True), Col("What it guards", "desc"),
             Col("Observed", "obs", num=True), Col("Rule", "rule", num=True, hide_small=True),
             Col("Verdict", "verdict", raw=True), Col("Checked", "when", num=True, hide_small=True)],
        ))  # fmt: skip

    q = model.table("silver.telemetry_quarantine", ("error_code", "platform_id"))
    c1, c2 = st.columns([3, 2], gap="large")
    with c1:
        st.header("Quarantine (Dead Letter Queue)")
        if q.empty:
            ui.empty("Nothing quarantined.")
        else:
            agg = (
                q.groupby(["error_code", "platform_id"], dropna=False)
                .size()
                .reset_index(name="records")
            )
            bars = (
                alt.Chart(agg)
                .mark_bar()
                .encode(
                    y=alt.Y("error_code:N", title=None, sort="-x", axis=alt.Axis(labelLimit=220)),
                    x=alt.X("sum(records):Q", title="records"),
                    color=alt.Color(
                        "platform_id:N", title=None, legend=alt.Legend(columns=3, orient="bottom")
                    ),
                    tooltip=["error_code", "platform_id", "records"],
                )
            )
            ui.chart(bars, height=220)
    with c2:
        st.header("Stuck sensors (warning)")
        stuck = model.table("ops.dq_stuck_runs")
        if stuck.empty:
            ui.empty("No abnormally long runs of one value while moving.")
        else:
            top = stuck.sort_values("run_length", ascending=False).head(8)
            st.html(ui.sheet_table(
                [{"vehicle": model.codes().get(r["vin"], data.short(r["vin"])), "signal": model.leaf(r["vss_path"]),
                  "run": f"{int(r['run_length'])} × {r['value']:g}", "typical": f"p99 {int(r['typical_run'])}"}
                 for _, r in top.iterrows()],
                [Col("Vehicle", "vehicle"), Col("Signal", "signal", path=True), Col("Frozen run", "run", num=True),
                 Col("Normal", "typical", num=True)],
            ))  # fmt: skip
            ui.note(
                "Measured against ground truth: precision 0.77, recall 0.32. A warning, not a gate."
            )

    st.header("One standard, every platform")
    dim = model.table("gold.dim_platform")
    if dim.empty:
        ui.empty("No platform contracts in Gold yet. Run the Gold build.")
        return
    needs = {
        "Speed": "Vehicle.Speed",
        "Acceleration": "Vehicle.Acceleration.Longitudinal",
        "Pack voltage": "Vehicle.Powertrain.TractionBattery.CurrentVoltage",
        "Pack current": "Vehicle.Powertrain.TractionBattery.CurrentCurrent",
        "Cell temperature": "Vehicle.Powertrain.TractionBattery.Temperature.Max",
        "Coolant": "Vehicle.Powertrain.CombustionEngine.EngineCoolant.Temperature",
    }
    rows = []
    for _, r in dim.sort_values("platform_id").iterrows():
        paths = set(r["vss_paths"])
        row = {
            "platform": r["platform_id"],
            "pt": r["powertrain"],
            "enc": r["encoding"],
            "n": len(paths),
        }
        for label, path in needs.items():
            row[label] = (
                '<span class="tag info">MAPPED</span>'
                if path in paths
                else '<span class="pseudo">—</span>'
            )
        rows.append(row)
    st.html(ui.sheet_table(rows, [
        Col("Platform contract", "platform"), Col("Powertrain", "pt", hide_small=True),
        Col("Encoding", "enc", hide_small=True), Col("Signals", "n", num=True),
        *[Col(label, label, raw=True) for label in needs],
    ]))  # fmt: skip
    ui.note(
        "Every KPI is written once against VSS paths; a platform contributes to a KPI exactly where its contract maps the input signal."
    )

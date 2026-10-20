"""COVESA Telemetry Lakehouse dashboard (Streamlit).

streamlit run src/covesa_lakehouse/dashboard/app.py     # or the docker compose "dashboard" service
"""

from __future__ import annotations

import streamlit as st

from covesa_lakehouse.dashboard import model, ui, views

st.set_page_config(
    page_title="COVESA Telemetry Lakehouse", layout="wide", initial_sidebar_state="collapsed"
)
ui.inject_css()

pages = [
    st.Page(views.overview, title="Overview", url_path="overview", default=True),
    st.Page(views.battery, title="Battery", url_path="battery"),
    st.Page(views.faults, title="Faults", url_path="faults"),
    st.Page(views.eco, title="Eco-driving", url_path="eco-driving"),
    st.Page(views.quality, title="Data quality", url_path="data-quality"),
]
current = st.navigation(pages, position="hidden")

with st.container(key="topbar", horizontal=True, vertical_alignment="center", gap="medium"):
    st.html('<div class="brand">COVESA <span>/</span> Telemetry Lakehouse</div>', width="content")
    for page in pages:
        st.page_link(page, width="content")
    gates = model.latest_gates()
    failed = (
        int((~gates["passed"] & (gates["severity"] == "error")).sum()) if not gates.empty else 0
    )
    warned = int((~gates["passed"] & (gates["severity"] == "warn")).sum()) if not gates.empty else 0
    days = model.days()
    period = f"{days[0]:%d %b} – {days[-1]:%d %b %Y}" if days else "no data"
    gate_chip = (
        f'<span class="chip fail">{failed} gate failed</span>'
        if failed
        else '<span class="chip pass">Gates pass'
        + (f" · {warned} warning" if warned else "")
        + "</span>"
    )
    vss = ", ".join(v.lstrip("v") for v in model.contracts_summary()["vss"])
    st.html(
        f'<div class="status"><span class="chip">{period}</span><span class="chip">VSS {vss}</span>'
        f'{gate_chip}<span class="chip syn">Synthetic data</span></div>'
    )

current.run()

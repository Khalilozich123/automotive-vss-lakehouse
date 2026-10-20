"""Headless screenshots of every dashboard view (desktop 1440 px and mobile 390 px).

Runs in the Playwright container on the compose network, so nothing is installed on the host:

    docker run --rm --network covesa-lakehouse_default -v "$PWD:/work" -w /work \
      mcr.microsoft.com/playwright/python:v1.64.0-noble \
      bash -c "pip install -q playwright==1.64.0 && python scripts/capture_dashboard.py"
"""

from __future__ import annotations

import os
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = os.environ.get("DASHBOARD_URL", "http://dashboard:8501")
OUT = Path("build/dashboard-screenshots")
VIEWS = {
    "": "Fleet status",  # the default page is served at the root
    "battery": "Battery health",
    "faults": "Fault precursors",
    "eco-driving": "Eco-driving",
    "data-quality": "Data quality and pipeline",
}
SIZES = {"desktop": (1440, 900), "mobile": (390, 844)}


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as p:
        browser = p.chromium.launch()
        for size, (w, h) in SIZES.items():
            page = browser.new_page(viewport={"width": w, "height": h}, device_scale_factor=1)
            for route, title in VIEWS.items():
                page.goto(f"{BASE}/{route}", wait_until="networkidle")
                page.get_by_role("heading", name=title).wait_for(timeout=60_000)
                page.wait_for_function(
                    "document.querySelectorAll("
                    "'[data-testid=\"stSkeleton\"], .stSpinner').length === 0",
                    timeout=60_000,
                )
                page.wait_for_timeout(2500)  # vega-embed draws after the data arrives
                path = OUT / f"{route or 'overview'}-{size}.png"
                page.screenshot(path=str(path), full_page=True)
                print(path)
            page.close()
        browser.close()
    for size in SIZES:  # the overview doubles as the canonical review capture
        (OUT / f"{size}.png").write_bytes((OUT / f"overview-{size}.png").read_bytes())


if __name__ == "__main__":
    main()

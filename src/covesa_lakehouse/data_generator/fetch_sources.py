"""Download the pinned upstream schema sources into ``sources/``.

* CAN databases (.dbc) from comma.ai opendbc (MIT), pinned to one commit.
* The COVESA Vehicle Signal Specification tree (MPL-2.0), pinned to one release.

The files are committed to the repo so builds never depend on GitHub being reachable;
re-run this only to bump a pin:  uv run python -m covesa_lakehouse.data_generator.fetch_sources
"""

from __future__ import annotations

import argparse
import urllib.request
from pathlib import Path

OPENDBC_COMMIT = "a7da786c8ce7b77ddbb674aef849bc86de016eff"
VSS_RELEASE = "v6.1"

DBC_FILES = (
    "tesla_powertrain.dbc",
    "gm_global_a_high_voltage_management.dbc",
    "byd_atto3.dbc",
    "gwm_haval_h6_phev_2024.dbc",
    "toyota_prius_2010_pt.dbc",
    "psa_aee2010_r3.dbc",
    "fca_giorgio.dbc",
    "vw_mqb.dbc",
    "hyundai_2015_ccan.dbc",
    "bmw_e9x_e8x.dbc",
)
VSS_FILES = ("vss.json", "units.yaml")

OPENDBC_RAW = f"https://raw.githubusercontent.com/commaai/opendbc/{OPENDBC_COMMIT}/opendbc/dbc"
VSS_RAW = "https://github.com/COVESA/vehicle_signal_specification/releases/download/" + VSS_RELEASE


def _download(url: str, dest: Path) -> None:
    with urllib.request.urlopen(url, timeout=60) as resp:
        dest.write_bytes(resp.read())
    print(f"  {dest}  ({dest.stat().st_size:,} bytes)")


def fetch(root: Path) -> None:
    dbc_dir, vss_dir = root / "dbc", root / "vss"
    dbc_dir.mkdir(parents=True, exist_ok=True)
    vss_dir.mkdir(parents=True, exist_ok=True)

    print(f"opendbc @ {OPENDBC_COMMIT[:7]}")
    for name in DBC_FILES:
        _download(f"{OPENDBC_RAW}/{name}", dbc_dir / name)

    print(f"COVESA VSS {VSS_RELEASE}")
    for name in VSS_FILES:
        _download(f"{VSS_RAW}/{name}", vss_dir / name)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", type=Path, default=Path("sources"))
    fetch(parser.parse_args().dest)


if __name__ == "__main__":
    main()

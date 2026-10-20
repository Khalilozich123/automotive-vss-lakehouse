"""Read-only access to the Gold and ops tables for the dashboard, through PyIceberg.

BI-style reads of an Iceberg lakehouse need no Spark cluster: PyIceberg asks the REST catalog
for the table's current snapshot and reads the Parquet files straight from object storage, with
partition pruning from the row filter.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import pandas as pd
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchTableError

from covesa_lakehouse.common.config_loader import load_contract_dir

PLATFORM_CODES = {
    "RENAULT_CMF_EV": "REN",
    "GM_BOLT_EV": "GMB",
    "TESLA_MODEL_S": "TSL",
    "BYD_ATTO3": "BYD",
    "GWM_HAVAL_H6_PHEV": "GWM",
    "TOYOTA_PRIUS_GEN3": "TOY",
    "VW_MQB": "VWM",
    "STELLANTIS_PSA_AEE2010": "PSA",
    "STELLANTIS_FCA_GIORGIO": "FCA",
    "HYUNDAI_KIA_CCAN_2015": "HYK",
    "BMW_E9X": "BMW",
}


@lru_cache(maxsize=1)
def catalog() -> Catalog:
    env = os.environ
    return load_catalog(
        "lakehouse",
        type="rest",
        uri=env.get("LAKEHOUSE_CATALOG_URI", "http://iceberg-rest:8181"),
        **{
            "s3.endpoint": env.get("LAKEHOUSE_S3_ENDPOINT", "http://rustfs:9000"),
            "s3.access-key-id": env.get("AWS_ACCESS_KEY_ID", "lakehouse"),
            "s3.secret-access-key": env.get("AWS_SECRET_ACCESS_KEY", "lakehouse-secret"),
            "s3.region": env.get("AWS_REGION", "us-east-1"),
            "s3.path-style-access": "true",
        },
    )


def read(table: str, columns: tuple[str, ...] = ("*",), row_filter: str = "true") -> pd.DataFrame:
    """Scan ``namespace.table`` into pandas; an absent table reads as an empty frame."""
    try:
        t = catalog().load_table(table)
    except NoSuchTableError:
        return pd.DataFrame(columns=[] if columns == ("*",) else list(columns))
    return t.scan(row_filter=row_filter, selected_fields=columns).to_pandas()


def units(contracts_dir: Path | None = None) -> dict[str, str]:
    """VSS path -> unit, from the contracts (the dashboard ships them, not the VSS tree)."""
    path = contracts_dir or Path(os.environ.get("CONTRACTS_DIR", "contracts"))
    out: dict[str, str] = {}
    for contract in load_contract_dir(path).contracts.values():
        for spec in contract.signals.values():
            out[spec.vss_path] = spec.unit
    out["Derived.TractionBattery.CellVoltageSpread"] = "mV"
    return out


def vehicle_codes(vins: pd.DataFrame) -> dict[str, str]:
    """Stable human-readable codes (``REN-03``) for pseudonymized VINs, per platform.

    Codes are an index within the platform, ordered by pseudonym, so they are stable across
    refreshes and reveal nothing about the real VIN.
    """
    codes: dict[str, str] = {}
    pairs = vins[["vin", "platform_id"]].drop_duplicates().sort_values(["platform_id", "vin"])
    for platform, group in pairs.groupby("platform_id"):
        prefix = PLATFORM_CODES.get(str(platform), str(platform)[:3])
        for i, vin in enumerate(group["vin"], start=1):
            codes[vin] = f"{prefix}-{i:02d}"
    return codes


def short(vin: str) -> str:
    """First 4 and last 4 characters of the pseudonym (never the real VIN)."""
    return f"{vin[:4]}…{vin[-4:]}" if isinstance(vin, str) and len(vin) > 8 else str(vin)

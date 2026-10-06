"""Synthetic but structurally valid VINs (ISO 3779 layout, North American check digit)."""

from __future__ import annotations

import random

# World Manufacturer Identifier per platform family (first three VIN characters).
WMI = {
    "VW_MQB": "WVW",
    "STELLANTIS_PSA_AEE2010": "VF3",
    "STELLANTIS_FCA_GIORGIO": "ZAR",
    "BMW_E9X": "WBA",
    "HYUNDAI_KIA_CCAN_2015": "KMH",
    "TOYOTA_PRIUS_GEN3": "JTD",
    "TESLA_MODEL_S": "5YJ",
    "GM_BOLT_EV": "1G1",
    "BYD_ATTO3": "LGX",
    "GWM_HAVAL_H6_PHEV": "LGW",
    "RENAULT_CMF_EV": "VF1",
}

_ALPHABET = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"  # I, O, Q are not allowed
_VALUES = {
    **{str(d): d for d in range(10)},
    **dict(zip("ABCDEFGH", range(1, 9), strict=True)),
    **dict(zip("JKLMN", range(1, 6), strict=True)),
    "P": 7,
    "R": 9,
    **dict(zip("STUVWXYZ", range(2, 10), strict=True)),
}
_WEIGHTS = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
_MODEL_YEARS = "DEFGHJKLMNPRST"  # 2013 .. 2026


def check_digit(vin: str) -> str:
    total = sum(_VALUES[c] * w for c, w in zip(vin, _WEIGHTS, strict=True))
    remainder = total % 11
    return "X" if remainder == 10 else str(remainder)


def make_vin(platform_id: str, rng: random.Random) -> str:
    wmi = WMI.get(platform_id, "SYN")
    vds = "".join(rng.choice(_ALPHABET) for _ in range(5))
    year = rng.choice(_MODEL_YEARS)
    plant = rng.choice(_ALPHABET)
    serial = f"{rng.randrange(1_000_000):06d}"
    draft = f"{wmi}{vds}0{year}{plant}{serial}"
    return draft[:8] + check_digit(draft) + draft[9:]

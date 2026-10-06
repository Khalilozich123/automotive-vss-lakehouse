"""Normalize the free-text units found in DBC files and convert them to VSS units.

DBC unit strings are inconsistent ("kph", "Khp", "Unit_KiloMeterPerHour", "Degré/s", "1/min").
Only multiplicative conversions are supported, so a conversion folds into the linear
decode: ``k * (raw * scale + offset) == raw * (k * scale) + (k * offset)``.
"""

from __future__ import annotations

_ALIASES: dict[str, tuple[str, ...]] = {
    "km/h": ("km/h", "kph", "kmh", "khp", "unit_kilometerperhour"),
    "mph": ("mph",),
    "m/s": ("m/s",),
    "rpm": ("rpm", "1/min", "rev/min", "unit_minutinver"),
    "Celsius": ("celsius", "c", "°c", "degc", "deg c", "unit_degrecelsi"),
    "percent": ("percent", "%", "unit_percent"),
    "ratio": ("ratio",),
    "m/s^2": ("m/s^2", "m/s2", "unit_meterperseconsquar"),
    "g": ("g", "unit_forceofgravi"),
    "degrees": ("degrees", "degree", "deg", "unit_degreofarc"),
    "degrees/s": ("degrees/s", "degree/s", "deg/s", "degré/s"),
    "rad/s": ("rad/s",),
    "V": ("v", "unit_volt"),
    "A": ("a", "unit_amper"),
    "Nm": ("nm", "n.m", "unit_newtometer"),
    "km": ("km", "unit_kilometer"),
    "m": ("m",),
}
_LOOKUP = {alias: unit for unit, aliases in _ALIASES.items() for alias in aliases}

# (from, to) -> factor
_FACTORS: dict[tuple[str, str], float] = {
    ("mph", "km/h"): 1.609344,
    ("m/s", "km/h"): 3.6,
    ("g", "m/s^2"): 9.80665,
    ("rad/s", "degrees/s"): 57.29577951308232,
    ("km", "m"): 1000.0,
    ("ratio", "percent"): 100.0,
}


def normalize_unit(raw: str | None) -> str | None:
    """Canonical unit for a DBC unit string, ``""`` for unitless, ``None`` if unrecognised."""
    text = (raw or "").strip().lower()
    if not text:
        return ""
    return _LOOKUP.get(text)


def conversion_factor(source_unit: str, vss_unit: str) -> float:
    if source_unit == vss_unit:
        return 1.0
    try:
        return _FACTORS[(source_unit, vss_unit)]
    except KeyError:
        raise ValueError(f"no conversion from {source_unit!r} to VSS unit {vss_unit!r}") from None

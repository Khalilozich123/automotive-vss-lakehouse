"""CAN decoding as native Spark expressions (no Python UDFs), parameterized by contract columns.

A signal's bit layout is pre-computed per contract row (see ``config_loader.bit_window``):
the bytes it spans (``first_byte``, ``n_bytes``), their order (``little_endian``), and the
right ``shift`` that brings its LSB to bit 0. Decoding then works the same way for every
platform and both byte orders:

1. assemble the spanned bytes of the hex payload into one 64-bit integer (at most 7 bytes);
2. shift right and mask ``length`` bits -> raw unsigned value;
3. two's complement for signed signals;
4. ``physical = raw * scale + offset``  (the automotive CAN scaling standard).
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

_HEX_BYTES = r"^([0-9A-Fa-f]{2})+$"


def _word_sql(data: str, p: str) -> str:
    return (
        f"aggregate(sequence(0, {p}n_bytes - 1), 0L, (acc, i) -> acc + shiftleft("
        f"cast(conv(substring({data}, ({p}first_byte + i) * 2 + 1, 2), 16, 10) AS BIGINT), "
        f"8 * if({p}little_endian, i, {p}n_bytes - 1 - i)))"
    )


def extract_unsigned(data: str, prefix: str = "") -> Column:
    """Raw unsigned field from hex column ``data``, using the ``{prefix}first_byte``... columns."""
    p = prefix
    return F.expr(f"shiftright({_word_sql(data, p)}, {p}shift) & (shiftleft(1L, {p}length) - 1)")


def to_signed(raw: Column, length: Column, is_signed: Column) -> Column:
    """Two's complement: a set sign bit means ``raw - 2^length``."""
    half = F.pow(F.lit(2), length - 1).cast("bigint")  # exact: powers of two up to 2^62
    return F.when(is_signed & (raw >= half), raw - half * 2).otherwise(raw)


def frame_is_well_formed(data: Column, dlc: Column) -> Column:
    """Even-length hex of exactly ``dlc`` bytes; anything else cannot be decoded safely."""
    return F.coalesce(data.rlike(_HEX_BYTES) & (F.length(data) == dlc * 2), F.lit(False))


def physical(raw: Column, scale: Column, offset: Column) -> Column:
    return raw * scale + offset

"""Keyed VIN pseudonymization (HMAC-SHA256) as a native Spark expression.

Why keyed: a VIN has a small, structured search space (manufacturer prefix, model year, 6-digit
serial), so a plain SHA-256 of it can be reversed by brute force. HMAC with a secret key cannot,
without the key. The same key always yields the same pseudonym, so per-vehicle analytics still
join across tables. ``vin_key_id`` is stored next to each pseudonym so that a key rotation is
traceable.

The Spark expression follows RFC 2104 exactly, without a Python UDF:
``HMAC(K, m) = H((K0 xor opad) || H((K0 xor ipad) || m))``.
"""

from __future__ import annotations

import hashlib
import hmac
import os

from pyspark.sql import Column
from pyspark.sql import functions as F

_BLOCK = 64  # SHA-256 block size in bytes


def _padded_keys(key: bytes) -> tuple[bytearray, bytearray]:
    if len(key) > _BLOCK:
        key = hashlib.sha256(key).digest()
    key = key.ljust(_BLOCK, b"\0")
    return bytearray(b ^ 0x36 for b in key), bytearray(b ^ 0x5C for b in key)


def hmac_sha256_hex(key: bytes, value: str) -> str:
    """Reference implementation (the Spark expression must match it)."""
    return hmac.new(key, value.encode("utf-8"), hashlib.sha256).hexdigest()


def hmac_sha256_col(key: bytes, value: Column) -> Column:
    ipad, opad = _padded_keys(key)
    inner = F.unhex(F.sha2(F.concat(F.lit(ipad), F.encode(value, "UTF-8")), 256))
    return F.sha2(F.concat(F.lit(opad), inner), 256)


def key_from_env() -> tuple[bytes, str]:
    """(key, key_id) from VIN_PSEUDONYMIZATION_KEY / _KEY_ID; production keys come from a vault."""
    key = os.environ.get("VIN_PSEUDONYMIZATION_KEY")
    if not key:
        raise RuntimeError("VIN_PSEUDONYMIZATION_KEY is not set")
    return key.encode("utf-8"), os.environ.get("VIN_PSEUDONYMIZATION_KEY_ID", "unversioned")

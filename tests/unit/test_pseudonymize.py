import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_col, hmac_sha256_hex, key_from_env

VINS = ["WVWZZZAUZHW000001", "VF1RFB00X65432101", "5YJSA1E26MF123456", "", "é-unicode"]


@pytest.mark.parametrize(
    "key",
    [b"k", b"local-dev-only-key", b"x" * 64, b"y" * 200],  # short, typical, block-sized, long
    ids=["short", "typical", "block", "long"],
)
def test_spark_hmac_matches_rfc2104_reference(spark: SparkSession, key: bytes) -> None:
    df = spark.createDataFrame([(v,) for v in VINS], "vin STRING")
    got = {
        r.vin: r.h
        for r in df.select("vin", hmac_sha256_col(key, F.col("vin")).alias("h")).collect()
    }
    assert got == {v: hmac_sha256_hex(key, v) for v in VINS}


def test_null_vin_stays_null(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], "vin STRING")
    (row,) = df.select(hmac_sha256_col(b"k", F.col("vin")).alias("h")).collect()
    assert row["h"] is None


def test_different_keys_give_unlinkable_pseudonyms() -> None:
    assert hmac_sha256_hex(b"key-1", VINS[0]) != hmac_sha256_hex(b"key-2", VINS[0])


def test_key_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VIN_PSEUDONYMIZATION_KEY", raising=False)
    with pytest.raises(RuntimeError, match="VIN_PSEUDONYMIZATION_KEY"):
        key_from_env()
    monkeypatch.setenv("VIN_PSEUDONYMIZATION_KEY", "s3cret")
    monkeypatch.setenv("VIN_PSEUDONYMIZATION_KEY_ID", "2026-10")
    assert key_from_env() == (b"s3cret", "2026-10")

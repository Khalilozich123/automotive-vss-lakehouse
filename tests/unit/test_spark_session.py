from pathlib import Path

import pytest

from covesa_lakehouse.common.spark_session import CATALOG_NAME, LakehouseConfig, spark_conf

CAT = f"spark.sql.catalog.{CATALOG_NAME}"


def test_conf_wires_iceberg_rest_catalog_with_s3fileio() -> None:
    conf = spark_conf(LakehouseConfig(catalog_uri="http://cat:8181", s3_endpoint="http://s3:9000"))

    assert conf["spark.sql.defaultCatalog"] == CATALOG_NAME
    assert conf[f"{CAT}.type"] == "rest"
    assert conf[f"{CAT}.uri"] == "http://cat:8181"
    assert conf[f"{CAT}.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf[f"{CAT}.s3.endpoint"] == "http://s3:9000"
    assert conf[f"{CAT}.s3.path-style-access"] == "true"
    assert "IcebergSparkSessionExtensions" in conf["spark.sql.extensions"]


def test_conf_omits_optional_settings_by_default() -> None:
    conf = spark_conf(LakehouseConfig())

    assert "spark.jars" not in conf
    assert "spark.driver.host" not in conf


def test_conf_sets_driver_host_for_client_mode() -> None:
    conf = spark_conf(LakehouseConfig(driver_host="workspace"))

    assert conf["spark.driver.host"] == "workspace"
    assert conf["spark.driver.bindAddress"] == "0.0.0.0"


def test_from_env_reads_overrides_and_jar_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    (tmp_path / "b.jar").touch()
    (tmp_path / "a.jar").touch()
    (tmp_path / "notes.txt").touch()
    monkeypatch.setenv("ICEBERG_JARS_DIR", str(tmp_path))
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://m:7077")
    monkeypatch.setenv("LAKEHOUSE_CATALOG_URI", "http://other:8181")
    monkeypatch.setenv("SPARK_DRIVER_HOST", "workspace")

    cfg = LakehouseConfig.from_env(app_name="job")

    assert cfg.app_name == "job"
    assert cfg.spark_master == "spark://m:7077"
    assert cfg.catalog_uri == "http://other:8181"
    assert cfg.driver_host == "workspace"
    assert cfg.iceberg_jars == (str(tmp_path / "a.jar"), str(tmp_path / "b.jar"))


def test_from_env_falls_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "ICEBERG_JARS_DIR",
        "SPARK_MASTER_URL",
        "LAKEHOUSE_CATALOG_URI",
        "SPARK_DRIVER_HOST",
    ):
        monkeypatch.delenv(var, raising=False)

    cfg = LakehouseConfig.from_env()

    assert cfg.spark_master == "local[*]"
    assert cfg.iceberg_jars == ()
    assert cfg.driver_host is None

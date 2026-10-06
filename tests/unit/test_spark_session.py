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


def test_conf_wires_s3a_for_plain_files_on_the_same_store() -> None:
    conf = spark_conf(LakehouseConfig(s3_endpoint="http://s3:9000", s3_access_key="k"))

    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://s3:9000"
    assert conf["spark.hadoop.fs.s3a.path.style.access"] == "true"
    assert conf["spark.hadoop.fs.s3a.connection.ssl.enabled"] == "false"
    assert conf["spark.hadoop.fs.s3a.access.key"] == "k"


def test_conf_omits_optional_settings_by_default() -> None:
    conf = spark_conf(LakehouseConfig())

    assert "spark.driver.extraClassPath" not in conf
    assert "spark.driver.host" not in conf


def test_extra_jars_go_on_driver_and_executor_classpath() -> None:
    conf = spark_conf(LakehouseConfig(extra_jars_dir="/opt/spark-extra-jars/"))

    assert conf["spark.driver.extraClassPath"] == "/opt/spark-extra-jars/*"
    assert conf["spark.executor.extraClassPath"] == "/opt/spark-extra-jars/*"


def test_conf_sets_driver_host_for_client_mode() -> None:
    conf = spark_conf(LakehouseConfig(driver_host="workspace"))

    assert conf["spark.driver.host"] == "workspace"
    assert conf["spark.driver.bindAddress"] == "0.0.0.0"


def test_from_env_reads_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SPARK_EXTRA_JARS_DIR", "/jars")
    monkeypatch.setenv("SPARK_MASTER_URL", "spark://m:7077")
    monkeypatch.setenv("LAKEHOUSE_CATALOG_URI", "http://other:8181")
    monkeypatch.setenv("SPARK_DRIVER_HOST", "workspace")

    cfg = LakehouseConfig.from_env(app_name="job")

    assert cfg.app_name == "job"
    assert cfg.spark_master == "spark://m:7077"
    assert cfg.catalog_uri == "http://other:8181"
    assert cfg.driver_host == "workspace"
    assert cfg.extra_jars_dir == "/jars"


def test_from_env_falls_back_to_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in (
        "SPARK_EXTRA_JARS_DIR",
        "SPARK_MASTER_URL",
        "LAKEHOUSE_CATALOG_URI",
        "SPARK_DRIVER_HOST",
    ):
        monkeypatch.delenv(var, raising=False)

    cfg = LakehouseConfig.from_env()

    assert cfg.spark_master == "local[*]"
    assert cfg.extra_jars_dir is None
    assert cfg.driver_host is None

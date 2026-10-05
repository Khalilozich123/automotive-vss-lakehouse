"""Phase 0 smoke test: the Spark cluster can write and read an Iceberg table on object storage.

Run inside the workspace container with the compose stack up:  uv run pytest -m integration
"""

from collections.abc import Iterator
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark() -> Iterator[SparkSession]:
    session = get_spark(LakehouseConfig.from_env(app_name="phase0-smoke-test"))
    yield session
    session.stop()


def test_iceberg_round_trip_on_object_storage(spark: SparkSession) -> None:
    table = f"smoke.roundtrip_{uuid4().hex[:8]}"
    spark.sql("CREATE NAMESPACE IF NOT EXISTS smoke")
    spark.sql(
        f"CREATE TABLE {table} (vin STRING, vss_path STRING, val_double DOUBLE) USING iceberg"
    )
    try:
        rows = [
            ("VIN0001", "Vehicle.Speed", 87.5),
            ("VIN0001", "Vehicle.Powertrain.TractionBattery.Voltage", 402.1),
            ("VIN0002", "Vehicle.Powertrain.TractionBattery.Temperature.Max", 32.0),
        ]
        spark.createDataFrame(rows, "vin STRING, vss_path STRING, val_double DOUBLE").writeTo(
            table
        ).append()

        read_back = spark.table(table).orderBy("vss_path").collect()
        assert [tuple(r) for r in read_back] == sorted(rows, key=lambda r: r[1])

        # Data and metadata were written to the S3 warehouse and a snapshot was committed.
        files = spark.sql(f"SELECT file_path FROM {table}.files").collect()
        assert files and all(f.file_path.startswith("s3://warehouse/") for f in files)
        assert spark.sql(f"SELECT * FROM {table}.snapshots").count() == 1
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")

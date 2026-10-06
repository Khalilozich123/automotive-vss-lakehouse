"""Shared fixtures.

There can only be one SparkSession per process, so every test that needs Spark uses this one:
inside the workspace container (SPARK_MASTER_URL set) it is the full lakehouse session on the
cluster; elsewhere (CI unit job) a plain local session, which is enough for DataFrame logic.
"""

import os
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession

from covesa_lakehouse.common.spark_session import LakehouseConfig, get_spark


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    if os.environ.get("SPARK_MASTER_URL"):
        session = get_spark(LakehouseConfig.from_env(app_name="pytest"))
    else:
        session = (
            SparkSession.builder.master("local[2]")
            .appName("pytest-local")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()

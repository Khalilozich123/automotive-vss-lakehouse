"""Silver end to end on the real stack: Bronze stream -> conformance -> Iceberg MERGE."""

import json
import shutil
from collections.abc import Callable, Iterator
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql import SparkSession

from covesa_lakehouse.bronze import ingest_raw
from covesa_lakehouse.bronze.landing import S3Landing
from covesa_lakehouse.silver import conformance_engine as engine
from covesa_lakehouse.silver import data_quality as dq

pytestmark = pytest.mark.integration

SAMPLE = Path("tests/fixtures/telemetry_sample.jsonl")
TRUTH = Path("tests/fixtures/telemetry_sample.truth.jsonl")
NEW_PLATFORM = "RENAULT_CMF_EV"


@pytest.fixture
def env(
    spark: SparkSession, tmp_path: Path, delete_s3_prefix: Callable[[str, str], None]
) -> Iterator[dict[str, object]]:
    run = uuid4().hex[:10]
    names: dict[str, object] = {
        "prefix": f"it-{run}/telemetry",
        "landing": f"s3a://landing/it-{run}/telemetry/",
        "bronze": f"lakehouse.it_silver.bronze_{run}",
        "bronze_ckpt": f"s3a://checkpoints/it-{run}/bronze",
        "silver_ckpt": f"s3a://checkpoints/it-{run}/silver",
        "tables": engine.SilverTables(
            signals=f"lakehouse.it_silver.signals_{run}",
            quarantine=f"lakehouse.it_silver.quarantine_{run}",
            dtc_events=f"lakehouse.it_silver.dtc_{run}",
        ),
        "contracts": tmp_path / "contracts",
    }
    yield names
    tables = names["tables"]
    assert isinstance(tables, engine.SilverTables)
    for table in (names["bronze"], tables.signals, tables.quarantine, tables.dtc_events):
        spark.sql(f"DROP TABLE IF EXISTS {table} PURGE")
    delete_s3_prefix("landing", f"it-{run}/")
    delete_s3_prefix("checkpoints", f"it-{run}/")


def _land_and_ingest(
    spark: SparkSession, env: dict[str, object], name: str, lines: list[str]
) -> None:
    S3Landing().put(
        f"{env['prefix']}/batch/dt=2026-09-01/hour=07/{name}.jsonl",
        ("\n".join(lines) + "\n").encode(),
    )
    ingest_raw.run(spark, str(env["landing"]), str(env["bronze"]), str(env["bronze_ckpt"]))


def test_new_contract_is_picked_up_by_the_running_stream(
    spark: SparkSession, env: dict[str, object]
) -> None:
    """Phase 3 definition of done: a new platform needs only a YAML file, no code or restart."""
    contracts = env["contracts"]
    tables = env["tables"]
    assert isinstance(contracts, Path) and isinstance(tables, engine.SilverTables)
    contracts.mkdir()
    for path in Path("contracts").glob("*.yaml"):
        if path.stem != NEW_PLATFORM.lower():
            shutil.copy(path, contracts)

    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    half = len(lines) // 2
    _land_and_ingest(spark, env, "part-a", lines[:half])

    query, stats = engine.start(
        spark,
        str(env["bronze"]),
        tables,
        str(env["silver_ckpt"]),
        contracts,
        continuous=True,
        interval="1 second",
    )
    try:
        query.processAllAvailable()
        signals = spark.table(tables.signals)
        quarantine = spark.table(tables.quarantine)
        assert signals.filter(f"platform_id = '{NEW_PLATFORM}'").count() == 0
        unknown = quarantine.filter(f"error_code = '{dq.UNKNOWN_PLATFORM}'")
        assert unknown.count() > 0
        assert {r.platform_id for r in unknown.select("platform_id").distinct().collect()} == {
            NEW_PLATFORM
        }

        # Onboard the platform: drop one YAML file while the query keeps running.
        shutil.copy(Path("contracts") / f"{NEW_PLATFORM.lower()}.yaml", contracts)
        _land_and_ingest(spark, env, "part-b", lines[half:])
        query.processAllAvailable()

        assert query.isActive  # same query, never restarted
        assert spark.table(tables.signals).filter(f"platform_id = '{NEW_PLATFORM}'").count() > 0
    finally:
        query.stop()
    assert stats.batches >= 2


def test_full_sample_conforms_and_replays_idempotently(
    spark: SparkSession, env: dict[str, object]
) -> None:
    tables = env["tables"]
    assert isinstance(tables, engine.SilverTables)
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    _land_and_ingest(spark, env, "all", lines)

    first = engine.run(spark, str(env["bronze"]), tables, str(env["silver_ckpt"]))
    assert first.bronze_rows == len(lines)

    signals = spark.table(tables.signals)
    assert signals.groupBy("msg_id", "vss_path").count().filter("count > 1").count() == 0

    truth = [json.loads(t) for t in TRUTH.read_text().splitlines()]
    duplicated = {t["msg_id"] for t in truth if t.get("value") == "duplicate"}
    assert duplicated  # the sample contains duplicate deliveries, yet Silver has each once
    oor = sum(1 for t in truth if t.get("value") == "out_of_range")
    codes = {
        r.error_code: r["count"]
        for r in spark.table(tables.quarantine).groupBy("error_code").count().collect()
    }
    assert codes[dq.PHYSICAL_RANGE_VIOLATION] == oor

    # Reprocess everything from scratch (fresh checkpoint): MERGE must insert nothing.
    replay = engine.run(spark, str(env["bronze"]), tables, str(env["silver_ckpt"]) + "-replay")
    assert replay.bronze_rows == len(lines)
    assert (replay.signals, replay.quarantined, replay.dtc_events) == (0, 0, 0)
    assert spark.table(tables.signals).count() == signals.count()

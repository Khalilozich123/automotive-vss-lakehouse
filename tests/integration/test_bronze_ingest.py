"""Bronze end to end on the real stack (object storage, Iceberg catalog, Spark cluster, MQTT)."""

import json
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from pyspark.sql import DataFrame, SparkSession

from covesa_lakehouse.bronze import ingest_raw
from covesa_lakehouse.bronze.landing import S3Landing
from covesa_lakehouse.bronze.pseudonymize import hmac_sha256_hex, key_from_env
from covesa_lakehouse.common.contract import load_contracts
from covesa_lakehouse.data_generator.generate_telemetry import MqttSink, build_fleet

pytestmark = pytest.mark.integration

SAMPLE = Path("tests/fixtures/telemetry_sample.jsonl")


@pytest.fixture
def isolated(
    spark: SparkSession, delete_s3_prefix: Callable[[str, str], None]
) -> Iterator[dict[str, str]]:
    """A private landing prefix, checkpoint and table per test (table dropped afterwards)."""
    run = uuid4().hex[:10]
    names = {
        "prefix": f"it-{run}/telemetry",
        "landing": f"s3a://landing/it-{run}/telemetry/",
        "checkpoint": f"s3a://checkpoints/it-{run}",
        "table": f"lakehouse.it_bronze.telemetry_raw_{run}",
    }
    yield names
    spark.sql(f"DROP TABLE IF EXISTS {names['table']} PURGE")
    delete_s3_prefix("landing", f"it-{run}/")
    delete_s3_prefix("checkpoints", f"it-{run}")


def _drop(prefix: str, name: str, data: bytes) -> None:
    S3Landing().put(f"{prefix}/batch/dt=2026-09-01/hour=07/{name}.jsonl", data)


def test_file_drops_are_ingested_exactly_once(
    spark: SparkSession, isolated: dict[str, str]
) -> None:
    lines = SAMPLE.read_bytes().splitlines(keepends=True)
    half = len(lines) // 2
    args = (isolated["landing"], isolated["table"], isolated["checkpoint"])

    _drop(isolated["prefix"], "part-a", b"".join(lines[:half]))
    assert ingest_raw.run(spark, *args) == half
    assert ingest_raw.run(spark, *args) == 0  # replay: nothing new, nothing duplicated

    _drop(isolated["prefix"], "part-b", b"".join(lines[half:]))
    assert ingest_raw.run(spark, *args) == len(lines) - half  # only the new file

    table = spark.table(isolated["table"])
    assert table.count() == len(lines)
    assert table.filter("source_system = 'batch'").count() == len(lines)
    assert table.select("source_file").distinct().count() == 2
    snapshots = spark.sql(f"SELECT * FROM {isolated['table']}.snapshots").count()
    assert snapshots == 2  # one atomic append per run that had data


def test_mqtt_messages_reach_bronze_through_the_bridge(
    spark: SparkSession, isolated: dict[str, str]
) -> None:
    """Simulator -> Mosquitto -> mqtt-bridge service -> s3://landing -> Spark -> Iceberg."""
    seed = uuid4().int % 1_000_000  # fresh VINs, so this run's rows are identifiable in Bronze
    fleet = build_fleet(load_contracts(Path("contracts")), 11, seed, 0.05, fault_rate=0.0)
    sink = MqttSink("mosquitto", 1883, qos=1, client_id=f"it-{seed}")
    sent = 0
    start = datetime.now(UTC)
    for t in range(30):
        for v in fleet:
            snapshot, dtcs, _ = v.advance(1.0)
            for message in v.encoder.encode(snapshot, start + timedelta(seconds=t), dtcs):
                sink.send(message)
                sent += 1
    sink.close()

    key, _ = key_from_env()
    vins = [v.vin for v in fleet]
    pseudonyms = [hmac_sha256_hex(key, vin) for vin in vins]

    def ours() -> DataFrame:
        table = spark.table(isolated["table"])
        return table.filter(table.vin.isin(pseudonyms))

    # The bridge flushes partial batches every 5 s; ingest until everything has arrived. Only the
    # bridge's partition for today is read: the shared landing zone may hold large backfills.
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    landing = f"s3a://landing/telemetry/mqtt/dt={today}/"
    args = (landing, isolated["table"], isolated["checkpoint"])
    deadline = time.monotonic() + 60
    while True:
        ingest_raw.run(spark, *args)
        if ours().count() >= sent or time.monotonic() > deadline:
            break
        time.sleep(3)

    rows = ours()
    assert rows.count() == sent
    assert rows.filter("source_system = 'mqtt' AND received_at IS NOT NULL").count() == sent
    payloads = [r.raw_payload for r in rows.select("raw_payload").collect()]
    assert not any(vin in p for vin in vins for p in payloads)
    parsed = [json.loads(p) for p in payloads if p.endswith("}")]
    assert parsed and {p["vin"] for p in parsed} <= set(pseudonyms)

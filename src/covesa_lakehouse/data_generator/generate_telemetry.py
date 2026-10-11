"""Multi-threaded fleet simulator publishing TCU telemetry for every platform contract.

    # live stream to Mosquitto, real time, 2% anomalous messages
    uv run python -m covesa_lakehouse.data_generator.generate_telemetry --vehicles 40

    # one simulated day as fast as possible into a file (file-drop ingestion / fixtures)
    uv run python -m covesa_lakehouse.data_generator.generate_telemetry \
        --sink jsonl --out data/landing/day1.jsonl --duration 86400 --speedup 0 \
        --start 2026-09-01T06:00:00Z --truth data/landing/day1.truth.jsonl

Vehicles are spread round-robin over the platforms. Everything is reproducible from ``--seed``.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import random
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import IO, Protocol

import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion

from covesa_lakehouse.bronze.landing import S3Landing
from covesa_lakehouse.bronze.mqtt_bridge import LandingBuffer
from covesa_lakehouse.common.contract import Contract, load_contracts
from covesa_lakehouse.data_generator.faults import FaultManager
from covesa_lakehouse.data_generator.payloads import Message, PayloadEncoder
from covesa_lakehouse.data_generator.vehicle_model import VehicleProfile, VehicleSim
from covesa_lakehouse.data_generator.vin import make_vin


@dataclass
class SimVehicle:
    vin: str
    contract: Contract
    sim: VehicleSim
    faults: FaultManager
    encoder: PayloadEncoder

    def advance(
        self, seconds: float
    ) -> tuple[dict[str, float], list[str], list[tuple[float, str, str]]]:
        """Simulate ``seconds`` in physics steps of at most 1 s, independent of the publish rate.

        Returns the final snapshot, the DTCs active at the end, and lifecycle events tagged with
        their offset (seconds) inside the interval.
        """
        n = max(1, math.ceil(seconds - 1e-9))
        step = seconds / n
        events: list[tuple[float, str, str]] = []
        snapshot: dict[str, float] = {}
        dtcs: list[str] = []
        for i in range(n):
            snapshot = self.sim.step(step)
            dtcs, step_events = self.faults.step(snapshot, step)
            events.extend(((i + 1) * step, e, value) for e, value in step_events)
        return snapshot, dtcs, events


def build_fleet(
    contracts: dict[str, Contract],
    n_vehicles: int,
    seed: int,
    anomaly_rate: float,
    fault_rate: float,
) -> list[SimVehicle]:
    platform_ids = sorted(contracts)
    fleet = []
    for i in range(n_vehicles):
        contract = contracts[platform_ids[i % len(platform_ids)]]
        rng = random.Random(f"{seed}-{i}")
        vin = make_vin(contract.platform_id, rng)
        profile = VehicleProfile.random(contract.powertrain, rng, contract.vehicle)
        sim = VehicleSim(profile, rng)
        fleet.append(
            SimVehicle(
                vin=vin,
                contract=contract,
                sim=sim,
                faults=FaultManager(sim, rng, fault_rate),
                encoder=PayloadEncoder(
                    contract, vin, rng.choice(contract.hardware_revs), rng, anomaly_rate
                ),
            )
        )
    return fleet


# ---------------------------------------------------------------------- sinks
class Sink(Protocol):
    def send(self, message: Message) -> None: ...
    def close(self) -> None: ...


class StdoutSink:
    def __init__(self) -> None:
        self._lock = threading.Lock()

    def send(self, message: Message) -> None:
        with self._lock:
            print(message.topic, message.payload)

    def close(self) -> None:
        sys.stdout.flush()


class JsonlSink:
    """One line per message: {"topic": ..., "payload": "<raw payload string>"}."""

    def __init__(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh: IO[str] = path.open("w", encoding="utf-8", newline="\n")
        self._lock = threading.Lock()

    def send(self, message: Message) -> None:
        line = json.dumps({"topic": message.topic, "payload": message.payload})
        with self._lock:
            self._fh.write(line + "\n")

    def close(self) -> None:
        self._fh.close()


class S3LandingSink:
    """Writes straight into the landing zone as batch file drops (large backfills)."""

    def __init__(self, writer_id: str, max_records: int = 50_000) -> None:
        self._buffer = LandingBuffer(
            S3Landing(), writer_id, max_records=max_records, max_age_s=1e9, source="batch"
        )
        self._lock = threading.Lock()

    def send(self, message: Message) -> None:
        with self._lock:
            self._buffer.add(message.topic, message.payload.encode("utf-8"), 0)
            if self._buffer.due():
                self._buffer.flush()

    def close(self) -> None:
        with self._lock:
            self._buffer.flush()


class MqttSink:
    def __init__(self, host: str, port: int, qos: int, client_id: str) -> None:
        self._client = mqtt.Client(CallbackAPIVersion.VERSION2, client_id=client_id)
        self._client.max_queued_messages_set(0)
        self._client.connect(host, port, keepalive=30)
        self._client.loop_start()
        self._qos = qos
        self._last: mqtt.MQTTMessageInfo | None = None

    def send(self, message: Message) -> None:
        self._last = self._client.publish(message.topic, message.payload, qos=self._qos)

    def close(self) -> None:
        if self._last is not None:
            self._last.wait_for_publish(timeout=10)
        self._client.loop_stop()
        self._client.disconnect()


class TruthLog:
    """Ground truth of injected anomalies and fault lifecycles, for validating Silver/Gold."""

    def __init__(self, path: Path | None) -> None:
        self._fh = None
        if path:
            path.parent.mkdir(parents=True, exist_ok=True)
            self._fh = path.open("w", encoding="utf-8", newline="\n")
        self._lock = threading.Lock()

    def write(self, record: dict[str, object]) -> None:
        if self._fh:
            with self._lock:
                self._fh.write(json.dumps(record) + "\n")

    def close(self) -> None:
        if self._fh:
            self._fh.close()


# ---------------------------------------------------------------------- simulation loop
@dataclass
class RunConfig:
    hz: float
    duration_s: float  # simulated seconds; 0 = run until interrupted
    speedup: float  # simulated seconds per wall second; 0 = as fast as possible
    start: datetime


class Stats:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self._lock = threading.Lock()

    def add(self, key: str, n: int = 1) -> None:
        with self._lock:
            self.counts[key] += n

    def snapshot(self) -> Counter[str]:
        with self._lock:
            return self.counts.copy()


def run_worker(
    vehicles: list[SimVehicle],
    sink: Sink,
    truth: TruthLog,
    cfg: RunConfig,
    stats: Stats,
    stop: threading.Event,
) -> None:
    dt = 1.0 / cfg.hz
    wall0 = time.monotonic()
    tick = 0
    while not stop.is_set():
        sim_elapsed = tick * dt
        if cfg.duration_s and sim_elapsed >= cfg.duration_s:
            break
        ts = cfg.start + timedelta(seconds=sim_elapsed)
        for v in vehicles:
            snapshot, dtcs, events = v.advance(dt)
            for offset, event, value in events:
                at = ts - timedelta(seconds=dt - offset)
                truth.write({"ts": at.isoformat(), "vin": v.vin, "event": event, "value": value})
                stats.add(event)
            for message in v.encoder.encode(snapshot, ts, dtcs):
                sink.send(message)
                stats.add("messages")
                if message.anomaly:
                    stats.add(f"anomaly:{message.anomaly}")
                    truth.write(
                        {
                            "ts": ts.isoformat(),
                            "vin": v.vin,
                            "msg_id": message.msg_id,
                            "event": "anomaly",
                            "value": message.anomaly,
                            **message.anomaly_detail,
                        }
                    )
        tick += 1
        if cfg.speedup > 0:
            delay = wall0 + tick * dt / cfg.speedup - time.monotonic()
            if delay > 0:
                stop.wait(delay)


def make_sink(args: argparse.Namespace, worker: int) -> Sink:
    if args.sink == "mqtt":
        return MqttSink(
            args.mqtt_host, args.mqtt_port, args.qos, f"fleet-sim-{os.getpid()}-{worker}"
        )
    if args.sink == "stdout":
        return StdoutSink()
    raise AssertionError(args.sink)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--contracts", type=Path, default=Path("contracts"))
    p.add_argument("--platforms", default="", help="comma-separated platform_ids (default: all)")
    p.add_argument("--vehicles", type=int, default=22)
    p.add_argument("--threads", type=int, default=4)
    p.add_argument(
        "--hz", type=float, default=1.0, help="messages per vehicle per simulated second"
    )
    p.add_argument("--duration", type=float, default=0.0, help="simulated seconds (0 = forever)")
    p.add_argument(
        "--speedup", type=float, default=1.0, help="sim seconds per wall second (0 = max)"
    )
    p.add_argument("--start", default="", help="simulation start, ISO-8601 UTC (default: now)")
    p.add_argument("--anomaly-rate", type=float, default=0.02, help="share of anomalous messages")
    p.add_argument("--fault-rate", type=float, default=0.3, help="fault scenarios per driving hour")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--sink", choices=("mqtt", "jsonl", "s3", "stdout"), default="mqtt")
    p.add_argument(
        "--shard",
        default="0/1",
        help="K/N: simulate only vehicles K, K+N, ... (run N processes for large backfills)",
    )
    p.add_argument("--out", type=Path, default=Path("data/landing/telemetry.jsonl"))
    p.add_argument("--truth", type=Path, default=None, help="write ground-truth events (JSONL)")
    p.add_argument("--mqtt-host", default=os.environ.get("MQTT_HOST", "localhost"))
    p.add_argument("--mqtt-port", type=int, default=int(os.environ.get("MQTT_PORT", "1883")))
    p.add_argument("--qos", type=int, choices=(0, 1), default=1)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    contracts = load_contracts(args.contracts)
    if args.platforms:
        wanted = {p.strip() for p in args.platforms.split(",")}
        unknown = wanted - set(contracts)
        if unknown:
            sys.exit(f"unknown platform_id(s): {sorted(unknown)}")
        contracts = {k: v for k, v in contracts.items() if k in wanted}

    start = (
        datetime.fromisoformat(args.start.replace("Z", "+00:00"))
        if args.start
        else datetime.now(UTC).replace(microsecond=0)
    )
    cfg = RunConfig(hz=args.hz, duration_s=args.duration, speedup=args.speedup, start=start)
    fleet = build_fleet(contracts, args.vehicles, args.seed, args.anomaly_rate, args.fault_rate)
    shard, n_shards = (int(x) for x in args.shard.split("/"))
    fleet = fleet[shard::n_shards]  # the fleet is built in full, so VINs/seeds match any sharding
    n_threads = max(1, min(args.threads, len(fleet)))

    shared: Sink | None = None
    if args.sink == "jsonl":
        shared = JsonlSink(args.out)
    elif args.sink == "s3":
        shared = S3LandingSink(f"sim-{args.seed}-{shard}of{n_shards}")
    sinks: list[Sink] = [shared or make_sink(args, i) for i in range(n_threads)]
    truth = TruthLog(args.truth)
    for v in fleet:  # hidden per-vehicle truth, used to validate the Gold KPIs
        p = v.sim.p
        truth.write(
            {
                "ts": start.isoformat(),
                "vin": v.vin,
                "event": "vehicle_profile",
                "platform_id": v.contract.platform_id,
                "powertrain": p.powertrain,
                "state_of_health": round(p.state_of_health, 4),
                "aggressiveness": round(p.aggressiveness, 4),
                "battery_kwh": p.battery_kwh,
                "ambient_c": round(p.ambient_c, 2),
            }
        )
    stats, stop = Stats(), threading.Event()
    workers = [
        threading.Thread(
            target=run_worker,
            args=(fleet[i::n_threads], sinks[i], truth, cfg, stats, stop),
            name=f"fleet-{i}",
            daemon=True,
        )
        for i in range(n_threads)
    ]
    print(
        f"{len(fleet)} vehicles on {len(contracts)} platforms, {n_threads} threads -> {args.sink}"
        f" | start={start.isoformat()} hz={args.hz} speedup={args.speedup or 'max'}"
        f" anomaly_rate={args.anomaly_rate}",
        file=sys.stderr,
    )
    wall0 = time.monotonic()
    for w in workers:
        w.start()
    try:
        while any(w.is_alive() for w in workers):
            for w in workers:
                w.join(timeout=10)
            counts = stats.snapshot()
            rate = counts["messages"] / max(time.monotonic() - wall0, 1e-9)
            print(f"  {counts['messages']:,} messages ({rate:,.0f}/s)", file=sys.stderr)
    except KeyboardInterrupt:
        stop.set()
        for w in workers:
            w.join()
    finally:
        for sink in {id(s): s for s in sinks}.values():
            sink.close()
        truth.close()

    counts = stats.snapshot()
    anomalies = {k.split(":", 1)[1]: v for k, v in counts.items() if k.startswith("anomaly:")}
    print(f"done: {counts['messages']:,} messages, anomalies={anomalies}", file=sys.stderr)
    print(
        f"faults: started={counts['scenario_started']} dtc_raised={counts['dtc_raised']}",
        file=sys.stderr,
    )


if __name__ == "__main__":
    main()

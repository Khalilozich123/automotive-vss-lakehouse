# COVESA-Compliant Telemetry Lakehouse

Multi-ECU and fleet signal harmonization for Software-Defined Vehicles: heterogeneous CAN / TCU
telemetry is landed, conformed to the [COVESA Vehicle Signal Specification](https://github.com/COVESA/vehicle_signal_specification)
and served as analytical marts, on a Medallion lakehouse (Bronze → Silver → Gold) built with
PySpark and Apache Iceberg.

> Status: **Phase 3 (Silver conformance engine + DLQ)** complete. See the phase plan below.

## Local stack

Everything runs in Docker; nothing is installed on the host.

| Service | Role | Host port |
|---|---|---|
| `rustfs` | S3-compatible object storage (local stand-in for AWS S3) | 9000 (API), [9001](http://localhost:9001) (console) |
| `postgres` | Backing database for the Iceberg catalog | 5432 |
| `iceberg-rest` | Apache Iceberg REST catalog | 8181 |
| `spark-master` / `spark-worker` | Spark 3.5 standalone cluster | [8080](http://localhost:8080) / [8081](http://localhost:8081) |
| `mosquitto` | MQTT broker emulating TCU → cloud delivery | 1883 |
| `mqtt-bridge` | Lands MQTT messages in `s3://landing` (Bronze landing zone) | — |
| `workspace` | Dev container and Spark driver (Python 3.11, Java 17, uv) | [4040](http://localhost:4040) (job UI) |
| `fleet-simulator` | Live fleet telemetry, opt-in: `docker compose --profile simulate up -d` | — |

All ports bind to `127.0.0.1`. Default credentials are local-only; override them with the
`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `CATALOG_DB_USER`, `CATALOG_DB_PASSWORD` environment variables.
The VIN pseudonymization key (`VIN_PSEUDONYMIZATION_KEY`, `VIN_PSEUDONYMIZATION_KEY_ID`) also has a
local-only default; in production it would come from a secret manager.

Pinned versions: Spark 3.5.9 · Iceberg 1.10.1 · Python 3.11 · Java 17. The driver (`workspace`) and
the executors are built from the same `docker/Dockerfile.spark`, so their versions always match.

> **Why RustFS instead of MinIO?** MinIO stopped publishing community Docker images in 2025, so a
> MinIO-based stack can no longer be reproduced from a clean machine. RustFS is an S3-compatible,
> Apache-2.0 replacement with the same ports and a web console; nothing in the code is
> RustFS-specific (Iceberg talks plain S3 through `S3FileIO`).

## Getting started

**Option A — VS Code (recommended).** Open the folder and choose **Reopen in Container**. The whole
stack starts and dependencies are installed into a named volume.

**Option B — command line.**

```bash
docker compose up -d --build --wait
docker compose exec workspace uv sync --frozen
docker compose exec workspace uv run pytest            # unit + integration
```

Quality checks (inside the workspace container):

```bash
uv run ruff check . && uv run black --check . && uv run mypy && uv run pytest -m "not integration"
```

Stop with `docker compose down` (add `-v` to wipe all data volumes).

## Data sources and platform contracts

Schemas come from real production CAN databases, and all telemetry is simulated from them. No
hardware or proprietary data is needed. See [`sources/README.md`](sources/README.md) for provenance
and licenses.

| Platform contract | Powertrain | Source DBC | Encoding | Signals |
|---|---|---|---|---|
| `VW_MQB` | ICE | opendbc `vw_mqb` | raw CAN frames | 9 |
| `STELLANTIS_PSA_AEE2010` | ICE | opendbc `psa_aee2010_r3` | raw CAN frames | 8 |
| `BMW_E9X` | ICE | opendbc `bmw_e9x_e8x` | raw CAN frames | 6 |
| `STELLANTIS_FCA_GIORGIO` | ICE | opendbc `fca_giorgio` | JSON | 7 |
| `HYUNDAI_KIA_CCAN_2015` | ICE | opendbc `hyundai_2015_ccan` | JSON | 10 |
| `TOYOTA_PRIUS_GEN3` | HEV | opendbc `toyota_prius_2010_pt` | JSON | 5 |
| `GWM_HAVAL_H6_PHEV` | PHEV | opendbc `gwm_haval_h6_phev_2024` | JSON | 1 |
| `TESLA_MODEL_S` | BEV | opendbc `tesla_powertrain` | JSON | 4 |
| `GM_BOLT_EV` | BEV | opendbc `gm_global_a_high_voltage_management` | JSON | 2 |
| `BYD_ATTO3` | BEV | opendbc `byd_atto3` | JSON | 4 |
| `RENAULT_CMF_EV` | BEV | **synthetic** (hand-written, follows the spec example) | JSON | 16 |

### How a contract is made

```
sources/dbc/<platform>.dbc  ──┐
sources/mappings/<platform>.yaml ──┼──▶ dbc_parser ──▶ contracts/<platform>.yaml
sources/vss/vss.json (VSS 6.1) ──┤        (validated, generated, committed)
sources/mappings/_bounds.yaml ──┘
```

* **Curated by hand:** which DBC signal corresponds to which VSS path (a few lines per platform).
  No tool can reliably infer that `Pack_Voltage`, `BMS_packVoltage` and `HV_BATT_VOLTAGE` all
  mean `Vehicle.Powertrain.TractionBattery.CurrentVoltage`.
* **Automated:** bit layout, scale/offset, conversion from the DBC unit to the VSS unit
  (mph→km/h, g→m/s², rad/s→deg/s, km→m, ratio→%) folded into the linear decode, and plausibility
  bounds. Each `vss_path` is checked against the pinned VSS release.

The validation caught a real defect in the original specification: its example contract uses
`Vehicle.Powertrain.TractionBattery.Voltage`, which does not exist in VSS 6.1. The correct leaf is
`TractionBattery.CurrentVoltage`.

**Onboarding a new platform:** add the DBC and a mapping file, then run the parser.

```bash
uv run python -m covesa_lakehouse.data_generator.dbc_parser          # writes contracts/
uv run python -m covesa_lakehouse.data_generator.dbc_parser --check  # CI: fails if contracts are stale
```

## Fleet simulator

`generate_telemetry` simulates a multi-platform fleet. Each vehicle runs a physics-lite model
that produces values in **VSS terms**. The model covers road load, regenerative braking, battery
I²R heating, state-of-health effects, DC charging sessions, engine warm-up and driver
aggressiveness. Each vehicle's contract then **encodes** those values into its platform's raw
integers or CAN frames. In other words, the simulator runs the Silver conformance step in reverse,
which gives Phase 3 a known ground truth to test against.

```bash
# live, real-time stream to Mosquitto (topic: vehicles/<platform_id>/<vin>/telemetry)
uv run python -m covesa_lakehouse.data_generator.generate_telemetry --vehicles 44

# one simulated day as fast as possible into a file, with ground truth (~1.9M messages, ~5 min)
uv run python -m covesa_lakehouse.data_generator.generate_telemetry --sink jsonl \
  --out data/landing/day1.jsonl --truth data/landing/day1.truth.jsonl \
  --duration 86400 --speedup 0 --start 2026-09-01T00:00:00Z
```

**Data-quality anomalies** (`--anomaly-rate`, default 2% of messages) are injected for the Silver
layer to handle without stopping the stream:

| Anomaly | What happens | Expected handling |
|---|---|---|
| `out_of_range` | A physically impossible value (or an all-ones "not available" CAN field) | Quarantine (DLQ) |
| `missing_signal` | A JSON key is missing, or a whole CAN frame is lost | Null / gap |
| `stuck_value` | A sensor freezes on one value for 20–120 messages | Flatline check |
| `corrupt_payload` | Truncated JSON or a short CAN frame | Parse error → DLQ |
| `duplicate` | The same message is delivered twice (same `msg_id`) | Deduplicate |
| `late_arrival` | The event time is 30–600 s behind the stream | Event-time handling |

**Fault scenarios** (`--fault-rate`) drift the telemetry for 3–15 minutes and then raise a DTC.
This gives the predictive-DTC mart (Phase 4) real precursors to find:

| DTC | Precursor | Powertrains |
|---|---|---|
| P0A7E battery over temperature | `TractionBattery.Temperature.Max` climbs about 1.5 °C/min | BEV, PHEV, HEV |
| P0A80 replace battery pack | spread between `CellVoltage.Max` and `CellVoltage.Min` widens | BEV, PHEV, HEV |
| P0217 coolant over temperature | `EngineCoolant.Temperature` rises past the thermostat band | ICE, HEV, PHEV |
| P0562 system voltage low | 12 V battery sags from ~14 V toward 11 V | ICE, HEV, PHEV |
| P0300 misfire | `CombustionEngine.Speed` jitter grows | ICE, HEV, PHEV |
| U0100 lost communication with ECM | powertrain signals increasingly missing | all |

`--truth` writes every injected anomaly and fault lifecycle to a JSONL file.
`tests/fixtures/telemetry_sample*.jsonl` is a small captured sample used by the tests.

## Bronze: landing and ingestion

```
fleet ──MQTT──▶ mqtt-bridge ──JSONL──▶ s3://landing/telemetry/mqtt/dt=…/hour=…/   ┐
backfills / file drops ─────────────▶ s3://landing/telemetry/batch/…             ┴▶ ingest_raw ──▶ lakehouse.bronze.telemetry_raw
```

Spark has no maintained MQTT source, so a small **bridge** subscribes to `vehicles/#` and writes
micro-batches (2,000 messages or 5 s) to an S3 **landing zone**. AWS IoT Core → Firehose → S3
follows the same pattern. Live data and file drops become the same thing, and **one generic Spark
job** ingests both.

**Delivery guarantees**

| Hop | Guarantee | How |
|---|---|---|
| Vehicle → bridge → landing | at-least-once | QoS 1, persistent MQTT session, and *manual* acks sent only after the batch is written to S3 |
| Landing → Bronze | exactly-once | Structured Streaming file source (processed files are tracked in the checkpoint) plus atomic Iceberg commits |

Duplicates can enter only at the first hop (a redelivery after a crash, or a TCU resending a
message). They keep their `msg_id` and are removed in Silver. Bronze keeps everything.

**`lakehouse.bronze.telemetry_raw`** (Iceberg v2, partitioned by `days(ingested_at)`, zstd)

| Column | Meaning |
|---|---|
| `ingestion_id` | UUID of the Bronze row |
| `vin`, `vin_key_id` | Keyed pseudonym (HMAC-SHA256) of the VIN, and the id of the key that produced it |
| `platform_id` | Taken from the MQTT topic, so even unparseable payloads are attributed |
| `hardware_rev`, `msg_id` | Envelope metadata, lifted only when the payload is valid JSON |
| `received_at`, `ingested_at` | Landing-writer receive time and Bronze commit time (UTC) |
| `source_system`, `source_file` | Lineage: `mqtt` or `batch`, and the exact landing object |
| `payload_sha256` | Hash of the stored payload, for audits and replay checks |
| `raw_payload` | The payload exactly as received, except that the VIN is replaced by its pseudonym |

**Privacy by design.** The VIN is pseudonymized with **HMAC-SHA256 under a secret key**, not a
plain SHA-256. VINs have a small, structured search space (manufacturer prefix, model year,
6-digit serial), so an unkeyed hash can be reversed by brute force. The pseudonym is computed as a
native Spark expression implementing RFC 2104 (no Python UDF) and is tested against Python's
`hmac`. The VIN is replaced **inside `raw_payload` too**, so no raw VIN reaches the lakehouse.
Rotating the key is traceable through `vin_key_id`.

```bash
uv run python -m covesa_lakehouse.bronze.ingest_raw                # drain new landing files, then stop
uv run python -m covesa_lakehouse.bronze.ingest_raw --continuous   # keep streaming (30 s micro-batches)
uv run python -m covesa_lakehouse.bronze.landing upload my_backfill.jsonl   # file drop
```

**Measured locally:** one simulated fleet-day (22 vehicles at 1 Hz: 1.9 M messages, 1 GB of
JSONL) was ingested in **68 s end to end, about 28 k rows/s**, on a single 4-core Spark worker,
including JVM start-up.

## Silver: metadata-driven conformance to COVESA VSS

```
bronze.telemetry_raw ─▶ parse envelope ─▶ message checks ─┬─ JSON: explode signals ─┐
 (Iceberg stream,                                          └─ CAN:  explode frames  ─┴─ join contract specs ─▶ decode ─▶ range check
  foreachBatch)                                                                         (broadcast, reloaded            │
                                                                                         every micro-batch)             ├─▶ silver.telemetry_signals
                                                                                                                        ├─▶ silver.telemetry_quarantine (DLQ)
                                                                                                                        └─▶ silver.dtc_events
```

**There is no per-platform code.** At every micro-batch, `contracts/` is reloaded and flattened into
a small *spec table*: one row per platform signal, with its scaling, bounds and pre-computed CAN
bit window. Decoding is a single generic Spark plan that **joins** messages to that table. JSON
signals are matched by name and CAN frames by frame id. Bit extraction (Intel and Motorola,
signed, simple multiplexing) is a native SQL expression parameterized by the spec columns, with
no Python UDF. Then `physical = raw × scale + offset` is applied and the result is checked against
the plausibility bounds.

**Onboarding a platform means adding one YAML file.** The integration test proves it: a *running*
streaming query quarantines a platform as `UNKNOWN_PLATFORM`, a contract file is then dropped in,
and the same query (never restarted) conforms that platform's next messages. A contract that
fails validation is skipped and reported, and the rest of the fleet keeps flowing.

**Dead Letter Queue** (`silver.telemetry_quarantine`): bad data is routed, never dropped, and never
stops the stream.

| Level | Error code | Cause |
|---|---|---|
| message | `PARSE_ERROR` | Not a complete, valid telemetry envelope (e.g. truncated) |
| message | `UNKNOWN_PLATFORM` | No valid contract for the platform |
| message | `ENCODING_MISMATCH` | E.g. a firmware update switched from JSON to CAN frames |
| message | `INVALID_TIMESTAMP` | Event time missing or unparseable |
| signal | `MALFORMED_FRAME` | CAN frame with bad hex or the wrong length |
| signal | `INVALID_RAW_VALUE` | JSON raw value that is not an integer |
| signal | `PHYSICAL_RANGE_VIOLATION` | Decoded value outside the plausibility bounds |

Signal-level errors quarantine **only the offending reading**: one glitching sensor does not throw
away the other readings in the same message. Message-level errors keep the raw payload for triage
and replay.

**Exactly-once and deduplication.** Each micro-batch is written with an Iceberg
`MERGE … WHEN NOT MATCHED THEN INSERT`, keyed on `msg_id` (plus the defect key in the DLQ). This
removes duplicate deliveries, including duplicates that arrive minutes apart, and makes a retried or
fully replayed batch harmless. A replay of every Bronze row from a fresh checkpoint inserted
**0 rows**.

**Verified against independent oracles:**

* Bit level: Spark's CAN extraction matches the Phase 1 `can_codec` (itself cross-checked
  against `cantools`) on every signal layout in every contract.
* Message level: Spark's decoded values match the pure-Python `reference_decoder` exactly on the
  captured sample.
* Anomaly level: the DLQ contains exactly the out-of-range values and corrupt payloads that the
  simulator's ground truth says were injected.

**Lessons learned while building it** (each is documented in the code):

* `from_json` can return a *partial* struct for a malformed document, e.g. `msg_id` and `ts` read
  from a truncated payload, and whether it does depends on the execution plan. Envelopes are
  therefore first validated as complete JSON (`get_json_object(payload, '$')`).
* Iceberg's per-session catalog cache made one Spark session miss another session's commits, such
  as those from the streaming `foreachBatch` session. The cache is disabled; the REST catalog is
  the source of truth.
* Caching the micro-batch input (about 125 k raw payloads per block) ran executors out of heap on
  a backfill. Each MERGE now streams its slice from Iceberg, and row counts come from the commit
  summaries instead of re-counting.

```bash
uv run python -m covesa_lakehouse.silver.conformance_engine                # drain new Bronze rows, then stop
uv run python -m covesa_lakehouse.silver.conformance_engine --continuous   # keep streaming
```

**Measured locally:** one simulated fleet-day (1.9 M Bronze rows) produced **12.4 M VSS readings,
18 k quarantined records and 42.6 k DTC events in about 4.4 min** on a single 4-core worker, about
48 k decoded readings/s.

## Repository layout

```
├─ compose.yaml                 # includes docker/docker-compose.yml so `docker compose up` works from root
├─ docker/                      # stack definition, Spark image, REST catalog image, Mosquitto config
├─ .devcontainer/               # VS Code devcontainer → the workspace service
├─ sources/                     # vendored DBCs + VSS release, curated mappings, bounds, DTC catalog
├─ contracts/                   # GENERATED platform contracts (the runtime source of truth)
├─ src/covesa_lakehouse/
│  ├─ common/
│  │  ├─ spark_session.py       # SparkSession factory (Iceberg REST + S3FileIO)
│  │  ├─ contract.py            # Pydantic contract model + loader
│  │  ├─ config_loader.py       # contract dir -> validated spec table (hot reload)
│  │  ├─ vss.py                 # VSS tree catalog
│  │  ├─ can_codec.py           # DBC bit packing (Intel/Motorola), verified against cantools
│  │  └─ reference_decoder.py   # pure-Python decoder, the oracle for the Spark engine
│  ├─ data_generator/
│  │  ├─ fetch_sources.py       # download pinned upstream sources
│  │  ├─ dbc_loader.py          # tolerant DBC loading (repairs known opendbc defects)
│  │  ├─ dbc_parser.py          # DBC + mapping → validated contract
│  │  ├─ vehicle_model.py       # physics-lite, VSS-native vehicle simulation
│  │  ├─ faults.py              # precursor → DTC fault scenarios
│  │  ├─ payloads.py            # contract-driven encoding + anomaly injection
│  │  └─ generate_telemetry.py  # multi-threaded fleet CLI (MQTT / JSONL / stdout)
│  ├─ bronze/
│  │  ├─ mqtt_bridge.py         # MQTT -> landing zone (persistent session, ack after write)
│  │  ├─ landing.py             # landing-zone layout, S3 writer, file-drop upload CLI
│  │  ├─ pseudonymize.py        # keyed HMAC-SHA256 VIN pseudonyms as a Spark expression
│  │  └─ ingest_raw.py          # generic Structured Streaming ingest into Iceberg Bronze
│  ├─ silver/
│  │  ├─ conformance_engine.py  # contract-driven decode -> Silver / DLQ / DTC tables (MERGE)
│  │  ├─ can_decoder.py         # CAN bit extraction + scaling as native Spark expressions
│  │  └─ data_quality.py        # error codes, validation rules, DLQ routing
│  └─ gold/                     # Phase 4
└─ tests/
   ├─ unit/                     # no external services needed (run in CI)
   ├─ integration/              # need the compose stack (marker: integration)
   └─ fixtures/                 # captured sample payloads + ground truth
```

## Delivery plan

| Phase | Milestone | Status |
|---|---|---|
| 0 | Dockerized lakehouse + CI | ✅ |
| 1 | DBC → YAML contracts, MQTT telemetry generator | ✅ |
| 2 | Bronze: generic append-only ingestion with lineage | ✅ |
| 3 | Silver: contract-driven conformance engine + DLQ | ✅ |
| 4 | Gold: battery SoH, predictive DTC, eco-driving marts | ⏳ |
| 5 | Orchestration, tests, quality gates | |
| 6 | Dashboard, benchmarks, one-command demo | |

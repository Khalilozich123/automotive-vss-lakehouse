# COVESA-Compliant Telemetry Lakehouse

Multi-ECU and fleet signal harmonization for Software-Defined Vehicles: heterogeneous CAN / TCU
telemetry is landed, conformed to the [COVESA Vehicle Signal Specification](https://github.com/COVESA/vehicle_signal_specification)
and served as analytical marts, on a Medallion lakehouse (Bronze → Silver → Gold) built with
PySpark and Apache Iceberg.

> Status: **Phase 0 — Foundations & local environment** complete. See the phase plan below.

## Local stack

Everything runs in Docker; nothing is installed on the host.

| Service | Role | Host port |
|---|---|---|
| `rustfs` | S3-compatible object storage (local stand-in for AWS S3) | 9000 (API), [9001](http://localhost:9001) (console) |
| `postgres` | Backing database for the Iceberg catalog | 5432 |
| `iceberg-rest` | Apache Iceberg REST catalog | 8181 |
| `spark-master` / `spark-worker` | Spark 3.5 standalone cluster | [8080](http://localhost:8080) / [8081](http://localhost:8081) |
| `mosquitto` | MQTT broker emulating TCU → cloud delivery | 1883 |
| `workspace` | Dev container and Spark driver (Python 3.11, Java 17, uv) | [4040](http://localhost:4040) (job UI) |

All ports bind to `127.0.0.1`. Default credentials are local-only; override them with the
`S3_ACCESS_KEY`, `S3_SECRET_KEY`, `CATALOG_DB_USER`, `CATALOG_DB_PASSWORD` environment variables.

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

## Repository layout

```
├─ compose.yaml                 # includes docker/docker-compose.yml so `docker compose up` works from root
├─ docker/
│  ├─ docker-compose.yml        # the stack
│  ├─ Dockerfile.spark          # runtime (Spark nodes) + dev (workspace) targets
│  ├─ rest-catalog/Dockerfile   # Iceberg REST catalog + Postgres JDBC driver
│  └─ mosquitto/mosquitto.conf
├─ .devcontainer/               # VS Code devcontainer → the workspace service
├─ contracts/                   # Phase 1+: declarative YAML platform contracts
├─ data_generator/              # Phase 1: DBC → YAML parser, telemetry publisher
├─ src/covesa_lakehouse/
│  ├─ common/spark_session.py   # SparkSession factory (Iceberg REST + S3FileIO)
│  ├─ bronze/ silver/ gold/     # Phases 2–4
└─ tests/
   ├─ unit/                     # no external services needed (run in CI)
   └─ integration/              # need the compose stack (marker: integration)
```

## Delivery plan

| Phase | Milestone | Status |
|---|---|---|
| 0 | Dockerized lakehouse + CI | ✅ |
| 1 | DBC → YAML contracts, MQTT telemetry generator | ⏳ |
| 2 | Bronze: generic append-only ingestion with lineage | |
| 3 | Silver: contract-driven conformance engine + DLQ | |
| 4 | Gold: battery SoH, predictive DTC, eco-driving marts | |
| 5 | Orchestration, tests, quality gates | |
| 6 | Dashboard, benchmarks, one-command demo | |

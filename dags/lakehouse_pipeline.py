"""
### COVESA telemetry lakehouse: Bronze -> Silver -> Gold, with quality gates

| task | what it does |
|---|---|
| `bronze_ingest` | drain new landing-zone files into `bronze.telemetry_raw` (exactly once) |
| `dq_bronze` | privacy gate (no raw VIN), attribution, unique ids |
| `silver_conform` | contract-driven decoding to COVESA VSS, DLQ, dedup (MERGE) |
| `dq_silver` | dedup, quarantine and parse-error rates, unknown platforms, stuck sensors, drift |
| `gold_build` | vehicle-state pivot + battery / DTC / eco-driving marts for the window |
| `dq_gold` | KPI plausibility and lineage |

An error-severity quality check fails its task, so bad data never reaches the next layer.
Every job is idempotent: a retry or a re-run is always safe.
The window (`days_back`, default 1) is "today and the N previous days", so late data is
re-processed. Trigger with `{"days_back": 14}` to rebuild two weeks.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

PYTHON = os.environ.get("LAKEHOUSE_PYTHON", "python")
HOME = os.environ.get("LAKEHOUSE_HOME", ".")
WINDOW = "--last-days {{ params.days_back }}"


def job(task_id: str, module: str, args: str = "", **kwargs: object) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {HOME} && {PYTHON} -m covesa_lakehouse.{module} {args}",
        **kwargs,
    )


with DAG(
    dag_id="lakehouse_pipeline",
    description="Bronze -> Silver -> Gold with data-quality gates",
    doc_md=__doc__,
    schedule="@hourly",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,  # Spark jobs share one small cluster
    params={"days_back": Param(1, type="integer", minimum=0, maximum=60)},
    default_args={
        "owner": "data-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["lakehouse", "covesa"],
):
    bronze = job("bronze_ingest", "bronze.ingest_raw")
    dq_bronze = job("dq_bronze", "quality.checks", f"--layer bronze {WINDOW}", retries=0)
    silver = job("silver_conform", "silver.conformance_engine")
    dq_silver = job("dq_silver", "quality.checks", f"--layer silver {WINDOW}", retries=0)
    gold = job("gold_build", "gold.build", WINDOW)
    dq_gold = job("dq_gold", "quality.checks", f"--layer gold {WINDOW}", retries=0)

    bronze >> dq_bronze >> silver >> dq_silver >> gold >> dq_gold

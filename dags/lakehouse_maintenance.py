"""
### Iceberg maintenance

Compacts small files (from streaming appends and per-batch MERGEs) and expires snapshots older
than 7 days, keeping at least 10, for every table in bronze / silver / gold / ops.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PYTHON = os.environ.get("LAKEHOUSE_PYTHON", "python")
HOME = os.environ.get("LAKEHOUSE_HOME", ".")

with DAG(
    dag_id="lakehouse_maintenance",
    description="Iceberg compaction and snapshot expiry",
    doc_md=__doc__,
    schedule="30 3 * * *",  # daily, off-peak
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["lakehouse", "maintenance"],
):
    BashOperator(
        task_id="compact_and_expire",
        bash_command=f"cd {HOME} && {PYTHON} -m covesa_lakehouse.ops.maintenance",
        execution_timeout=timedelta(hours=2),
    )

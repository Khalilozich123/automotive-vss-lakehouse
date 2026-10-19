"""CI gate: every DAG imports cleanly and the expected pipelines exist.

Run with Airflow's interpreter:  python /opt/lakehouse/dags/check_dags.py
"""

from __future__ import annotations

import os
import sys

from airflow.dag_processing.dagbag import DagBag

EXPECTED = {"lakehouse_pipeline", "lakehouse_maintenance"}

bag = DagBag(dag_folder=os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "dags"))
problems = [f"import error in {path}: {err}" for path, err in bag.import_errors.items()]
missing = EXPECTED - set(bag.dag_ids)
if missing:
    problems.append(f"missing DAGs: {sorted(missing)}")
for dag_id in sorted(EXPECTED & set(bag.dag_ids)):
    dag = bag.get_dag(dag_id)
    assert dag is not None
    print(f"{dag_id}: {len(dag.tasks)} tasks: {', '.join(t.task_id for t in dag.tasks)}")
if problems:
    sys.exit("\n".join(problems))

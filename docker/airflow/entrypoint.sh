#!/usr/bin/env bash
# Create the Airflow metadata database on the shared Postgres (idempotent), migrate, then run.
set -euo pipefail

python - <<'PY'
import os
import time

import psycopg2

params = dict(
    host=os.environ.get("AIRFLOW_DB_HOST", "postgres"),
    user=os.environ["AIRFLOW_DB_USER"],
    password=os.environ["AIRFLOW_DB_PASSWORD"],
    dbname="postgres",
)
for attempt in range(30):
    try:
        conn = psycopg2.connect(**params)
        break
    except psycopg2.OperationalError:
        time.sleep(2)
else:
    raise SystemExit("Postgres is not reachable")
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'airflow'")
    if cur.fetchone() is None:
        cur.execute("CREATE DATABASE airflow")
        print("created database airflow")
conn.close()
PY

airflow db migrate
exec airflow "$@"

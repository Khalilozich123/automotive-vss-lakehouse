#!/usr/bin/env bash
# One-command demo: stack up -> simulate a fleet -> full pipeline DAG with quality gates ->
# validation against ground truth -> dashboard. Every step runs in a container; the host only
# needs Docker and bash (Git Bash on Windows).
#
#   scripts/demo.sh                    # 3 simulated days x 44 vehicles (about 10 min)
#   DAYS=14 scripts/demo.sh            # the full validation dataset (about 45 min)
#
# The simulated period ends at today's midnight (UTC), so the hourly DAG's "last N days" window
# covers it. Re-running is safe: every layer is idempotent.
set -euo pipefail

DAYS=${DAYS:-3}
VEHICLES=${VEHICLES:-44}
SHARDS=${SHARDS:-4}
cd "$(dirname "$0")/.."
dc() { docker compose "$@"; }
step() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

step "1/5 Start the stack"
dc up -d --build --wait
dc exec -T workspace uv sync --frozen

step "2/5 Simulate $DAYS days x $VEHICLES vehicles into the landing zone ($SHARDS shards)"
dc exec -T -e DAYS="$DAYS" -e VEHICLES="$VEHICLES" -e SHARDS="$SHARDS" workspace bash -c '
  set -euo pipefail
  start=$(date -u -d "-$DAYS days" +%Y-%m-%dT00:00:00Z)
  rm -rf data/demo && mkdir -p data/demo
  for k in $(seq 0 $((SHARDS - 1))); do
    uv run python -m covesa_lakehouse.data_generator.generate_telemetry --sink s3 \
      --shard "$k/$SHARDS" --vehicles "$VEHICLES" --hz 0.2 --duration $((DAYS * 86400)) \
      --speedup 0 --start "$start" --seed 2026 --fault-rate 0.1 --threads 1 \
      --truth "data/demo/truth-$k.jsonl" &
  done
  wait'

step "3/5 Run the lakehouse_pipeline DAG: Bronze -> Silver -> Gold, gated by data-quality checks"
dc exec -T airflow airflow dags test lakehouse_pipeline -c "{\"days_back\": $DAYS}"

step "4/5 Validate the Gold KPIs against the simulator's hidden truth"
dc exec -T workspace uv run python -m covesa_lakehouse.gold.validate --truth 'data/demo/truth-*.jsonl'

step "5/5 Dashboard"
dc up -d --wait dashboard
echo "Open http://localhost:8501 (Airflow: http://localhost:8088, Spark: http://localhost:8080)"

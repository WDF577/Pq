#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "== Flink checkpoint recovery drill =="
docker compose ps --status running >/dev/null

wait_for_jobs() {
  local deadline=$((SECONDS + 180))
  until python3 - <<'PY'
import json
import urllib.request

with urllib.request.urlopen("http://localhost:8081/jobs/overview", timeout=5) as response:
    jobs = json.load(response)["jobs"]
healthy = (
    len(jobs) == 9
    and all(job["state"] == "RUNNING" for job in jobs)
    and all(job["tasks"]["running"] == job["tasks"]["total"] for job in jobs)
)
raise SystemExit(0 if healthy else 1)
PY
  do
    if (( SECONDS >= deadline )); then
      echo "Flink jobs did not recover within 180 seconds." >&2
      docker compose ps >&2
      exit 1
    fi
    sleep 3
  done
}

restore_count() {
  docker logs rtdw_flink_jobmanager 2>&1 \
    | grep -c "Restoring job .* from Checkpoint" || true
}

wait_for_new_restore() {
  local previous_count="$1"
  local deadline=$((SECONDS + 180))
  while (( $(restore_count) <= previous_count )); do
    if (( SECONDS >= deadline )); then
      echo "No new checkpoint restore evidence appeared within 180 seconds." >&2
      exit 1
    fi
    sleep 2
  done
}

echo "0. Confirm all nine jobs are healthy before fault injection"
wait_for_jobs

echo "1. Start a bounded producer in the background"
python3 scripts/generate_order_journeys.py \
  --journeys "${JOURNEYS:-10000}" \
  --time-span-minutes 20 \
  --interval 0.001 &
producer_pid=$!

echo "2. Let checkpoints and operator state advance"
sleep 15

echo "3. Kill TaskManager to simulate an unplanned process failure"
restore_count_before="$(restore_count)"
docker kill rtdw_flink_taskmanager >/dev/null

echo "4. Ask the local orchestrator to relaunch TaskManager, then wait for checkpoint recovery"
# A manual `docker kill` is considered an operator action and may not trigger
# Docker's restart policy. `compose up` models the relaunch that Kubernetes or
# another production orchestrator would perform after detecting the failure.
docker compose up -d flink-taskmanager >/dev/null
wait_for_new_restore "$restore_count_before"
wait_for_jobs

wait "$producer_pid"
sleep 20

echo "5. Reload Kafka outputs and run business-key acceptance checks"
python3 scripts/load_kafka_to_clickhouse.py \
  --group-id "fault_drill_loader_$(date +%s)" \
  --max-messages 0 \
  --idle-timeout 20
bash scripts/verify_result.sh

echo "Fault drill passed. Capture Flink checkpoint/restart screenshots for interview evidence."

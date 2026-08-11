#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "== Realtime data warehouse local demo =="
echo "Warning: this rebuilds the project containers and resets its Docker volumes."
echo

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing command: $cmd. Install it before running the demo." >&2
    exit 1
  fi
}

need_cmd docker
need_cmd python3
need_cmd curl

echo "1. Check Python dependencies"
python3 - <<'PY' >/dev/null 2>&1 || python3 -m pip install -r requirements.txt
import confluent_kafka
import mysql.connector
PY

echo "2. Check Flink connector jars"
bash scripts/download_connectors.sh

echo "3. Rebuild only the core infrastructure"
docker compose down -v --remove-orphans
docker compose up -d --build \
  zookeeper kafka mysql clickhouse \
  flink-volume-init flink-jobmanager flink-taskmanager

echo "4. Wait for core infrastructure health"
until docker exec rtdw_mysql mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done
until docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --list >/dev/null 2>&1; do
  sleep 2
done
until docker exec rtdw_clickhouse wget -qO- http://localhost:8123/ping >/dev/null 2>&1; do
  sleep 2
done
until curl -fsS http://localhost:8081/overview >/dev/null 2>&1; do
  sleep 2
done

echo "5. Initialize MySQL dimensions and business tables"
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim_expanded.sql
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_business_tables.sql

echo "6. Create Kafka topics with their declared cleanup policies"
bash scripts/create_kafka_topics.sh

echo "7. Initialize ClickHouse tables before the loader starts"
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/create_clickhouse_tables.sql

echo "8. Start the long-running loader, freshness exporter, and monitoring stack"
docker compose up -d --build \
  clickhouse-loader pipeline-freshness-exporter prometheus grafana
until [[ "$(docker inspect --format='{{.State.Health.Status}}' rtdw_clickhouse_loader 2>/dev/null)" == "healthy" ]]; do
  sleep 2
done
until [[ "$(docker inspect --format='{{.State.Health.Status}}' rtdw_pipeline_freshness_exporter 2>/dev/null)" == "healthy" ]]; do
  sleep 2
done

echo "9. Submit the behavior and order CDC Flink SQL jobs"
bash scripts/run_flink_sql.sh
MSYS_NO_PATHCONV=1 docker exec rtdw_flink_jobmanager /opt/flink/bin/flink list
jobs_ready=0
for _ in $(seq 1 100); do
  if python3 - <<'PY'
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
  then
    jobs_ready=1
    break
  fi
  sleep 3
done
if [[ "$jobs_ready" -ne 1 ]]; then
  echo "Nine Flink jobs did not become healthy within 300 seconds." >&2
  exit 1
fi

echo "10. Generate traceable user journeys and mutable order transactions"
python3 scripts/generate_order_journeys.py --journeys 5000 --interval 0 --time-span-minutes 120
python3 scripts/generate_order_transactions.py --orders 500

echo "11. Rehearse an additive nullable-field schema evolution"
bash scripts/schema_evolution_drill.sh

echo "12. Wait 30 seconds for Flink output to reach the long-running loader"
sleep 30

echo "13. Verify results and generate the data-quality report"
bash scripts/verify_result.sh
python3 scripts/quality_report.py --output artifacts/latest_quality_report.md

echo
echo "Demo complete. Flink: http://localhost:8081"
echo "Prometheus: http://localhost:9090"
echo "Grafana: http://localhost:3000 (local default: admin/admin)"
echo "Stop the environment with: docker compose down"

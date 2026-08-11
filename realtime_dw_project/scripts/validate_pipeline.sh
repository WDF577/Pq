#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "1. check connector jars"
bash scripts/download_connectors.sh
python3 -m compileall scripts tests
python3 -m pytest -q

echo "2. reset old demo containers and volumes"
docker compose down -v --remove-orphans

echo "3. start docker services"
docker compose up -d --build

echo "4. wait for infrastructure"
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

echo "5. initialize mysql dimensions"
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim_expanded.sql
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_business_tables.sql

echo "6. create kafka topics"
bash scripts/create_kafka_topics.sh

echo "7. initialize clickhouse tables"
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/create_clickhouse_tables.sql

echo "8. submit flink sql jobs"
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

echo "9. generate traceable test journeys"
python3 scripts/generate_order_journeys.py --journeys 500 --interval 0.001 --time-span-minutes 10
python3 scripts/generate_order_transactions.py --orders 100

echo "10.1 run additive schema evolution drill"
bash scripts/schema_evolution_drill.sh

echo "10. load kafka result topics into clickhouse"
python3 scripts/load_kafka_to_clickhouse.py --group-id validation_loader --max-messages 0 --idle-timeout 20

echo "11. verify clickhouse results"
bash scripts/verify_result.sh
python3 scripts/quality_report.py --output artifacts/latest_quality_report.md

#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

CI_JOURNEYS="${CI_JOURNEYS:-300}"
CI_ORDERS="${CI_ORDERS:-40}"
RUN_TOMBSTONE_DRILL="${RUN_TOMBSTONE_DRILL:-true}"

wait_until() {
  local label="$1"
  local timeout_seconds="$2"
  shift 2
  local deadline=$((SECONDS + timeout_seconds))

  until "$@" >/dev/null 2>&1; do
    if (( SECONDS >= deadline )); then
      echo "Timed out after ${timeout_seconds}s waiting for ${label}." >&2
      return 1
    fi
    sleep 3
  done
  echo "ready: ${label}"
}

mysql_ready() {
  docker exec rtdw_mysql mysql -uroot -proot -e "SELECT 1"
}

kafka_ready() {
  docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --list
}

clickhouse_ready() {
  docker exec rtdw_clickhouse wget -qO- http://localhost:8123/ping
}

flink_api_ready() {
  curl -fsS http://localhost:8081/overview
}

loader_ready() {
  curl -fsS http://localhost:9410/healthz
}

flink_jobs_healthy() {
  python3 - <<'PY'
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
}

pipeline_rows_ready() {
  local counts
  counts="$(docker exec rtdw_clickhouse clickhouse-client --password clickhouse --query "
    SELECT count() FROM dwd_user_behavior FINAL
    UNION ALL SELECT count() FROM ads_realtime_overview FINAL
    UNION ALL SELECT count() FROM ads_product_rank FINAL
    UNION ALL SELECT count() FROM ads_category_rank FINAL
    UNION ALL SELECT count() FROM ads_channel_funnel FINAL
    UNION ALL SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0
    UNION ALL SELECT count() FROM dim_product_scd2 FINAL WHERE is_deleted = 0
    UNION ALL SELECT count() FROM ads_order_lifecycle FINAL WHERE is_deleted = 0
    UNION ALL SELECT count() FROM ads_order_daily FINAL WHERE is_deleted = 0
  ")" || return 1

  [[ "$(printf '%s\n' "$counts" | awk '$1 > 0 {positive++} END {print positive + 0}')" -eq 9 ]]
}

echo "1/10 Download Flink connectors (cache-friendly)."
bash scripts/download_connectors.sh

echo "2/10 Start only the resource-critical data plane."
docker compose down -v --remove-orphans
docker compose up -d --build \
  zookeeper kafka mysql clickhouse flink-volume-init flink-jobmanager flink-taskmanager

echo "3/10 Wait for infrastructure health."
wait_until "MySQL" 180 mysql_ready
wait_until "Kafka" 180 kafka_ready
wait_until "ClickHouse" 180 clickhouse_ready
wait_until "Flink REST API" 240 flink_api_ready

echo "4/10 Initialize source tables, Kafka topics and ClickHouse serving tables."
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim_expanded.sql
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_business_tables.sql
bash scripts/create_kafka_topics.sh
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery \
  < scripts/create_clickhouse_tables.sql

echo "5/10 Submit all Flink SQL pipelines and require exactly nine healthy jobs."
bash scripts/run_flink_sql.sh
wait_until "nine RUNNING Flink jobs" 300 flink_jobs_healthy

echo "6/10 Start the long-running Kafka-to-ClickHouse loader."
docker compose up -d --build clickhouse-loader
wait_until "ClickHouse loader" 180 loader_ready

echo "7/10 Generate a small deterministic behavior and order workload."
python3 scripts/generate_order_journeys.py \
  --journeys "$CI_JOURNEYS" \
  --time-span-minutes 45 \
  --interval 0
python3 scripts/generate_order_transactions.py --orders "$CI_ORDERS"

echo "8/10 Wait for all DWD/ADS outputs to reach ClickHouse."
wait_until "nine non-empty serving outputs" 300 pipeline_rows_ready

if [[ "${RUN_TOMBSTONE_DRILL,,}" == "true" ]]; then
  echo "9/10 Verify DELETE tombstone propagation and source restoration."
  python3 scripts/delete_tombstone_drill.py --timeout 120
else
  echo "9/10 Tombstone drill disabled by workflow input."
fi

echo "10/10 Run serving-layer assertions and the 24-rule quality gate."
bash scripts/verify_result.sh
mkdir -p artifacts/ci
python3 scripts/quality_report.py --output artifacts/ci/quality-report.md

echo "Integration smoke test passed: 9/9 Flink jobs and all data-quality rules are healthy."

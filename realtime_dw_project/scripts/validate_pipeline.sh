#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "1. check connector jars"
bash scripts/download_connectors.sh

echo "2. reset old demo containers and volumes"
docker compose down -v --remove-orphans

echo "3. start docker services"
docker compose up -d

echo "4. wait for mysql"
until docker exec rtdw_mysql mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "5. initialize mysql dimensions"
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim.sql

echo "6. create kafka topics"
bash scripts/create_kafka_topics.sh

echo "7. initialize clickhouse tables"
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/create_clickhouse_tables.sql

echo "8. submit flink sql jobs"
bash scripts/run_flink_sql.sh
MSYS_NO_PATHCONV=1 docker exec rtdw_flink_jobmanager /opt/flink/bin/flink list

echo "9. generate test events"
python3 scripts/generate_mock_events.py --count 200 --interval 0.01
python3 scripts/generate_mock_events.py --count 20 --interval 0.01 --event-time-offset 600

echo "10. load kafka result topics into clickhouse"
python3 scripts/load_kafka_to_clickhouse.py --group-id validation_loader --max-messages 260 --idle-timeout 20

echo "11. verify clickhouse results"
bash scripts/verify_result.sh
python3 scripts/quality_report.py

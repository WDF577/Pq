#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_DIR"

echo "== 电商实时数仓本地演示 =="
echo "说明：本脚本会重建本项目的 Docker 容器和数据卷，用于得到一份干净的演示结果。"
echo

need_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "缺少命令：$cmd，请先安装后再运行。" >&2
    exit 1
  fi
}

need_cmd docker
need_cmd python3

echo "1. 检查 Python 依赖"
python3 - <<'PY' >/dev/null 2>&1 || python3 -m pip install -r requirements.txt
import confluent_kafka
PY

echo "2. 检查 Flink 连接器"
bash scripts/download_connectors.sh

echo "3. 重建本地演示环境"
docker compose down -v --remove-orphans
docker compose up -d

echo "4. 等待 MySQL 可用"
until docker exec rtdw_mysql mysql -uroot -proot -e "SELECT 1" >/dev/null 2>&1; do
  sleep 2
done

echo "5. 初始化 MySQL 维表"
docker exec -i rtdw_mysql mysql -uroot -proot ecommerce < scripts/create_mysql_dim_expanded.sql

echo "6. 创建 Kafka Topic"
bash scripts/create_kafka_topics.sh

echo "7. 初始化 ClickHouse 表"
docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/create_clickhouse_tables.sql

echo "8. 提交 Flink SQL 任务"
bash scripts/run_flink_sql.sh
docker exec rtdw_flink_jobmanager //opt/flink/bin/flink list

echo "9. 生成测试数据（10万条，覆盖过去2小时，含脏数据）"
python3 scripts/generate_mock_events.py --count 100000 --interval 0 --time-span-minutes 120

echo "10. 等待 Flink 处理完成（30秒）"
sleep 30

echo "11. 装载 Flink 输出结果到 ClickHouse"
python3 scripts/load_kafka_to_clickhouse.py --group-id demo_loader --max-messages 0 --idle-timeout 30

echo "12. 查询并验收结果"
bash scripts/verify_result.sh

echo
echo "演示完成。可以打开 Flink 页面查看任务：http://localhost:8081"
echo "停止环境：docker compose down"

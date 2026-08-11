#!/usr/bin/env bash
set -euo pipefail

submit_flink_sql() {
  local label="$1"
  shift
  local output
  local native_exit=0
  output="$(MSYS_NO_PATHCONV=1 cat "$@" \
    | MSYS_NO_PATHCONV=1 docker exec -i rtdw_flink_jobmanager /opt/flink/bin/sql-client.sh 2>&1)" \
    || native_exit=$?
  printf '%s\n' "$output"
  # SQL Client can print [ERROR] while still returning process exit code 0.
  if [[ "$native_exit" -ne 0 ]] || grep -q '^\[ERROR\]' <<<"$output"; then
    echo "$label failed (native exit code $native_exit)." >&2
    return 1
  fi
}

submit_flink_sql "Behavior pipeline" \
  flink-sql/01_create_source_tables.sql \
  flink-sql/02_create_dwd_tables.sql \
  flink-sql/03_create_dws_ads_tables.sql
submit_flink_sql "MySQL CDC pipeline" flink-sql/04_mysql_cdc_to_ods.sql
submit_flink_sql "Order DWD pipeline" flink-sql/05_order_dwd.sql
submit_flink_sql "Order ADS pipeline" flink-sql/06_order_ads.sql

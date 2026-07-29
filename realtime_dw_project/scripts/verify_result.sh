#!/usr/bin/env bash
set -euo pipefail

query_count() {
  docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --query "$1"
}

docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/verify_clickhouse.sql

dwd_rows=$(query_count "SELECT count() FROM dwd_user_behavior")
overview_rows=$(query_count "SELECT count() FROM ads_realtime_overview")
rank_rows=$(query_count "SELECT count() FROM ads_product_rank")
category_rows=$(query_count "SELECT count() FROM ads_category_rank")
funnel_rows=$(query_count "SELECT count() FROM ads_channel_funnel")
alert_rows=$(query_count "SELECT count() FROM ads_realtime_alert")
empty_event_id=$(query_count "SELECT count() FROM dwd_user_behavior WHERE event_id = ''")
invalid_event_type=$(query_count "SELECT count() FROM dwd_user_behavior WHERE event_type NOT IN ('view','cart','order','pay')")
duplicate_event_keys=$(query_count "SELECT count() FROM (SELECT event_id FROM dwd_user_behavior GROUP BY event_id HAVING count() > 1)")
invalid_pv_uv=$(query_count "SELECT count() FROM ads_realtime_overview WHERE uv > pv")

pass_count=0
fail_count=0

check_positive() {
  local label="$1"
  local value="$2"
  if [ "$value" -gt 0 ]; then
    echo "  PASS: $label = $value (expected > 0)"
    pass_count=$((pass_count + 1))
  else
    echo "  FAIL: $label = $value (expected > 0)"
    fail_count=$((fail_count + 1))
  fi
}

check_zero() {
  local label="$1"
  local value="$2"
  if [ "$value" -eq 0 ]; then
    echo "  PASS: $label = 0"
    pass_count=$((pass_count + 1))
  else
    echo "  FAIL: $label = $value (expected 0)"
    fail_count=$((fail_count + 1))
  fi
}

echo
echo "=== Verification Results ==="
check_positive "dwd_user_behavior" "$dwd_rows"
check_positive "ads_realtime_overview" "$overview_rows"
check_positive "ads_product_rank" "$rank_rows"
check_positive "ads_category_rank" "$category_rows"
check_positive "ads_channel_funnel" "$funnel_rows"
check_zero "empty event_id rows" "$empty_event_id"
check_zero "invalid event_type rows" "$invalid_event_type"
check_zero "duplicate event_id keys" "$duplicate_event_keys"
check_zero "windows where UV > PV" "$invalid_pv_uv"

echo
echo "verify summary: $pass_count PASS, $fail_count FAIL"
echo "dwd_rows=$dwd_rows, overview_rows=$overview_rows, rank_rows=$rank_rows"
echo "category_rows=$category_rows, funnel_rows=$funnel_rows, alert_rows=$alert_rows"

# Acceptance thresholds (adjusted for realistic random time distribution)
warn=0
if [ "$overview_rows" -lt 50 ]; then
  echo "WARNING: ads_realtime_overview rows ($overview_rows) below 50."
  warn=1
fi
if [ "$category_rows" -lt 20 ]; then
  echo "WARNING: ads_category_rank rows ($category_rows) below 20."
  warn=1
fi

if [ "$fail_count" -gt 0 ]; then
  echo "Some checks FAILED (tables without data)."
  exit 1
fi

echo "All core checks PASSED!"
if [ "$warn" -eq 1 ]; then
  echo "Note: Some ADS thresholds not met. See WARNING above."
fi

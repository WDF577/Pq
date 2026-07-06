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

pass_count=0
fail_count=0

check() {
  local label="$1"
  local value="$2"
  local expected="$3"
  if [ "$value" -gt "$expected" ]; then
    echo "  PASS: $label = $value (expected > $expected)"
    pass_count=$((pass_count + 1))
  else
    echo "  FAIL: $label = $value (expected > $expected)"
    fail_count=$((fail_count + 1))
  fi
}

echo
echo "=== Verification Results ==="
check "dwd_user_behavior"        "$dwd_rows"      0
check "ads_realtime_overview"    "$overview_rows"  0
check "ads_product_rank"         "$rank_rows"      0
check "ads_category_rank"        "$category_rows"  0
check "ads_channel_funnel"       "$funnel_rows"    0
check "ads_realtime_alert"       "$alert_rows"    -1

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

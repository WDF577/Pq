#!/usr/bin/env bash
set -euo pipefail

query_count() {
  docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --query "$1"
}

docker exec -i rtdw_clickhouse clickhouse-client --password clickhouse --multiquery < scripts/verify_clickhouse.sql

dwd_rows=$(query_count "SELECT count() FROM dwd_user_behavior FINAL")
dirty_rows=$(query_count "SELECT count() FROM dwd_dirty_behavior FINAL")
overview_rows=$(query_count "SELECT count() FROM ads_realtime_overview FINAL")
rank_rows=$(query_count "SELECT count() FROM ads_product_rank FINAL")
category_rows=$(query_count "SELECT count() FROM ads_category_rank FINAL")
funnel_rows=$(query_count "SELECT count() FROM ads_channel_funnel FINAL")
alert_rows=$(query_count "SELECT count() FROM ads_realtime_alert")
empty_event_id=$(query_count "SELECT count() FROM dwd_user_behavior FINAL WHERE event_id = ''")
invalid_event_type=$(query_count "SELECT count() FROM dwd_user_behavior FINAL WHERE event_type NOT IN ('view','cart','order','pay')")
duplicate_event_keys=$(query_count "SELECT count() FROM (SELECT event_id FROM dwd_user_behavior FINAL GROUP BY event_id HAVING count() > 1)")
invalid_pv_uv=$(query_count "SELECT count() FROM ads_realtime_overview FINAL WHERE uv > pv")
invalid_funnel_order=$(query_count "SELECT count() FROM ads_channel_funnel FINAL WHERE cart_users > view_users OR order_users > cart_users OR pay_users > order_users")
order_detail_rows=$(query_count "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0")
scd2_rows=$(query_count "SELECT count() FROM dim_product_scd2 FINAL WHERE is_deleted = 0")
order_lifecycle_rows=$(query_count "SELECT count() FROM ads_order_lifecycle FINAL WHERE is_deleted = 0")
order_daily_rows=$(query_count "SELECT count() FROM ads_order_daily FINAL WHERE is_deleted = 0")
duplicate_detail_keys=$(query_count "SELECT count() FROM (SELECT detail_id FROM dwd_order_detail FINAL WHERE is_deleted = 0 GROUP BY detail_id HAVING count() > 1)")
invalid_order_status=$(query_count "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status NOT IN ('CREATED','PAID','CANCELLED','REFUNDED')")
paid_without_payment=$(query_count "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status IN ('PAID','REFUNDED') AND (payment_id IS NULL OR payment_status != 'SUCCESS')")
invalid_refund=$(query_count "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND order_status = 'REFUNDED' AND (refund_id IS NULL OR refund_status != 'SUCCESS' OR refund_amount <= 0)")
historical_price_mismatch=$(query_count "SELECT count() FROM dwd_order_detail FINAL WHERE is_deleted = 0 AND unit_price != catalog_price")
invalid_scd2_current=$(query_count "SELECT count() FROM (SELECT product_id FROM dim_product_scd2 FINAL WHERE is_deleted = 0 GROUP BY product_id HAVING countIf(is_current = 1) != 1)")
invalid_order_lifecycle=$(query_count "SELECT count() FROM ads_order_lifecycle FINAL WHERE is_deleted = 0 AND (paid_orders > total_orders OR cancelled_orders > total_orders OR refunded_orders > paid_orders OR refund_amount > paid_amount)")

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
check_positive "dwd_order_detail" "$order_detail_rows"
check_positive "dim_product_scd2" "$scd2_rows"
check_positive "ads_order_lifecycle" "$order_lifecycle_rows"
check_positive "ads_order_daily" "$order_daily_rows"
check_zero "empty event_id rows" "$empty_event_id"
check_zero "invalid event_type rows" "$invalid_event_type"
check_zero "duplicate event_id keys" "$duplicate_event_keys"
check_zero "windows where UV > PV" "$invalid_pv_uv"
check_zero "funnel windows with reversed stage counts" "$invalid_funnel_order"
check_zero "duplicate detail keys" "$duplicate_detail_keys"
check_zero "invalid order status" "$invalid_order_status"
check_zero "paid orders without payment" "$paid_without_payment"
check_zero "invalid refunds" "$invalid_refund"
check_zero "historical price mismatches" "$historical_price_mismatch"
check_zero "invalid SCD2 current versions" "$invalid_scd2_current"
check_zero "invalid order lifecycle metrics" "$invalid_order_lifecycle"

echo
echo "verify summary: $pass_count PASS, $fail_count FAIL"
echo "dwd_rows=$dwd_rows, overview_rows=$overview_rows, rank_rows=$rank_rows"
echo "category_rows=$category_rows, funnel_rows=$funnel_rows, alert_rows=$alert_rows"
echo "dirty_rows=$dirty_rows"
echo "order_detail_rows=$order_detail_rows, scd2_rows=$scd2_rows"
echo "order_lifecycle_rows=$order_lifecycle_rows, order_daily_rows=$order_daily_rows"

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

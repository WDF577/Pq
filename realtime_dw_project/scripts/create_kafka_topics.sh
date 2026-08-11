#!/usr/bin/env bash
set -euo pipefail

create_topic() {
  local topic="$1"
  local cleanup_policy="${2:-delete}"
  docker exec rtdw_kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "${KAFKA_PARTITIONS:-3}" \
    --replication-factor 1 \
    --config "cleanup.policy=$cleanup_policy"
  # --if-not-exists does not update an existing topic, so reconcile the
  # policy explicitly as well as setting it on first creation.
  docker exec rtdw_kafka kafka-configs \
    --bootstrap-server kafka:29092 \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "cleanup.policy=$cleanup_policy"
}

create_topic ods_user_behavior
create_topic dwd_dirty_behavior
create_topic dwd_user_behavior "compact,delete"
create_topic ads_realtime_overview "compact,delete"
create_topic ads_product_rank "compact,delete"
create_topic ads_channel_funnel "compact,delete"
create_topic ads_category_rank "compact,delete"
create_topic ods_order_info "compact,delete"
create_topic ods_order_detail "compact,delete"
create_topic ods_payment_info "compact,delete"
create_topic ods_refund_info "compact,delete"
create_topic ods_dim_product_scd2 "compact,delete"
create_topic dwd_order_detail "compact,delete"
create_topic ads_order_lifecycle "compact,delete"
create_topic ads_order_daily "compact,delete"
# The deterministic message_id is the key. Pure compaction keeps the latest
# pending/replayed state without time-based deletion silently expiring an
# unresolved poison message. Capacity still needs operational monitoring.
create_topic clickhouse_loader_dlq "compact"

docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --list

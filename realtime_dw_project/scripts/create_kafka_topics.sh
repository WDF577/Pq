#!/usr/bin/env bash
set -euo pipefail

create_topic() {
  local topic="$1"
  docker exec rtdw_kafka kafka-topics \
    --bootstrap-server kafka:29092 \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1
}

create_topic ods_user_behavior
create_topic dwd_user_behavior
create_topic ads_realtime_overview
create_topic ads_product_rank
create_topic ads_channel_funnel
create_topic ads_category_rank

docker exec rtdw_kafka kafka-topics --bootstrap-server kafka:29092 --list

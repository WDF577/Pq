CREATE DATABASE IF NOT EXISTS default;

DROP TABLE IF EXISTS dwd_user_behavior;
DROP TABLE IF EXISTS dwd_dirty_behavior;
DROP TABLE IF EXISTS ads_realtime_overview;
DROP TABLE IF EXISTS ads_product_rank;
DROP TABLE IF EXISTS ads_category_rank;
DROP TABLE IF EXISTS ads_channel_funnel;
DROP TABLE IF EXISTS ads_realtime_alert;
DROP TABLE IF EXISTS dwd_order_detail;
DROP TABLE IF EXISTS dim_product_scd2;
DROP TABLE IF EXISTS ads_order_lifecycle;
DROP TABLE IF EXISTS ads_order_daily;

CREATE TABLE dwd_user_behavior (
  event_id String,
  session_id String,
  order_id Nullable(String),
  event_sequence Int32,
  event_version UInt64,
  user_id Int64,
  product_id Int64,
  product_name Nullable(String),
  category_id Nullable(Int64),
  category_name Nullable(String),
  shop_id Int64,
  shop_name Nullable(String),
  event_type LowCardinality(String),
  channel LowCardinality(String),
  amount Decimal(10, 2),
  province Nullable(String),
  city Nullable(String),
  event_ts DateTime,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY event_id;

CREATE TABLE dwd_dirty_behavior (
  event_id Nullable(String),
  session_id Nullable(String),
  order_id Nullable(String),
  event_sequence Nullable(Int32),
  event_version Nullable(UInt64),
  user_id Nullable(Int64),
  product_id Nullable(Int64),
  shop_id Nullable(Int64),
  event_type Nullable(String),
  channel Nullable(String),
  amount Nullable(Decimal(10, 2)),
  event_time Nullable(String),
  error_reason String,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (source_topic, source_partition, source_offset);

CREATE TABLE ads_realtime_overview (
  window_start DateTime,
  window_end DateTime,
  pv Int64,
  uv Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2),
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (window_start, window_end);

CREATE TABLE ads_product_rank (
  window_start DateTime,
  window_end DateTime,
  product_id Int64,
  product_name Nullable(String),
  category_name Nullable(String),
  pay_count Int64,
  pay_amount Decimal(18, 2),
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (window_start, window_end, product_id);

CREATE TABLE ads_category_rank (
  window_start DateTime,
  window_end DateTime,
  category_name String,
  pay_count Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2),
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (window_start, window_end, category_name);

CREATE TABLE ads_channel_funnel (
  window_start DateTime,
  window_end DateTime,
  channel LowCardinality(String),
  view_users Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (window_start, window_end, channel);

CREATE TABLE ads_realtime_alert (
  window_start DateTime,
  window_end DateTime,
  alert_type String,
  alert_level LowCardinality(String),
  alert_message String,
  metric_name String,
  metric_value Float64,
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (created_at, alert_type);

CREATE TABLE dim_product_scd2 (
  product_id Int64,
  version_no Int32,
  product_name String,
  category_id Int64,
  category_name String,
  price Decimal(10, 2),
  effective_from DateTime64(3),
  effective_to DateTime64(3),
  is_current Int8,
  updated_at DateTime64(3),
  is_deleted UInt8 DEFAULT 0,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (product_id, version_no);

CREATE TABLE dwd_order_detail (
  detail_id String,
  order_id Nullable(String),
  user_id Nullable(Int64),
  shop_id Nullable(Int64),
  product_id Nullable(Int64),
  product_name Nullable(String),
  category_id Nullable(Int64),
  category_name Nullable(String),
  product_version Nullable(Int32),
  catalog_price Nullable(Decimal(10, 2)),
  quantity Nullable(Int32),
  unit_price Nullable(Decimal(10, 2)),
  detail_amount Nullable(Decimal(12, 2)),
  order_status Nullable(String),
  order_amount Nullable(Decimal(12, 2)),
  channel Nullable(String),
  promotion_code Nullable(String),
  payment_id Nullable(String),
  payment_status Nullable(String),
  payment_method Nullable(String),
  payment_amount Nullable(Decimal(12, 2)),
  payment_time Nullable(DateTime64(3)),
  refund_id Nullable(String),
  refund_status Nullable(String),
  refund_amount Nullable(Decimal(12, 2)),
  refund_reason Nullable(String),
  refund_time Nullable(DateTime64(3)),
  order_create_time Nullable(DateTime64(3)),
  order_update_time Nullable(DateTime64(3)),
  detail_update_time Nullable(DateTime64(3)),
  order_version Nullable(Int64),
  detail_version Nullable(Int64),
  payment_version Nullable(Int64),
  refund_version Nullable(Int64),
  is_deleted UInt8 DEFAULT 0,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY detail_id;

CREATE TABLE ads_order_lifecycle (
  channel String,
  total_orders Nullable(Int64),
  paid_orders Nullable(Int64),
  cancelled_orders Nullable(Int64),
  refunded_orders Nullable(Int64),
  order_amount Nullable(Decimal(22, 2)),
  paid_amount Nullable(Decimal(22, 2)),
  refund_amount Nullable(Decimal(22, 2)),
  is_deleted UInt8 DEFAULT 0,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY channel;

CREATE TABLE ads_order_daily (
  order_date Date,
  channel String,
  total_orders Nullable(Int64),
  paid_orders Nullable(Int64),
  cancelled_orders Nullable(Int64),
  refunded_orders Nullable(Int64),
  order_amount Nullable(Decimal(22, 2)),
  paid_amount Nullable(Decimal(22, 2)),
  refund_amount Nullable(Decimal(22, 2)),
  is_deleted UInt8 DEFAULT 0,
  source_topic LowCardinality(String),
  source_partition Int32,
  source_offset Int64,
  ingest_version UInt64
) ENGINE = ReplacingMergeTree(ingest_version)
ORDER BY (order_date, channel);

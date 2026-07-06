CREATE DATABASE IF NOT EXISTS default;

DROP TABLE IF EXISTS dwd_user_behavior;
DROP TABLE IF EXISTS ads_realtime_overview;
DROP TABLE IF EXISTS ads_product_rank;
DROP TABLE IF EXISTS ads_category_rank;
DROP TABLE IF EXISTS ads_channel_funnel;
DROP TABLE IF EXISTS ads_realtime_alert;

CREATE TABLE dwd_user_behavior (
  event_id String,
  user_id Int64,
  product_id Int64,
  product_name String,
  category_id Int64,
  category_name String,
  shop_id Int64,
  shop_name String,
  event_type String,
  channel String,
  amount Decimal(10, 2),
  province String,
  city String,
  event_ts DateTime
) ENGINE = MergeTree
ORDER BY (event_ts, event_type, product_id);

CREATE TABLE ads_realtime_overview (
  window_start DateTime,
  window_end DateTime,
  pv Int64,
  uv Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2)
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end);

CREATE TABLE ads_product_rank (
  window_start DateTime,
  window_end DateTime,
  product_id Int64,
  product_name String,
  category_name String,
  pay_count Int64,
  pay_amount Decimal(18, 2)
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end, product_id);

CREATE TABLE ads_category_rank (
  window_start DateTime,
  window_end DateTime,
  category_name String,
  pay_count Int64,
  pay_users Int64,
  pay_amount Decimal(18, 2)
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end, category_name);

CREATE TABLE ads_channel_funnel (
  window_start DateTime,
  window_end DateTime,
  channel String,
  view_users Int64,
  cart_users Int64,
  order_users Int64,
  pay_users Int64
) ENGINE = ReplacingMergeTree
ORDER BY (window_start, window_end, channel);

CREATE TABLE ads_realtime_alert (
  window_start DateTime,
  window_end DateTime,
  alert_type String,
  alert_level String,
  alert_message String,
  metric_name String,
  metric_value Float64,
  created_at DateTime DEFAULT now()
) ENGINE = MergeTree
ORDER BY (created_at, alert_type);

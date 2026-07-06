CREATE TABLE ads_realtime_overview_kafka (
  window_start STRING,
  window_end STRING,
  pv BIGINT,
  uv BIGINT,
  cart_users BIGINT,
  order_users BIGINT,
  pay_users BIGINT,
  pay_amount DECIMAL(18,2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_realtime_overview',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE ads_product_rank_kafka (
  window_start STRING,
  window_end STRING,
  product_id BIGINT,
  product_name STRING,
  category_name STRING,
  pay_count BIGINT,
  pay_amount DECIMAL(18,2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_product_rank',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE ads_category_rank_kafka (
  window_start STRING,
  window_end STRING,
  category_name STRING,
  pay_count BIGINT,
  pay_users BIGINT,
  pay_amount DECIMAL(18,2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_category_rank',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE ads_channel_funnel_kafka (
  window_start STRING,
  window_end STRING,
  channel STRING,
  view_users BIGINT,
  cart_users BIGINT,
  order_users BIGINT,
  pay_users BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'ads_channel_funnel',
  'properties.bootstrap.servers' = 'kafka:29092',
  'format' = 'json'
);

INSERT INTO ads_realtime_overview_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  COUNT(*) AS pv,
  COUNT(DISTINCT user_id) AS uv,
  COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_users,
  COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_users,
  COUNT(DISTINCT CASE WHEN event_type = 'pay' THEN user_id END) AS pay_users,
  SUM(CASE WHEN event_type = 'pay' THEN amount ELSE CAST(0 AS DECIMAL(10,2)) END) AS pay_amount
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;

INSERT INTO ads_product_rank_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  product_id,
  product_name,
  category_name,
  COUNT(*) AS pay_count,
  SUM(amount) AS pay_amount
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
)
WHERE event_type = 'pay'
GROUP BY window_start, window_end, product_id, product_name, category_name;

INSERT INTO ads_category_rank_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  category_name,
  COUNT(*) AS pay_count,
  COUNT(DISTINCT user_id) AS pay_users,
  SUM(amount) AS pay_amount
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
)
WHERE event_type = 'pay'
GROUP BY window_start, window_end, category_name;

INSERT INTO ads_channel_funnel_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  channel,
  COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS view_users,
  COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_users,
  COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_users,
  COUNT(DISTINCT CASE WHEN event_type = 'pay' THEN user_id END) AS pay_users
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end, channel;

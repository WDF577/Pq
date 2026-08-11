CREATE TABLE ads_realtime_overview_kafka (
  window_start STRING,
  window_end STRING,
  pv BIGINT,
  uv BIGINT,
  cart_users BIGINT,
  order_users BIGINT,
  pay_users BIGINT,
  pay_amount DECIMAL(18,2),
  PRIMARY KEY (window_start, window_end) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_realtime_overview',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-overview-'
);

CREATE TABLE ads_product_rank_kafka (
  window_start STRING,
  window_end STRING,
  product_id BIGINT,
  product_name STRING,
  category_name STRING,
  pay_count BIGINT,
  pay_amount DECIMAL(18,2),
  PRIMARY KEY (window_start, window_end, product_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_product_rank',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-product-'
);

CREATE TABLE ads_category_rank_kafka (
  window_start STRING,
  window_end STRING,
  category_name STRING,
  pay_count BIGINT,
  pay_users BIGINT,
  pay_amount DECIMAL(18,2),
  PRIMARY KEY (window_start, window_end, category_name) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_category_rank',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-category-'
);

CREATE TABLE ads_channel_funnel_kafka (
  window_start STRING,
  window_end STRING,
  channel STRING,
  view_users BIGINT,
  cart_users BIGINT,
  order_users BIGINT,
  pay_users BIGINT,
  PRIMARY KEY (window_start, window_end, channel) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_channel_funnel',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-funnel-'
);

INSERT INTO ads_realtime_overview_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS pv,
  COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS uv,
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
  COALESCE(category_name, 'UNKNOWN') AS category_name,
  COUNT(*) AS pay_count,
  COUNT(DISTINCT user_id) AS pay_users,
  SUM(amount) AS pay_amount
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '5' MINUTE)
)
WHERE event_type = 'pay'
GROUP BY window_start, window_end, COALESCE(category_name, 'UNKNOWN');

CREATE TEMPORARY VIEW dws_session_stage_view AS
SELECT
  window_start,
  window_end,
  channel,
  session_id,
  MIN(CASE WHEN event_type = 'view' THEN event_ts END) AS view_ts,
  MIN(CASE WHEN event_type = 'cart' THEN event_ts END) AS cart_ts,
  MIN(CASE WHEN event_type = 'order' THEN event_ts END) AS order_ts,
  MIN(CASE WHEN event_type = 'pay' THEN event_ts END) AS pay_ts
FROM TABLE(
  TUMBLE(TABLE dwd_user_behavior_view, DESCRIPTOR(event_ts), INTERVAL '30' MINUTE)
)
GROUP BY window_start, window_end, channel, session_id;

INSERT INTO ads_channel_funnel_kafka
SELECT
  CAST(window_start AS STRING) AS window_start,
  CAST(window_end AS STRING) AS window_end,
  channel,
  SUM(CASE WHEN view_ts IS NOT NULL THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS view_users,
  SUM(CASE WHEN view_ts IS NOT NULL AND cart_ts >= view_ts THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS cart_users,
  SUM(CASE WHEN view_ts IS NOT NULL AND cart_ts >= view_ts AND order_ts >= cart_ts THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS order_users,
  SUM(CASE WHEN view_ts IS NOT NULL AND cart_ts >= view_ts AND order_ts >= cart_ts AND pay_ts >= order_ts THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS pay_users
FROM dws_session_stage_view
GROUP BY window_start, window_end, channel;

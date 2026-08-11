CREATE TEMPORARY VIEW dirty_user_behavior_view AS
SELECT
  event_id,
  session_id,
  order_id,
  event_sequence,
  event_version,
  user_id,
  product_id,
  shop_id,
  event_type,
  channel,
  amount,
  event_time,
  CONCAT_WS(';',
    CASE WHEN event_id IS NULL OR CHAR_LENGTH(TRIM(event_id)) = 0 THEN 'EMPTY_EVENT_ID' END,
    CASE WHEN session_id IS NULL OR CHAR_LENGTH(TRIM(session_id)) = 0 THEN 'EMPTY_SESSION_ID' END,
    CASE WHEN user_id IS NULL THEN 'EMPTY_USER_ID' END,
    CASE WHEN event_type NOT IN ('view', 'cart', 'order', 'pay') THEN 'INVALID_EVENT_TYPE' END,
    CASE WHEN event_ts IS NULL THEN 'INVALID_EVENT_TIME' END
  ) AS error_reason
FROM ods_user_behavior
WHERE event_id IS NULL
   OR CHAR_LENGTH(TRIM(event_id)) = 0
   OR session_id IS NULL
   OR CHAR_LENGTH(TRIM(session_id)) = 0
   OR user_id IS NULL
   OR event_type NOT IN ('view', 'cart', 'order', 'pay')
   OR event_ts IS NULL;

CREATE TABLE dwd_dirty_behavior_kafka (
  event_id STRING,
  session_id STRING,
  order_id STRING,
  event_sequence INT,
  event_version BIGINT,
  user_id BIGINT,
  product_id BIGINT,
  shop_id BIGINT,
  event_type STRING,
  channel STRING,
  amount DECIMAL(10,2),
  event_time STRING,
  error_reason STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_dirty_behavior',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'key.fields' = 'event_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-dirty-'
);

INSERT INTO dwd_dirty_behavior_kafka
SELECT * FROM dirty_user_behavior_view;

CREATE TEMPORARY VIEW dwd_user_behavior_view AS
SELECT
  o.event_id,
  o.session_id,
  o.order_id,
  o.event_sequence,
  o.event_version,
  o.user_id,
  o.product_id,
  p.product_name,
  p.category_id,
  p.category_name,
  o.shop_id,
  s.shop_name,
  o.event_type,
  o.channel,
  o.amount,
  o.event_ts,
  r.province,
  r.city
FROM ods_user_behavior AS o
LEFT JOIN dim_product FOR SYSTEM_TIME AS OF o.proc_time AS p
ON o.product_id = p.product_id
LEFT JOIN dim_shop FOR SYSTEM_TIME AS OF o.proc_time AS s
ON o.shop_id = s.shop_id
LEFT JOIN dim_region FOR SYSTEM_TIME AS OF o.proc_time AS r
ON s.region_id = r.region_id
WHERE o.event_id IS NOT NULL
  AND CHAR_LENGTH(TRIM(o.event_id)) > 0
  AND o.session_id IS NOT NULL
  AND CHAR_LENGTH(TRIM(o.session_id)) > 0
  AND o.user_id IS NOT NULL
  AND o.event_type IN ('view', 'cart', 'order', 'pay')
  AND o.event_ts IS NOT NULL;

CREATE TABLE dwd_user_behavior_kafka (
  event_id STRING,
  session_id STRING,
  order_id STRING,
  event_sequence INT,
  event_version BIGINT,
  user_id BIGINT,
  product_id BIGINT,
  product_name STRING,
  category_id BIGINT,
  category_name STRING,
  shop_id BIGINT,
  shop_name STRING,
  event_type STRING,
  channel STRING,
  amount DECIMAL(10,2),
  event_ts STRING,
  province STRING,
  city STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_behavior',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'key.fields' = 'event_id',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-dwd-'
);

INSERT INTO dwd_user_behavior_kafka
SELECT
  event_id,
  session_id,
  order_id,
  event_sequence,
  event_version,
  user_id,
  product_id,
  product_name,
  category_id,
  category_name,
  shop_id,
  shop_name,
  event_type,
  channel,
  amount,
  CAST(event_ts AS STRING) AS event_ts,
  province,
  city
FROM dwd_user_behavior_view;

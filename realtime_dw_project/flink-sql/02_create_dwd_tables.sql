CREATE TEMPORARY VIEW dwd_user_behavior_view AS
SELECT
  o.event_id,
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
  AND o.user_id IS NOT NULL
  AND o.event_type IN ('view', 'cart', 'order', 'pay');

CREATE TABLE dwd_user_behavior_kafka (
  event_id STRING,
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
  'format' = 'json',
  'json.timestamp-format.standard' = 'SQL'
);

INSERT INTO dwd_user_behavior_kafka
SELECT
  event_id,
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

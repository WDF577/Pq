CREATE TABLE ods_user_behavior (
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
  event_ts AS TO_TIMESTAMP(event_time),
  proc_time AS PROCTIME(),
  WATERMARK FOR event_ts AS event_ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_user_behavior',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw_flink',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.fail-on-missing-field' = 'false',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE dim_product (
  product_id BIGINT,
  product_name STRING,
  category_id BIGINT,
  category_name STRING,
  price DECIMAL(10,2),
  PRIMARY KEY (product_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai',
  'table-name' = 'dim_product',
  'username' = 'flink_lookup',
  'password' = 'flink_lookup',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min',
  'lookup.max-retries' = '3'
);

CREATE TABLE dim_shop (
  shop_id BIGINT,
  shop_name STRING,
  region_id BIGINT,
  PRIMARY KEY (shop_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai',
  'table-name' = 'dim_shop',
  'username' = 'flink_lookup',
  'password' = 'flink_lookup',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min',
  'lookup.max-retries' = '3'
);

CREATE TABLE dim_region (
  region_id BIGINT,
  province STRING,
  city STRING,
  PRIMARY KEY (region_id) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysql:3306/ecommerce?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai',
  'table-name' = 'dim_region',
  'username' = 'flink_lookup',
  'password' = 'flink_lookup',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '1000',
  'lookup.partial-cache.expire-after-write' = '10 min',
  'lookup.max-retries' = '3'
);

-- Stateful changelog joins build the current order detail wide table. Thirty
-- days covers this project's refund observation window; production must align
-- the value with the contractual maximum refund delay and state capacity.
SET 'table.exec.state.ttl' = '30 d';

CREATE TABLE ods_order_info_changelog (
  order_id STRING,
  user_id BIGINT,
  shop_id BIGINT,
  order_status STRING,
  channel STRING,
  order_amount DECIMAL(12,2),
  promotion_code STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  version_no BIGINT,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ods_order_info',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw-order-dwd-order',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE ods_order_detail_changelog (
  detail_id STRING,
  order_id STRING,
  product_id BIGINT,
  quantity INT,
  unit_price DECIMAL(10,2),
  detail_amount DECIMAL(12,2),
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  version_no BIGINT,
  PRIMARY KEY (detail_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ods_order_detail',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw-order-dwd-detail',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE ods_payment_info_changelog (
  payment_id STRING,
  order_id STRING,
  payment_status STRING,
  payment_method STRING,
  payment_amount DECIMAL(12,2),
  payment_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  version_no BIGINT,
  PRIMARY KEY (payment_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ods_payment_info',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw-order-dwd-payment',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE ods_refund_info_changelog (
  refund_id STRING,
  order_id STRING,
  payment_id STRING,
  refund_status STRING,
  refund_amount DECIMAL(12,2),
  refund_reason STRING,
  refund_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  version_no BIGINT,
  PRIMARY KEY (refund_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ods_refund_info',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw-order-dwd-refund',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE ods_dim_product_scd2_changelog (
  product_id BIGINT,
  version_no INT,
  product_name STRING,
  category_id BIGINT,
  category_name STRING,
  price DECIMAL(10,2),
  effective_from TIMESTAMP(3),
  effective_to TIMESTAMP(3),
  is_current TINYINT,
  updated_at TIMESTAMP(3),
  PRIMARY KEY (product_id, version_no) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ods_dim_product_scd2',
  'properties.bootstrap.servers' = 'kafka:29092',
  'properties.group.id' = 'rtdw-order-dwd-product-scd2',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

CREATE TABLE dwd_order_detail_kafka (
  detail_id STRING,
  order_id STRING,
  user_id BIGINT,
  shop_id BIGINT,
  product_id BIGINT,
  product_name STRING,
  category_id BIGINT,
  category_name STRING,
  product_version INT,
  catalog_price DECIMAL(10,2),
  quantity INT,
  unit_price DECIMAL(10,2),
  detail_amount DECIMAL(12,2),
  order_status STRING,
  order_amount DECIMAL(12,2),
  channel STRING,
  promotion_code STRING,
  payment_id STRING,
  payment_status STRING,
  payment_method STRING,
  payment_amount DECIMAL(12,2),
  payment_time TIMESTAMP(3),
  refund_id STRING,
  refund_status STRING,
  refund_amount DECIMAL(12,2),
  refund_reason STRING,
  refund_time TIMESTAMP(3),
  order_create_time TIMESTAMP(3),
  order_update_time TIMESTAMP(3),
  detail_update_time TIMESTAMP(3),
  order_version BIGINT,
  detail_version BIGINT,
  payment_version BIGINT,
  refund_version BIGINT,
  PRIMARY KEY (detail_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'dwd_order_detail',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-dwd-order-detail-'
);

INSERT INTO dwd_order_detail_kafka
SELECT
  d.detail_id,
  d.order_id,
  o.user_id,
  o.shop_id,
  d.product_id,
  s.product_name,
  s.category_id,
  s.category_name,
  s.version_no,
  s.price,
  d.quantity,
  d.unit_price,
  d.detail_amount,
  o.order_status,
  o.order_amount,
  o.channel,
  o.promotion_code,
  p.payment_id,
  p.payment_status,
  p.payment_method,
  p.payment_amount,
  p.payment_time,
  r.refund_id,
  r.refund_status,
  r.refund_amount,
  r.refund_reason,
  r.refund_time,
  o.create_time,
  o.update_time,
  d.update_time,
  o.version_no,
  d.version_no,
  p.version_no,
  r.version_no
FROM ods_order_detail_changelog AS d
JOIN ods_order_info_changelog AS o
  ON d.order_id = o.order_id
JOIN ods_dim_product_scd2_changelog AS s
  ON d.product_id = s.product_id
 AND d.create_time >= s.effective_from
 AND d.create_time < s.effective_to
LEFT JOIN ods_payment_info_changelog AS p
  ON d.order_id = p.order_id
LEFT JOIN ods_refund_info_changelog AS r
  ON d.order_id = r.order_id;

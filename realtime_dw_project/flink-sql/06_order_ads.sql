-- Keep current order-level aggregation state for the same 30-day refund window
-- used by the upstream changelog join.
SET 'table.exec.state.ttl' = '30 d';

CREATE TABLE dwd_order_detail_changelog (
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
  'properties.group.id' = 'rtdw-order-ads',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL'
);

-- Collapse multi-line orders before calculating order-level metrics so order
-- amount and payment amount are never multiplied by the number of details.
CREATE TEMPORARY VIEW dws_order_current AS
SELECT
  order_id,
  MAX(channel) AS channel,
  MAX(order_status) AS order_status,
  MAX(order_amount) AS order_amount,
  MAX(COALESCE(payment_amount, CAST(0 AS DECIMAL(12,2)))) AS payment_amount,
  MAX(COALESCE(refund_amount, CAST(0 AS DECIMAL(12,2)))) AS refund_amount,
  MAX(DATE_FORMAT(order_create_time, 'yyyy-MM-dd')) AS order_date
FROM dwd_order_detail_changelog
GROUP BY order_id;

CREATE TABLE ads_order_lifecycle_kafka (
  channel STRING,
  total_orders BIGINT,
  paid_orders BIGINT,
  cancelled_orders BIGINT,
  refunded_orders BIGINT,
  order_amount DECIMAL(22,2),
  paid_amount DECIMAL(22,2),
  refund_amount DECIMAL(22,2),
  PRIMARY KEY (channel) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_order_lifecycle',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-ads-order-lifecycle-'
);

CREATE TABLE ads_order_daily_kafka (
  order_date STRING,
  channel STRING,
  total_orders BIGINT,
  paid_orders BIGINT,
  cancelled_orders BIGINT,
  refunded_orders BIGINT,
  order_amount DECIMAL(22,2),
  paid_amount DECIMAL(22,2),
  refund_amount DECIMAL(22,2),
  PRIMARY KEY (order_date, channel) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'ads_order_daily',
  'properties.bootstrap.servers' = 'kafka:29092',
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-ads-order-daily-'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO ads_order_lifecycle_kafka
  SELECT
    channel,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status IN ('PAID', 'REFUNDED') THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS paid_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS cancelled_orders,
    SUM(CASE WHEN order_status = 'REFUNDED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS refunded_orders,
    SUM(order_amount) AS order_amount,
    SUM(payment_amount) AS paid_amount,
    SUM(refund_amount) AS refund_amount
  FROM dws_order_current
  GROUP BY channel;

  INSERT INTO ads_order_daily_kafka
  SELECT
    order_date,
    channel,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN order_status IN ('PAID', 'REFUNDED') THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS paid_orders,
    SUM(CASE WHEN order_status = 'CANCELLED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS cancelled_orders,
    SUM(CASE WHEN order_status = 'REFUNDED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS refunded_orders,
    SUM(order_amount) AS order_amount,
    SUM(payment_amount) AS paid_amount,
    SUM(refund_amount) AS refund_amount
  FROM dws_order_current
  GROUP BY order_date, channel;
END;

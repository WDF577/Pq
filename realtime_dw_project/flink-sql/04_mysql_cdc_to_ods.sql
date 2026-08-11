-- MySQL snapshot + binlog CDC into compacted Kafka ODS topics.
-- Each source uses a unique server-id as required by MySQL replication.

CREATE TABLE mysql_order_info (
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
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'flink_cdc',
  'password' = 'flink_cdc',
  'database-name' = 'ecommerce',
  'table-name' = 'order_info',
  'server-id' = '5401',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'connect.max-retries' = '5'
);

CREATE TABLE mysql_order_detail (
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
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'flink_cdc',
  'password' = 'flink_cdc',
  'database-name' = 'ecommerce',
  'table-name' = 'order_detail',
  'server-id' = '5402',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'connect.max-retries' = '5'
);

CREATE TABLE mysql_payment_info (
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
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'flink_cdc',
  'password' = 'flink_cdc',
  'database-name' = 'ecommerce',
  'table-name' = 'payment_info',
  'server-id' = '5403',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'connect.max-retries' = '5'
);

CREATE TABLE mysql_refund_info (
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
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'flink_cdc',
  'password' = 'flink_cdc',
  'database-name' = 'ecommerce',
  'table-name' = 'refund_info',
  'server-id' = '5404',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'connect.max-retries' = '5'
);

CREATE TABLE mysql_dim_product_scd2 (
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
  'connector' = 'mysql-cdc',
  'hostname' = 'mysql',
  'port' = '3306',
  'username' = 'flink_cdc',
  'password' = 'flink_cdc',
  'database-name' = 'ecommerce',
  'table-name' = 'dim_product_scd2',
  'server-id' = '5405',
  'server-time-zone' = 'Asia/Shanghai',
  'scan.startup.mode' = 'initial',
  'scan.incremental.snapshot.enabled' = 'true',
  'connect.max-retries' = '5'
);

CREATE TABLE ods_order_info_kafka (
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
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-cdc-order-'
);

CREATE TABLE ods_order_detail_kafka (
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
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-cdc-detail-'
);

CREATE TABLE ods_payment_info_kafka (
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
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-cdc-payment-'
);

CREATE TABLE ods_refund_info_kafka (
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
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-cdc-refund-'
);

CREATE TABLE ods_dim_product_scd2_kafka (
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
  'key.format' = 'json',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'rtdw-cdc-product-scd2-'
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO ods_order_info_kafka SELECT * FROM mysql_order_info;
  INSERT INTO ods_order_detail_kafka SELECT * FROM mysql_order_detail;
  INSERT INTO ods_payment_info_kafka SELECT * FROM mysql_payment_info;
  INSERT INTO ods_refund_info_kafka SELECT * FROM mysql_refund_info;
  INSERT INTO ods_dim_product_scd2_kafka SELECT * FROM mysql_dim_product_scd2;
END;

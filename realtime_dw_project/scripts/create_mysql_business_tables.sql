USE ecommerce;

DROP PROCEDURE IF EXISTS sp_change_product_price;
DROP TABLE IF EXISTS refund_info;
DROP TABLE IF EXISTS payment_info;
DROP TABLE IF EXISTS order_detail;
DROP TABLE IF EXISTS order_info;
DROP TABLE IF EXISTS dim_product_scd2;
DROP TABLE IF EXISTS cdc_schema_contract;

-- Least-privilege account for Flink CDC incremental snapshots and binlog reads.
CREATE USER IF NOT EXISTS 'flink_cdc'@'%' IDENTIFIED BY 'flink_cdc';
GRANT SELECT, SHOW VIEW ON ecommerce.* TO 'flink_cdc'@'%';
GRANT SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'flink_cdc'@'%';

-- Lookup joins do not need binlog or write privileges.
CREATE USER IF NOT EXISTS 'flink_lookup'@'%' IDENTIFIED BY 'flink_lookup';
GRANT SELECT ON ecommerce.dim_product TO 'flink_lookup'@'%';
GRANT SELECT ON ecommerce.dim_shop TO 'flink_lookup'@'%';
GRANT SELECT ON ecommerce.dim_region TO 'flink_lookup'@'%';
FLUSH PRIVILEGES;

CREATE TABLE dim_product_scd2 (
  product_id BIGINT NOT NULL,
  version_no INT NOT NULL,
  product_name VARCHAR(100) NOT NULL,
  category_id BIGINT NOT NULL,
  category_name VARCHAR(100) NOT NULL,
  price DECIMAL(10,2) NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  effective_to DATETIME(3) NOT NULL,
  is_current TINYINT(1) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (product_id, version_no),
  KEY idx_product_scd2_range (product_id, effective_from, effective_to),
  KEY idx_product_scd2_current (product_id, is_current)
) ENGINE=InnoDB;

INSERT INTO dim_product_scd2 (
  product_id, version_no, product_name, category_id, category_name, price,
  effective_from, effective_to, is_current
)
SELECT
  product_id, 1, product_name, category_id, category_name, price,
  '2000-01-01 00:00:00.000', '9999-12-31 23:59:59.999', 1
FROM dim_product;

CREATE TABLE order_info (
  order_id VARCHAR(64) PRIMARY KEY,
  user_id BIGINT NOT NULL,
  shop_id BIGINT NOT NULL,
  order_status VARCHAR(20) NOT NULL,
  channel VARCHAR(20) NOT NULL,
  order_amount DECIMAL(12,2) NOT NULL,
  promotion_code VARCHAR(40) NULL,
  create_time DATETIME(3) NOT NULL,
  update_time DATETIME(3) NOT NULL,
  version_no BIGINT NOT NULL,
  KEY idx_order_user_time (user_id, create_time),
  KEY idx_order_status_time (order_status, update_time)
) ENGINE=InnoDB;

CREATE TABLE order_detail (
  detail_id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL,
  product_id BIGINT NOT NULL,
  quantity INT NOT NULL,
  unit_price DECIMAL(10,2) NOT NULL,
  detail_amount DECIMAL(12,2) NOT NULL,
  create_time DATETIME(3) NOT NULL,
  update_time DATETIME(3) NOT NULL,
  version_no BIGINT NOT NULL,
  KEY idx_order_detail_order (order_id),
  KEY idx_order_detail_product_time (product_id, create_time)
) ENGINE=InnoDB;

CREATE TABLE payment_info (
  payment_id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL,
  payment_status VARCHAR(20) NOT NULL,
  payment_method VARCHAR(20) NOT NULL,
  payment_amount DECIMAL(12,2) NOT NULL,
  payment_time DATETIME(3) NULL,
  update_time DATETIME(3) NOT NULL,
  version_no BIGINT NOT NULL,
  UNIQUE KEY uk_payment_order (order_id),
  KEY idx_payment_status_time (payment_status, update_time)
) ENGINE=InnoDB;

CREATE TABLE refund_info (
  refund_id VARCHAR(64) PRIMARY KEY,
  order_id VARCHAR(64) NOT NULL,
  payment_id VARCHAR(64) NOT NULL,
  refund_status VARCHAR(20) NOT NULL,
  refund_amount DECIMAL(12,2) NOT NULL,
  refund_reason VARCHAR(200) NULL,
  refund_time DATETIME(3) NULL,
  update_time DATETIME(3) NOT NULL,
  version_no BIGINT NOT NULL,
  UNIQUE KEY uk_refund_order (order_id),
  KEY idx_refund_status_time (refund_status, update_time)
) ENGINE=InnoDB;

CREATE TABLE cdc_schema_contract (
  table_name VARCHAR(64) NOT NULL,
  schema_version INT NOT NULL,
  compatible_change VARCHAR(32) NOT NULL,
  applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  description VARCHAR(255) NOT NULL,
  PRIMARY KEY (table_name, schema_version)
) ENGINE=InnoDB;

INSERT INTO cdc_schema_contract VALUES
('order_info', 1, 'BASELINE', CURRENT_TIMESTAMP(3), 'Initial order lifecycle contract'),
('order_detail', 1, 'BASELINE', CURRENT_TIMESTAMP(3), 'Initial order line contract'),
('payment_info', 1, 'BASELINE', CURRENT_TIMESTAMP(3), 'Initial payment contract'),
('refund_info', 1, 'BASELINE', CURRENT_TIMESTAMP(3), 'Initial refund contract'),
('dim_product_scd2', 1, 'BASELINE', CURRENT_TIMESTAMP(3), 'Initial SCD2 product contract');

DELIMITER $$
CREATE PROCEDURE sp_change_product_price(
  IN p_product_id BIGINT,
  IN p_new_price DECIMAL(10,2),
  IN p_effective_from DATETIME(3)
)
BEGIN
  DECLARE v_current_version INT;
  DECLARE v_current_start DATETIME(3);
  DECLARE v_product_name VARCHAR(100);
  DECLARE v_category_id BIGINT;
  DECLARE v_category_name VARCHAR(100);

  DECLARE EXIT HANDLER FOR SQLEXCEPTION
  BEGIN
    ROLLBACK;
    RESIGNAL;
  END;

  START TRANSACTION;
  SELECT version_no, effective_from, product_name, category_id, category_name
    INTO v_current_version, v_current_start, v_product_name, v_category_id, v_category_name
  FROM dim_product_scd2
  WHERE product_id = p_product_id AND is_current = 1
  FOR UPDATE;

  IF p_effective_from <= v_current_start THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'effective_from must advance the current SCD2 version';
  END IF;

  UPDATE dim_product_scd2
  SET effective_to = p_effective_from, is_current = 0
  WHERE product_id = p_product_id AND version_no = v_current_version;

  INSERT INTO dim_product_scd2 (
    product_id, version_no, product_name, category_id, category_name, price,
    effective_from, effective_to, is_current
  ) VALUES (
    p_product_id, v_current_version + 1, v_product_name, v_category_id,
    v_category_name, p_new_price, p_effective_from,
    '9999-12-31 23:59:59.999', 1
  );

  UPDATE dim_product SET price = p_new_price WHERE product_id = p_product_id;
  COMMIT;
END$$
DELIMITER ;

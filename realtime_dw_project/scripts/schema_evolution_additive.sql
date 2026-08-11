USE ecommerce;

-- Safe, repeatable additive rehearsal: the running explicit Flink schema
-- continues to project version-1 fields while the contract records version 2.
SET @delivery_type_exists = (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'order_info'
    AND column_name = 'delivery_type'
);
SET @delivery_type_ddl = IF(
  @delivery_type_exists = 0,
  'ALTER TABLE order_info ADD COLUMN delivery_type VARCHAR(20) NULL AFTER promotion_code',
  'SELECT ''delivery_type already exists; skipping DDL'''
);
PREPARE delivery_type_stmt FROM @delivery_type_ddl;
EXECUTE delivery_type_stmt;
DEALLOCATE PREPARE delivery_type_stmt;

INSERT INTO cdc_schema_contract (
  table_name, schema_version, compatible_change, applied_at, description
) VALUES (
  'order_info', 2, 'ADD_NULLABLE_COLUMN', CURRENT_TIMESTAMP(3),
  'Added nullable delivery_type; downstream adoption requires a planned savepoint upgrade'
)
ON DUPLICATE KEY UPDATE
  applied_at = VALUES(applied_at),
  description = VALUES(description);

UPDATE order_info
SET delivery_type = CASE WHEN MOD(user_id, 2) = 0 THEN 'EXPRESS' ELSE 'STANDARD' END
WHERE delivery_type IS NULL;

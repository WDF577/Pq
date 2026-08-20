-- Spark SQL owns the historical recomputation logic. Python only registers
-- JDBC sources, executes this statement and persists/reconciles the result.
WITH payment_current AS (
  SELECT
    order_id,
    MAX(CASE WHEN payment_status = 'SUCCESS' THEN payment_amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS payment_amount
  FROM payment_info
  GROUP BY order_id
),
refund_current AS (
  SELECT
    order_id,
    MAX(CASE WHEN refund_status = 'SUCCESS' THEN refund_amount ELSE CAST(0 AS DECIMAL(12, 2)) END) AS refund_amount
  FROM refund_info
  GROUP BY order_id
),
order_current AS (
  SELECT
    CAST(o.create_time AS DATE) AS order_date,
    o.order_id,
    o.channel,
    o.order_status,
    o.order_amount,
    COALESCE(p.payment_amount, CAST(0 AS DECIMAL(12, 2))) AS payment_amount,
    COALESCE(r.refund_amount, CAST(0 AS DECIMAL(12, 2))) AS refund_amount
  FROM order_info o
  LEFT JOIN payment_current p ON o.order_id = p.order_id
  LEFT JOIN refund_current r ON o.order_id = r.order_id
)
SELECT
  order_date,
  channel,
  COUNT(*) AS total_orders,
  SUM(CASE WHEN order_status IN ('PAID', 'REFUNDED') THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS paid_orders,
  SUM(CASE WHEN order_status = 'CANCELLED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS cancelled_orders,
  SUM(CASE WHEN order_status = 'REFUNDED' THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END) AS refunded_orders,
  CAST(SUM(order_amount) AS DECIMAL(22, 2)) AS order_amount,
  CAST(SUM(payment_amount) AS DECIMAL(22, 2)) AS paid_amount,
  CAST(SUM(refund_amount) AS DECIMAL(22, 2)) AS refund_amount
FROM order_current
GROUP BY order_date, channel

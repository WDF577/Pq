SELECT
  COALESCE(o.order_date, r.order_date) AS order_date,
  COALESCE(o.channel, r.channel) AS channel,
  COALESCE(o.total_orders, 0) AS offline_total_orders,
  COALESCE(r.total_orders, 0) AS realtime_total_orders,
  COALESCE(o.paid_orders, 0) AS offline_paid_orders,
  COALESCE(r.paid_orders, 0) AS realtime_paid_orders,
  COALESCE(o.cancelled_orders, 0) AS offline_cancelled_orders,
  COALESCE(r.cancelled_orders, 0) AS realtime_cancelled_orders,
  COALESCE(o.refunded_orders, 0) AS offline_refunded_orders,
  COALESCE(r.refunded_orders, 0) AS realtime_refunded_orders,
  COALESCE(o.order_amount, CAST(0 AS DECIMAL(22, 2))) AS offline_order_amount,
  COALESCE(r.order_amount, CAST(0 AS DECIMAL(22, 2))) AS realtime_order_amount,
  COALESCE(o.paid_amount, CAST(0 AS DECIMAL(22, 2))) AS offline_paid_amount,
  COALESCE(r.paid_amount, CAST(0 AS DECIMAL(22, 2))) AS realtime_paid_amount,
  COALESCE(o.refund_amount, CAST(0 AS DECIMAL(22, 2))) AS offline_refund_amount,
  COALESCE(r.refund_amount, CAST(0 AS DECIMAL(22, 2))) AS realtime_refund_amount,
  CASE WHEN
    COALESCE(o.total_orders, 0) = COALESCE(r.total_orders, 0)
    AND COALESCE(o.paid_orders, 0) = COALESCE(r.paid_orders, 0)
    AND COALESCE(o.cancelled_orders, 0) = COALESCE(r.cancelled_orders, 0)
    AND COALESCE(o.refunded_orders, 0) = COALESCE(r.refunded_orders, 0)
    AND COALESCE(o.order_amount, CAST(0 AS DECIMAL(22, 2))) = COALESCE(r.order_amount, CAST(0 AS DECIMAL(22, 2)))
    AND COALESCE(o.paid_amount, CAST(0 AS DECIMAL(22, 2))) = COALESCE(r.paid_amount, CAST(0 AS DECIMAL(22, 2)))
    AND COALESCE(o.refund_amount, CAST(0 AS DECIMAL(22, 2))) = COALESCE(r.refund_amount, CAST(0 AS DECIMAL(22, 2)))
  THEN true ELSE false END AS is_match
FROM offline_order_daily o
FULL OUTER JOIN realtime_order_daily r
  ON o.order_date = r.order_date AND o.channel = r.channel

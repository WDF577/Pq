-- Power BI Import query: one row per order date and channel.
SELECT
  order_date,
  channel,
  total_orders,
  paid_orders,
  cancelled_orders,
  refunded_orders,
  order_amount,
  paid_amount,
  refund_amount,
  paid_amount - refund_amount AS net_paid_amount
FROM ads_order_daily FINAL
WHERE is_deleted = 0
ORDER BY order_date, channel;

-- Optional current lifecycle snapshot: one row per channel.
SELECT
  channel,
  total_orders,
  paid_orders,
  cancelled_orders,
  refunded_orders,
  order_amount,
  paid_amount,
  refund_amount,
  paid_amount - refund_amount AS net_paid_amount
FROM ads_order_lifecycle FINAL
WHERE is_deleted = 0
ORDER BY paid_amount DESC;

-- Optional SCD2 audit table for a drill-through page.
SELECT
  product_id,
  version_no,
  product_name,
  category_name,
  price,
  effective_from,
  effective_to,
  is_current
FROM dim_product_scd2 FINAL
WHERE is_deleted = 0
ORDER BY product_id, version_no;

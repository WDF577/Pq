SELECT count() AS dwd_rows FROM dwd_user_behavior;

SELECT *
FROM ads_realtime_overview FINAL
ORDER BY window_start DESC
LIMIT 10;

SELECT
  window_start,
  window_end,
  product_name,
  category_name,
  pay_count,
  pay_amount
FROM ads_product_rank FINAL
ORDER BY window_start DESC, pay_amount DESC
LIMIT 10;

SELECT
  window_start,
  window_end,
  category_name,
  pay_count,
  pay_users,
  pay_amount
FROM ads_category_rank FINAL
ORDER BY window_start DESC, pay_amount DESC
LIMIT 10;

SELECT
  window_start,
  window_end,
  channel,
  view_users,
  cart_users,
  order_users,
  pay_users
FROM ads_channel_funnel FINAL
ORDER BY window_start DESC, view_users DESC
LIMIT 10;

SELECT *
FROM ads_realtime_alert
ORDER BY created_at DESC
LIMIT 10;

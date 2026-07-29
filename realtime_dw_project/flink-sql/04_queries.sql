-- ClickHouse 查询示例

SELECT *
FROM ads_realtime_overview FINAL
ORDER BY window_start DESC
LIMIT 20;

SELECT
  window_start,
  window_end,
  product_name,
  category_name,
  pay_count,
  pay_amount
FROM ads_product_rank FINAL
ORDER BY window_start DESC, pay_amount DESC
LIMIT 20;

SELECT
  channel,
  countIf(event_type = 'view') AS view_cnt,
  countIf(event_type = 'cart') AS cart_cnt,
  countIf(event_type = 'order') AS order_cnt,
  countIf(event_type = 'pay') AS pay_cnt,
  sumIf(amount, event_type = 'pay') AS pay_amount
FROM dwd_user_behavior
GROUP BY channel
ORDER BY pay_amount DESC;

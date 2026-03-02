SELECT
  o.order_year,
  o.order_month,
  COUNT(DISTINCT o.order_key) AS total_orders,
  COUNT(DISTINCT o.customer_key) AS unique_customers,
  SUM(o.total_price) AS total_revenue,
  AVG(o.total_price) AS avg_order_value
FROM acme_edw_dev.edw_silver.orders o
GROUP BY o.order_year, o.order_month
ORDER BY o.order_year, o.order_month

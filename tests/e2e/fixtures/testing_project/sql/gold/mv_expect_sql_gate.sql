SELECT
  o.order_key,
  o.customer_key,
  o.total_price,
  (o.total_price IS NOT NULL AND o.order_key IS NOT NULL) AS amount_reconciled
FROM ${catalog}.${silver_schema}.orders o

-- Gold aggregation for the sales_by_nation materialized view.
-- ${...} substitution tokens resolve inside external SQL files too.
-- Uses the POST-RENAME silver column names (total_price, customer_id) and
-- joins only the CURRENT SCD2 dimension rows (__END_AT IS NULL).
SELECT n.n_name AS nation, count(*) AS order_count, sum(o.total_price) AS revenue
FROM ${catalog}.${silver_schema}.orders o
JOIN ${catalog}.${silver_schema}.dim_customer c
  ON o.customer_id = c.c_custkey AND c.__END_AT IS NULL
JOIN ${catalog}.${bronze_schema}.nation n ON c.c_nationkey = n.n_nationkey
GROUP BY n.n_name

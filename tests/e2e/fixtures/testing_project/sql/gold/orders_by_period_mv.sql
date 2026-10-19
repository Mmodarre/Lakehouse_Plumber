-- Rendered with the gold_mv_template parameters at template expansion,
-- then environment tokens are substituted.
SELECT
{%- for column in group_by %}
  o.{{ column }},
{%- endfor %}
  COUNT(DISTINCT o.order_key) AS total_orders,
  SUM(o.total_price) AS total_revenue
FROM ${catalog}.{{ silver_schema }}.orders o
GROUP BY {% for column in group_by %}o.{{ column }}{% if not loop.last %}, {% endif %}{% endfor %}

SELECT
  c.customer_id,
  c.market_segment
FROM ${catalog}.{{ silver_schema }}.customer_dim c
{%- if current_only %}
WHERE c.__end_at IS NULL
{%- endif %}

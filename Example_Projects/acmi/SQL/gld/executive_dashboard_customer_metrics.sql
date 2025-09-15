SELECT
    COUNT(*) as total_customers,
    AVG(lifetime_value) as avg_customer_lifetime_value,
    AVG(total_orders) as avg_orders_per_customer,
    AVG(customer_tenure_days) as avg_customer_tenure_days,
    SUM(
        CASE
            WHEN total_orders = 1 THEN 1
            ELSE 0
        END
    ) as one_time_customers,
    SUM(
        CASE
            WHEN total_orders > 5 THEN 1
            ELSE 0
        END
    ) as loyal_customers
FROM
    { catalog }.{ gold_schema }.customer_lifetime_value_mv target: v_customer_metrics_sql - name: top_products_sql type: load source: type: sql sql: |
SELECT
    month,
    COUNT(DISTINCT part_id) as active_products,
    SUM(total_revenue) as product_revenue,
    AVG(avg_unit_price) as avg_product_price,
    SUM(total_quantity_sold) as total_units_sold
FROM
    { catalog }.{ gold_schema }.product_performance_mv
GROUP BY
    month
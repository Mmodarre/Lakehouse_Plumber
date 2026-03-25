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
    {catalog}.{gold_schema}.customer_lifetime_value_mv
SELECT
    c.customer_id,
    c.name as customer_name,
    c.market_segment,
    n.name as nation,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(o.total_price) as lifetime_value,
    AVG(o.total_price) as avg_order_value,
    MIN(o.order_date) as first_order_date,
    MAX(o.order_date) as last_order_date,
    DATEDIFF(MAX(o.order_date), MIN(o.order_date)) as customer_tenure_days
FROM
    { catalog }.{ silver_schema }.customer_dim c
    JOIN { catalog }.{ silver_schema }.orders_fct o ON c.customer_id = o.customer_id
    AND o.order_date >= c.__start_at
    AND (
        o.order_date < c.__end_at
        OR c.__end_at IS NULL
    )
    JOIN { catalog }.{ silver_schema }.nation_dim n ON c.nation_id = n.nation_id
    AND o.order_date >= n.__start_at
    AND (
        o.order_date < n.__end_at
        OR n.__end_at IS NULL
    )
GROUP BY
    c.customer_id,
    c.name,
    c.market_segment,
    n.name
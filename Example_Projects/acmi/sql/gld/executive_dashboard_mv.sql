SELECT
    ms.year,
    ms.month,
    CONCAT(ms.year, '-', LPAD(ms.month, 2, '0')) as year_month,
    -- Revenue Metrics
    ms.monthly_revenue,
    ms.prev_month_revenue,
    CASE
        WHEN ms.prev_month_revenue > 0 THEN ROUND(
            (ms.monthly_revenue - ms.prev_month_revenue) / ms.prev_month_revenue * 100,
            2
        )
        ELSE NULL
    END as revenue_growth_pct,
    -- Order Metrics
    ms.monthly_orders,
    ms.prev_month_orders,
    CASE
        WHEN ms.prev_month_orders > 0 THEN ROUND(
            (ms.monthly_orders - ms.prev_month_orders) / ms.prev_month_orders * 100,
            2
        )
        ELSE NULL
    END as orders_growth_pct,
    -- Customer Metrics
    ms.monthly_customers,
    ms.avg_monthly_order_value,
    cm.avg_customer_lifetime_value,
    cm.avg_orders_per_customer,
    cm.avg_customer_tenure_days,
    ROUND(
        cm.one_time_customers / cm.total_customers * 100,
        2
    ) as one_time_customer_pct,
    ROUND(cm.loyal_customers / cm.total_customers * 100, 2) as loyal_customer_pct,
    -- Regional Performance
    rp.active_regions,
    rp.total_regional_revenue,
    rp.avg_regional_revenue,
    -- Product Performance
    tp.active_products,
    tp.product_revenue,
    tp.avg_product_price,
    tp.total_units_sold,
    -- KPIs
    ROUND(ms.monthly_revenue / ms.monthly_customers, 2) as revenue_per_customer,
    ROUND(ms.monthly_revenue / ms.monthly_orders, 2) as revenue_per_order,
    ROUND(tp.total_units_sold / ms.monthly_orders, 2) as units_per_order,
    -- Current timestamp for freshness
    CURRENT_TIMESTAMP() as dashboard_updated_at
FROM
    v_monthly_sales_sql ms
    LEFT JOIN v_regional_performance_sql rp ON ms.year = YEAR(rp.month)
    AND ms.month = MONTH(rp.month)
    LEFT JOIN v_top_products_sql tp ON ms.year = YEAR(tp.month)
    AND ms.month = MONTH(tp.month)
    CROSS JOIN v_customer_metrics_sql cm
WHERE
    ms.year >= YEAR(add_months(CURRENT_DATE(), -24))
ORDER BY
    ms.year,
    ms.month
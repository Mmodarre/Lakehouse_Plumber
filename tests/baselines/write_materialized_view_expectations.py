@dp.materialized_view(
    name="gold_cat.gold_sch.customer_summary",
    comment="Materialized view: customer_summary",
    table_properties={})
@dp.expect_all({"plausible_count": "customer_count < 1000000"})
@dp.expect_all_or_drop({"counted": "customer_count > 0"})
@dp.expect_all_or_fail({"has_region": "region IS NOT NULL"})
def customer_summary():
    """Write to gold_cat.gold_sch.customer_summary from multiple sources"""
    # Materialized views use batch processing
    df = spark.sql("""SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region""")
    
    return df 
@dp.materialized_view(
    name="gold_cat.gold_sch.customer_summary",
    comment="Materialized view: customer_summary",
    table_properties={})
def customer_summary():
    """Write to gold_cat.gold_sch.customer_summary from multiple sources"""
    # Materialized views use batch processing
    df = spark.sql("""SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region""")
    
    return df 
@dp.materialized_view(
    name="gold.sch.orders_mv",
    comment="Materialized view: orders_mv",
    table_properties={})
@dp.expect_all({"small_amt": "amount < 1000"})
@dp.expect_all_or_drop({"pos_amt": "amount > 0"})
@dp.expect_all_or_fail({"has_id": "id IS NOT NULL"})
def orders_mv():
    """Write to gold.sch.orders_mv from multiple sources"""
    # Materialized views use batch processing
    df = spark.read.table("v_orders")
    
    # Add operational metadata columns
    df = df.withColumns({        '_loaded_at': F.current_timestamp()    })
    
    return df 
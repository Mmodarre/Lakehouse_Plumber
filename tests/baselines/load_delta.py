@dp.temporary_view()
def v_customers():
    """Delta source: main.bronze.customers"""
    df = spark.readStream.table("main.bronze.customers")
    
    return df 
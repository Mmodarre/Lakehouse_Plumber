@dp.temporary_view(comment="SQL transform: transform_customers")
def v_customers_clean():
    """SQL transform: transform_customers"""
    df = spark.sql("""SELECT * FROM v_customers WHERE email IS NOT NULL""")
    
    return df 
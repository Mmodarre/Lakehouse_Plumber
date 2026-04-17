@dp.temporary_view()
def v_metrics():
    """SQL source: load_metrics"""
    df = spark.sql("""SELECT * FROM metrics WHERE date > current_date() - 7""")
    
    return df 
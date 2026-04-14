# Try to register the custom data source
try:
    spark.dataSource.register(TestDataSource)
except Exception:
    pass  # Ignore if already registered

@dp.temporary_view()
def v_test_data():
    """Load data from custom data source: TestDataSource"""
    df = spark.readStream \
        .format("test_datasource") \
        .load()
    
    
    return df 
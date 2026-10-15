# Register the custom data source
spark.dataSource.register(TestDataSource)

@dp.temporary_view()
def v_test_data():
    """Load data from custom data source: TestDataSource"""
    df = spark.readStream \
        .format("test_datasource") \
        .load()
    
    
    return df 
@dp.temporary_view()
def v_custom_data():
    """Python source: custom_loaders.load_custom_data"""
    # Call the external Python function with spark and parameters
    parameters = {"start_date": "2024-01-01", "batch_size": 1000}
    df = load_custom_data(spark, parameters)
    
    return df 
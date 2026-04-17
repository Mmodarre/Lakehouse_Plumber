@dp.temporary_view()
def v_raw_files():
    """Load data from json files at /mnt/data/raw"""
    df = spark.readStream \
        .format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/mnt/data/raw")
    
    return df 
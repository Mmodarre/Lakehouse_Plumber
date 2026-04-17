@dp.temporary_view()
def v_kafka_data():
    """Load data from Kafka topics at localhost:9092"""
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "test_topic") \
        .load()
    
    
    return df

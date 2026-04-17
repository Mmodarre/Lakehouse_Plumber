# Kafka sink to test_topic

# Create Kafka sink
dp.create_sink(
    name="kafka_sink",
    format="kafka",
    options={"kafka.bootstrap.servers": "localhost:9092", "topic": "test_topic"}
)

# Define append flow(s)
@dp.append_flow(
    target="kafka_sink",
    name="f_kafka_sink_1",
    comment="Kafka sink to test_topic"
)
def f_kafka_sink_1():
    df = spark.readStream.table("v_data")
    
    
    # Runtime validation for strict mode (user must provide key and value columns)
    if "value" not in df.columns:
        raise ValueError("Kafka sink requires 'value' column in strict mode. Add a transform action to create it.")
    # Key column is optional for Kafka
    
    return df







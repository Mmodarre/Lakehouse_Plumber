# Custom sink: MyCustomDataSink

# Register custom data sink
try:
    spark.dataSource.register(MyCustomDataSink)
except Exception:
    pass  # Already registered

# Create custom sink
dp.create_sink(
    name="custom_sink",
    format="my_custom_format",
    options={}
)

# Define append flow(s)
@dp.append_flow(
    target="custom_sink",
    name="f_custom_sink_1",
    comment="""Custom sink: MyCustomDataSink"""
)
def f_custom_sink_1():
    df = spark.readStream.table("v_data")
    
    
    return df

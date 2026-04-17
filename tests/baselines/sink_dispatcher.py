# Delta sink to catalog.schema.table

# Create Delta sink
dp.create_sink(
    name="delta_sink",
    format="delta",
    options={"tableName": "catalog.schema.table"}
)

# Define append flow(s)
@dp.append_flow(
    target="delta_sink",
    name="f_delta_sink_1",
    comment="Delta sink to catalog.schema.table"
)
def f_delta_sink_1():
    df = spark.readStream.table("v_data")
    
    
    return df







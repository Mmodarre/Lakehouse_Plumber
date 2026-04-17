# Create the streaming table
dp.create_streaming_table(
    name="silver_cat.silver_sch.customers",
    comment="Streaming table: customers")


# Define append flow(s)
@dp.append_flow(
    target="silver_cat.silver_sch.customers",
    name="f_customers",
    comment="Append flow to silver_cat.silver_sch.customers"
)
def f_customers():
    """Append flow to silver_cat.silver_sch.customers"""
    # Streaming flow
    df = spark.readStream.table("v_customers_final")
    
    return df

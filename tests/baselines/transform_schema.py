@dp.temporary_view()
def v_customer_standardized():
    """Standardize customer schema and data types"""
    df = spark.read.table("v_customer_raw")
    
    # Apply column renaming
    df = df.withColumnRenamed("c_custkey", "customer_id")
    df = df.withColumnRenamed("c_name", "customer_name")
    df = df.withColumnRenamed("c_address", "address")
    df = df.withColumnRenamed("c_phone", "phone_number")
    
    # Apply type casting
    df = df.withColumn("customer_id", F.col("customer_id").cast("BIGINT"))
    df = df.withColumn("account_balance", F.col("account_balance").cast("DECIMAL(18,2)"))
    df = df.withColumn("phone_number", F.col("phone_number").cast("STRING"))
    
    # Strict schema enforcement - select only specified columns
    # Schema-defined columns (will fail if missing)
    columns_to_select = [
        "customer_id",        "customer_name",        "address",        "phone_number",        "account_balance"    ]
    
    
    df = df.select(*columns_to_select)
    
    return df 
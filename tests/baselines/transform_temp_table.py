@dp.table(
    temporary=True,
)
def customers_staging():
    """Temporary table: customers_staging"""
    df = spark.read.table("v_customers_enriched")

    return df 
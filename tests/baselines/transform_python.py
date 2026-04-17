@dp.temporary_view()
def v_customers_enriched():
    """Python transform: enrich.enrich"""
    # Load source view(s)
    v_customers_validated_df = spark.read.table("v_customers_validated")
    
    # Apply Python transformation
    parameters = {"enrichment_type": "full"}
    df = enrich(v_customers_validated_df, spark, parameters)
    
    
    return df 
@dp.temporary_view()
# These expectations will fail the pipeline if violated
@dp.expect_all_or_fail({"id_not_null": "id IS NOT NULL"})
# These expectations will drop rows that violate them
@dp.expect_all_or_drop({"age_check": "age >= 18"})
# These expectations will log warnings but not drop rows
@dp.expect_all({"email_not_null": "email IS NOT NULL"})
def v_customers_validated():
    """Data quality checks for v_customers_clean"""
    df = spark.readStream.table("v_customers_clean")
    
    return df
@dp.temporary_view()
def v_external_data():
    """JDBC source: load_external"""
    df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://host:5432/mydb") \
        .option("user", "admin") \
        .option("password", "pass123") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "external_table") \
        .load()
    
    return df 
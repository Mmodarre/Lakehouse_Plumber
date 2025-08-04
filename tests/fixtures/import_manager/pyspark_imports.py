"""PySpark file with various import patterns and potential conflicts."""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions
from pyspark.sql.window import Window

def create_sample_dataframe(spark: SparkSession) -> DataFrame:
    """Create a sample DataFrame using imported functions."""
    df = spark.createDataFrame([
        ("Alice", 25),
        ("Bob", 30)
    ], ["name", "age"])
    
    return df.withColumn("processed_at", F.current_timestamp()) \
             .withColumn("status", lit("active")) 
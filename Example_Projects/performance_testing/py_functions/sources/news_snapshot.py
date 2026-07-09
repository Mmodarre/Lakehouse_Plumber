"""Snapshot function for market news — reads current state from bronze table."""

from pyspark.sql import DataFrame, SparkSession


def next_snapshot(latest_version: int, *, catalog: str, schema: str) -> tuple[DataFrame, int] | None:
    spark = SparkSession.getActiveSession()
    table_name = f"{catalog}.{schema}.market_news_raw"

    current_version = (
        spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        .select("version")
        .first()[0]
    )

    if current_version <= latest_version:
        return None

    df = spark.read.table(table_name)
    return df, current_version

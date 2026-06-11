"""Localize every UTC timestamp column to a target timezone.

Dynamic by design: inspect the DataFrame schema at runtime and convert *all*
TimestampType columns — regardless of name or how many there are. In SQL you
must name each column explicitly; in Python it is a loop over the schema. This
is the canonical reason to reach for a python transform.
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import TimestampType


def normalize_timestamps(df: DataFrame, spark, parameters: dict) -> DataFrame:
    target_tz = parameters.get("target_tz", "UTC")
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampType):
            df = df.withColumn(
                field.name, F.from_utc_timestamp(F.col(field.name), target_tz)
            )
    return df

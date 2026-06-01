"""Leaf helper module for the transitive helper-copying E2E fixture.

Has no local imports of its own — it is the deepest node in the closure.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import trim


def clean(df: DataFrame) -> DataFrame:
    """Trim whitespace from the ``name`` column."""
    return df.withColumn("name", trim(df["name"]))

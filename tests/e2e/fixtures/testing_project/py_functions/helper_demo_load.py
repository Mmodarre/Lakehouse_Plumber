"""Entry module (python LOAD) for the helper-copying E2E fixture.

Imports TWO local helpers via absolute-local imports:
  * ``from helpers.aggregate import aggregate_rows`` — whose own
    ``from helpers.other import tag`` is a transitive ABSOLUTE helper->helper
    import (rewritten inside the copied helper);
  * ``from helpers.sub.deep import deepen`` — a NESTED sub-package member
    (copied with structure preserved).
Both entry imports are prefix-rewritten to ``from custom_python_functions.…``.
"""

from pyspark.sql import DataFrame, SparkSession

from helpers.aggregate import aggregate_rows
from helpers.sub.deep import deepen


def load_helper_rows(spark: SparkSession, parameters: dict) -> DataFrame:
    """Build a tiny DataFrame, then enrich it through the helper closure."""
    rows = [(1, "  alpha  "), (2, "  beta  ")]
    df = spark.createDataFrame(rows, schema="id INT, name STRING")
    return deepen(aggregate_rows(df))

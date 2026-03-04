"""Jobs stats loader for monitoring pipeline.

Uses the Databricks SDK to fetch Jobs run statistics.
"""

from pyspark.sql import DataFrame


def get_jobs_stats(spark, parameters) -> DataFrame:
    """Fetch Jobs run statistics using Databricks SDK.

    Args:
        spark: SparkSession instance
        parameters: Pipeline parameters dict

    Returns:
        DataFrame with jobs run statistics
    """
    raise NotImplementedError(
        "Jobs stats loader not yet implemented. "
        "Replace this placeholder with Databricks SDK calls to fetch job run stats."
    )

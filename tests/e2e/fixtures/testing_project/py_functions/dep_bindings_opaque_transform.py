from pyspark.sql import DataFrame


def _qualified(suffix: str) -> str:
    return "acme_edw_dev.edw_bronze." + suffix


def opaque_helper_read(df: DataFrame, spark, parameters) -> DataFrame:
    """Read a table whose name is computed by a helper call.

    Static dependency extraction does not follow helper calls, so this read is
    opaque and surfaces as an LHP-DEP-002 warning recommending depends_on.
    """
    lookup = spark.read.table(_qualified(parameters["suffix"]))
    return df.unionByName(lookup, allowMissingColumns=True)

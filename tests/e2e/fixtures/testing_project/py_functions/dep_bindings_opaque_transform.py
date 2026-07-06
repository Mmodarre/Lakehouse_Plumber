import os

from pyspark.sql import DataFrame


def opaque_helper_read(df: DataFrame, spark, parameters) -> DataFrame:
    """Read a table whose name only exists in the runtime environment.

    The table name comes from an environment variable, so no static
    resolution (parameter binding, call-site propagation, return folding)
    can ever know it — the read stays opaque and surfaces as an
    LHP-DEP-002 advisory recommending an explicit depends_on declaration.
    """
    lookup = spark.read.table(os.environ["DEP_BINDINGS_LOOKUP_TABLE"])
    return df.unionByName(lookup, allowMissingColumns=True)

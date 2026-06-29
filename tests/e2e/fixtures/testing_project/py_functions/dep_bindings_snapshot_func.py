from typing import Optional, Tuple
from pyspark.sql import DataFrame


def next_param_snapshot(
    latest_snapshot_version: Optional[int],
    *,
    source_table: str,
) -> Optional[Tuple[DataFrame, int]]:
    """Parameter-driven snapshot reader.

    The snapshot table name arrives as a YAML-declared parameter bound to the
    keyword-only ``source_table`` argument via functools.partial.
    """

    if latest_snapshot_version is None:
        df = spark.read.table(source_table)
        return (df, 1)

    return None

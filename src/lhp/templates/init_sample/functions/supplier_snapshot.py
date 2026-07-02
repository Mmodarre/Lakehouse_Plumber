from typing import Optional, Tuple
from pyspark.sql import DataFrame

def next_supplier_snapshot(
    latest_version: Optional[int],
    *,
    catalog: str,
    bronze_schema: str,
) -> Optional[Tuple[DataFrame, int]]:
    """Return the next supplier snapshot and its version, or None when exhausted."""
    table = f"{catalog}.{bronze_schema}.supplier_snapshots"
    if latest_version is None:
        nxt = spark.sql(f"SELECT min(snapshot_id) AS v FROM {table}").collect()[0].v
    else:
        nxt = spark.sql(
            f"SELECT min(snapshot_id) AS v FROM {table} WHERE snapshot_id > {latest_version}"
        ).collect()[0].v
    if nxt is None:
        return None
    df = spark.sql(f"SELECT * FROM {table} WHERE snapshot_id = {nxt}").drop("snapshot_id")
    return (df, int(nxt))

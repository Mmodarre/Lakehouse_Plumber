from typing import Optional, Tuple
from pyspark.sql import DataFrame


def next_supplier_snapshot(
    latest_snapshot_version: Optional[int],
    *,
    catalog: str,
    schema: str,
    table: str,
) -> Optional[Tuple[DataFrame, int]]:

    if latest_snapshot_version is None:
        df = spark.sql(f"""
            SELECT * FROM {catalog}.{schema}.{table}
            WHERE snapshot_id = (SELECT min(snapshot_id) FROM {catalog}.{schema}.{table})
        """)

        min_snapshot_id = spark.sql(f"""
            SELECT min(snapshot_id) as min_id FROM {catalog}.{schema}.{table}
        """).collect()[0].min_id

        return (df, min_snapshot_id)

    else:
        next_snapshot_result = spark.sql(f"""
            SELECT min(snapshot_id) as next_id
            FROM {catalog}.{schema}.{table}
            WHERE snapshot_id > '{latest_snapshot_version}'
        """).collect()[0]

        if next_snapshot_result.next_id is None:
            return None

        next_snapshot_id = next_snapshot_result.next_id
        df = spark.sql(f"""
            SELECT * FROM {catalog}.{schema}.{table}
            WHERE snapshot_id = '{next_snapshot_id}'
        """)

        return (df, next_snapshot_id)

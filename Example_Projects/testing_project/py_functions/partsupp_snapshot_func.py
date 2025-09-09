from typing import Optional, Tuple
from pyspark.sql import DataFrame


def next_snapshot_and_version(
        latest_snapshot_version: Optional[int]
) -> Optional[Tuple[DataFrame, int]]:

    if latest_snapshot_version is None:
        df = spark.sql("""
            SELECT * FROM {catalog}.{bronze_schema}.partsupp 
            WHERE snapshot_id = (SELECT min(snapshot_id) FROM {catalog}.{bronze_schema}.partsupp)
        """)

        min_snapshot_id = spark.sql("""
            SELECT min(snapshot_id) as min_id FROM {catalog}.{bronze_schema}.partsupp
        """).collect()[0].min_id

        return (df, min_snapshot_id)

    else:
        next_snapshot_result = spark.sql(f"""
            SELECT min(snapshot_id) as next_id 
            FROM {catalog}.{bronze_schema}.partsupp 
            WHERE snapshot_id > '{latest_snapshot_version}'
        """).collect()[0]

        if next_snapshot_result.next_id is None:
            return None

        next_snapshot_id = next_snapshot_result.next_id
        df = spark.sql(f"""
            SELECT * except(rn) FROM (
                SELECT *, 
                ROW_NUMBER() OVER (PARTITION BY part_id, supplier_id ORDER BY last_modified_dt DESC) as rn
                FROM {catalog}.{bronze_schema}.partsupp
                WHERE snapshot_id = '{next_snapshot_id}'
            ) 
            WHERE rn = 1
        """)

        return (df, next_snapshot_id)

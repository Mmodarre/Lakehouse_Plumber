from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.expect_all_or_fail({"row_count_match": "abs(source_count - target_count) <= 0"})
@dp.table(name="tmp_test_test_row_count", comment="Test: row_count", temporary=True)
def tmp_test_test_row_count():
    """Test: row_count"""
    return spark.sql("""
        SELECT * FROM
                      (SELECT COUNT(*) AS source_count FROM v_source),
                      (SELECT COUNT(*) AS target_count FROM v_target)
    """)
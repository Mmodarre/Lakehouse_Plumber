"""Custom data source with wildcard imports to test operational metadata import consistency."""

from pyspark.sql.functions import *  # Wildcard import - triggers expression adaptation
from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


class APIWithWildcardSource(DataSource):
    """Custom data source that uses wildcard imports."""

    @classmethod
    def name(cls):
        return "api_wildcard"

    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
                StructField("value", IntegerType(), True),
            ]
        )

    def reader(self, schema: StructType):
        return APIWithWildcardReader(schema, self.options)


class APIWithWildcardReader(DataSourceReader):
    """Reader for API with wildcard source."""

    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def read(self, partition):
        # Generate some test data using wildcard-imported functions
        yield (1, "test_item", 100)
        yield (2, "another_item", 200)


# Register the data source
spark.dataSource.register(APIWithWildcardSource)

"""Entry module (custom data source) for the transitive helper-copying E2E fixture.

Imports the same LOCAL helper sub-package as the transform entry. Because this
class is registered as a custom PySpark data source, LHP emits a
``register_pickle_by_value(custom_python_functions)`` line so the class travels
by value; the helper closure must be embedded for that to be importable on the
executor.
"""

from pyspark.sql.datasource import DataSource, DataSourceReader
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

from helpers.transforms import enrich


class HelperBackedSource(DataSource):
    """Custom data source whose reader leans on the imported helper closure."""

    @classmethod
    def name(cls):
        return "helper_backed"

    def schema(self):
        return StructType(
            [
                StructField("id", IntegerType(), False),
                StructField("name", StringType(), True),
            ]
        )

    def reader(self, schema: StructType):
        return HelperBackedReader(schema, self.options)


class HelperBackedReader(DataSourceReader):
    """Reader that references the helper so the import is load-bearing."""

    def __init__(self, schema, options):
        self.schema = schema
        self.options = options
        self._enrich = enrich

    def read(self, partition):
        yield (1, "  alpha  ")
        yield (2, "  beta  ")

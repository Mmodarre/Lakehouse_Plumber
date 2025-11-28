"""Custom data sink with wildcard imports to test BaseSinkWriteGenerator operational metadata."""

from pyspark.sql.functions import *  # Wildcard import - triggers expression adaptation
from pyspark.sql.datasource import DataSink, DataSourceWriter

class CustomSinkWithWildcard(DataSink):
    """Custom sink that uses wildcard imports."""
    
    @classmethod
    def name(cls):
        return "custom_sink_wildcard"
    
    def writer(self, schema, saveMode):
        return CustomSinkWriter(schema, saveMode, self.options)


class CustomSinkWriter(DataSourceWriter):
    """Writer for custom sink with wildcard."""
    
    def __init__(self, schema, save_mode, options):
        self.schema = schema
        self.save_mode = save_mode
        self.options = options
    
    def write(self, iterator):
        # Process rows using wildcard-imported functions
        for row in iterator:
            # Simulate writing data
            pass
    
    def commit(self, messages):
        # Commit write operation
        pass
    
    def abort(self, messages):
        # Abort write operation
        pass


# Register the sink
spark.dataSource.register(CustomSinkWithWildcard)


"""Sample custom data source file for testing ImportManager."""

from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
from pyspark.sql.functions import *
from pyspark.sql.types import *
from typing import Iterator, Tuple
import dlt
import json
import os
import requests
import time

# Test comment that should be preserved

class TestInputPartition(InputPartition):
    """Input partition for test data source."""
    def __init__(self, start_time, end_time):
        self.start_time = start_time
        self.end_time = end_time

class TestDataSource(DataSource):
    """Test data source for ImportManager testing."""

    @classmethod
    def name(cls):
        return "test_datasource"

    def schema(self):
        return """
            id int,
            name string,
            timestamp timestamp
        """

    def streamReader(self, schema):
        return TestDataSourceReader(schema, self.options)

class TestDataSourceReader(DataSourceStreamReader):
    """Test streaming reader."""

    def __init__(self, schema, options):
        self.schema = schema
        self.options = options

    def initialOffset(self):
        return {"offset": 0}

    def latestOffset(self):
        return {"offset": 1}

    def partitions(self, start, end):
        return [TestInputPartition(start, end)]

    def commit(self, end):
        pass

    def read(self, partition):
        yield (1, "test", time.time())

# This comment should also be preserved 
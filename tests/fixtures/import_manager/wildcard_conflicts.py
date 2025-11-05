"""File with wildcard import conflicts for testing."""

# This should create conflicts that ImportManager needs to resolve
from pyspark.sql import functions as F
from pyspark.sql.functions import *  # Should take precedence
from pyspark.sql.types import StructType
from pyspark.sql.types import *  # Should take precedence
from pyspark import pipelines as dp
from datetime import datetime, timedelta

def sample_with_conflicts():
    """Function using imports that have conflicts."""
    # These calls should work with either import style
    return {
        "timestamp": current_timestamp(),  # From wildcard import
        "formatted": F.lit("test"),        # Would fail if F import is removed
        "struct": StructType([])           # Would fail if specific import is removed
    } 
"""File with mixed import styles for comprehensive testing."""

# Standard library imports
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime, timedelta

# Third-party imports  
import requests
import json

# PySpark imports - various styles
import pyspark
from pyspark.sql import SparkSession
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StructType, StringType
import pyspark.sql.functions as psf

# DLT imports
import dlt
from dlt import view, table

# Custom/relative imports
from .utils import helper_function
from ..config import settings

def complex_function(spark: SparkSession) -> None:
    """Function using various imported modules."""
    data_path = Path("/tmp/data")
    
    df = spark.read.json(str(data_path))
    
    # Using different import styles
    result = df.select(
        F.col("name"),
        col("age"),
        lit("processed"),
        psf.current_timestamp().alias("timestamp")
    ).filter(
        when(F.col("age") > 18, True).otherwise(False)
    )
    
    return result 
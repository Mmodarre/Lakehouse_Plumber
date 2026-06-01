"""Leaf module inside the nested ``helpers/sub`` sub-package.

Referenced by the python-load entry via ``from helpers.sub.deep import deepen``
(absolute-local, two segments deep). LHP mirrors the nested directory under
``custom_python_functions/helpers/sub/deep.py`` and prefix-rewrites the entry's
import to ``from custom_python_functions.helpers.sub.deep import deepen``.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def deepen(df: DataFrame) -> DataFrame:
    """Add a marker column proving the nested helper was reachable."""
    return df.withColumn("depth", lit("helpers.sub.deep"))

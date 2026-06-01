"""Helper that exercises a transitive ABSOLUTE helper->helper import.

Distinct from ``helpers/transforms.py`` (which uses the RELATIVE form
``from .util import clean``): this module reaches its sibling via the
ABSOLUTE-local form ``from helpers.other import tag``. Once copied under
``custom_python_functions/``, LHP prefix-rewrites that line to
``from custom_python_functions.helpers.other import tag`` while the
``from pyspark.sql.functions import lit`` external import is left untouched.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from helpers.other import tag


def aggregate_rows(df: DataFrame) -> DataFrame:
    """Tag the rows (via the absolute-imported sibling) then mark them aggregated."""
    tagged = tag(df)
    return tagged.withColumn("aggregated", lit(True))

"""Sibling helper module — the target of an ABSOLUTE local helper->helper import.

Has only an external import of its own (pyspark). It is referenced by
``helpers/aggregate.py`` via ``from helpers.other import tag`` (absolute-local),
which LHP prefix-rewrites to ``from custom_python_functions.helpers.other import tag``
inside the copied ``aggregate.py`` — proving transitive-absolute rewrite reaches
helper-to-helper imports, not just the entry.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


def tag(df: DataFrame) -> DataFrame:
    """Tag the input with a constant marker column."""
    return df.withColumn("tagged_by", lit("helpers.other"))

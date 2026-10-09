"""The ``pyspark.sql.types`` names generated code may reference, and their import form.

Single source of truth for two consumers: the ``StructType`` emitter, which
reports the names a schema uses, and :class:`ImportDetector`, which resolves the
same names when an operational-metadata expression calls them. Both register
one ``from pyspark.sql.types import <Name>`` statement per name, so the import
manager's set-dedup merges overlapping names across actions rather than emitting
a redefinition.
"""

SPARK_TYPE_NAMES: tuple[str, ...] = (
    "StructType",
    "StructField",
    "StringType",
    "LongType",
    "IntegerType",
    "DoubleType",
    "FloatType",
    "BooleanType",
    "DateType",
    "TimestampType",
    "DecimalType",
    "BinaryType",
    "ByteType",
    "ShortType",
)


def spark_type_import(type_name: str) -> str:
    """Return the single-name import statement for a ``pyspark.sql.types`` name."""
    return f"from pyspark.sql.types import {type_name}"

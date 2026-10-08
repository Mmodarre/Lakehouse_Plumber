"""Tests for the shared pyspark.sql.types name list and import form."""

from lhp.core.codegen.imports import SPARK_TYPE_NAMES, spark_type_import


def test_spark_type_import_is_a_single_name_statement():
    assert spark_type_import("LongType") == "from pyspark.sql.types import LongType"


def test_type_names_are_unique():
    assert len(set(SPARK_TYPE_NAMES)) == len(SPARK_TYPE_NAMES)

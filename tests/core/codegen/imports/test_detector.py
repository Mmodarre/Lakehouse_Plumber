"""Tests for ImportDetector's pyspark.sql.types resolution."""

import pytest

from lhp.core.codegen.imports import (
    SPARK_TYPE_NAMES,
    ImportDetector,
    spark_type_import,
)

_F_IMPORT = "from pyspark.sql import functions as F"


@pytest.mark.parametrize("strategy", ["ast", "regex"])
def test_long_type_cast_is_detected(strategy):
    detected = ImportDetector(strategy=strategy).detect_imports(
        'F.col("id").cast(LongType())'
    )

    assert detected == {_F_IMPORT, "from pyspark.sql.types import LongType"}


@pytest.mark.parametrize("strategy", ["ast", "regex"])
def test_decimal_type_with_arguments_is_detected(strategy):
    detected = ImportDetector(strategy=strategy).detect_imports(
        'F.col("price").cast(DecimalType(10, 2))'
    )

    assert detected == {_F_IMPORT, "from pyspark.sql.types import DecimalType"}


def test_regex_fallback_detects_decimal_type_in_unparseable_expression():
    detected = ImportDetector(strategy="ast").detect_imports(
        'F.col("price").cast(DecimalType(10, 2)) +'
    )

    assert "from pyspark.sql.types import DecimalType" in detected


@pytest.mark.parametrize("strategy", ["ast", "regex"])
@pytest.mark.parametrize("type_name", SPARK_TYPE_NAMES)
def test_every_shared_type_name_is_detected(strategy, type_name):
    detected = ImportDetector(strategy=strategy).detect_imports(f"{type_name}()")

    assert detected == {spark_type_import(type_name)}

"""Tests for Spark column-reference quoting (issue #324)."""

import pytest

from lhp.utils.spark_identifiers import backtick_quote, spark_column_ref

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("customer name", "`customer name`"),
        ("plain", "`plain`"),
        ("tick`name", "`tick``name`"),
        ("``", "``````"),
    ],
)
def test_backtick_quote_wraps_and_doubles_inner_backticks(name, expected):
    assert backtick_quote(name) == expected


@pytest.mark.parametrize(
    "name",
    [
        "c_custkey",
        "$amount",
        "customer name",
        " padded ",
        "first-name",
        "k:v",
        'dq"name',
        "x -> y",
    ],
)
def test_names_spark_resolves_as_written_are_unchanged(name):
    assert spark_column_ref(name) == name


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("a.b", "`a.b`"),
        ("a.*", "`a.*`"),
        ("*", "`*`"),
        ("tick`name", "`tick``name`"),
        ("`", "````"),
    ],
)
def test_names_spark_would_parse_are_backtick_quoted(name, expected):
    assert spark_column_ref(name) == expected

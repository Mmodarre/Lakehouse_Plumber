"""Tests for :mod:`lhp.utils.identifiers`.

``replace_non_identifier_chars`` keeps exactly the behaviour of the regex the
data-quality and cloudfiles generators used inline: every character outside
``[a-zA-Z0-9_]`` becomes ``_``, one for one, and nothing else changes. It does
not make the result a valid Python identifier (a leading digit or a keyword is
kept), because changing that would rename users' generated quarantine flows.
"""

import re

import pytest

from lhp.utils.identifiers import replace_non_identifier_chars

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("v_orders_raw", "v_orders_raw"),
        ("cat.schema.table", "cat_schema_table"),
        ("my-view name", "my_view_name"),
        ("a..b", "a__b"),
        ("café", "caf_"),
        ("", ""),
    ],
)
def test_replaces_each_non_identifier_character_with_underscore(value, expected):
    assert replace_non_identifier_chars(value) == expected


@pytest.mark.parametrize("value", ["1st_view", "class", "lambda"])
def test_leading_digits_and_keywords_are_kept(value):
    assert replace_non_identifier_chars(value) == value


def test_matches_the_regex_it_replaces():
    samples = ["${catalog}.bronze.orders", "x-y z/1", "ÄÖÜ_ok", "tab\there"]
    for value in samples:
        assert replace_non_identifier_chars(value) == re.sub(
            r"[^a-zA-Z0-9_]", "_", value
        )

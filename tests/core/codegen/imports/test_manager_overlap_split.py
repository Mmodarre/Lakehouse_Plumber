"""Overlapping ``from`` imports must not bind one name twice (issue #299).

Generators register imports as whole lines, and two lines from the same module
can share a name: the quarantine data-quality generator adds ``from
pyspark.sql.types import MapType, StringType`` while a schema load or an
operational-metadata expression adds ``from pyspark.sql.types import
StringType``. Exact-string dedup kept both lines, so the generated module bound
``StringType`` twice (ruff F811). The manager now splits only the lines that
share a binding into one line per name; lines with no shared binding stay
byte-identical.
"""

import ast

import pytest

from lhp.core.codegen.imports import ImportManager

pytestmark = pytest.mark.unit


def _resolve(*imports: str) -> list[str]:
    manager = ImportManager()
    for statement in imports:
        manager.add_import(statement)
    return manager.get_consolidated_imports()


def _binding_counts(import_block: list[str]) -> dict[str, int]:
    """How many times each local name is bound by ``from`` imports."""
    counts: dict[str, int] = {}
    for statement in import_block:
        for node in ast.walk(ast.parse(statement)):
            if isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name != "*":
                        name = alias.asname or alias.name
                        counts[name] = counts.get(name, 0) + 1
    return counts


def test_full_overlap_is_split_into_one_line_per_name():
    resolved = _resolve(
        "from pyspark.sql.types import MapType, StringType",
        "from pyspark.sql.types import StringType",
    )

    assert resolved == [
        "from pyspark.sql.types import MapType",
        "from pyspark.sql.types import StringType",
    ]


def test_partial_overlap_splits_both_lines():
    resolved = _resolve("from mod import A, B", "from mod import B, C")

    assert resolved == [
        "from mod import A",
        "from mod import B",
        "from mod import C",
    ]


def test_aliased_overlap_keeps_the_alias():
    resolved = _resolve("from mod import A as X, B", "from mod import A as X")

    assert resolved == ["from mod import A as X", "from mod import B"]


def test_redundant_alias_counts_as_the_same_binding():
    resolved = _resolve("from mod import A as A, B", "from mod import A")

    assert resolved == ["from mod import A", "from mod import B"]


def test_same_name_with_different_alias_is_not_an_overlap():
    resolved = _resolve("from mod import A, B", "from mod import A as X")

    assert resolved == ["from mod import A as X", "from mod import A, B"]


def test_lines_without_overlap_stay_byte_identical():
    resolved = _resolve(
        "from pyspark.sql.types import MapType, StringType",
        "from pyspark.sql.types import LongType",
        "from pyspark.sql import functions as F",
    )

    assert "from pyspark.sql.types import MapType, StringType" in resolved
    assert "from pyspark.sql.types import LongType" in resolved
    assert "from pyspark.sql import functions as F" in resolved


def test_only_the_overlapping_lines_of_a_module_are_split():
    resolved = _resolve(
        "from mod import A, B",
        "from mod import B",
        "from mod import C, D",
    )

    assert resolved == [
        "from mod import A",
        "from mod import B",
        "from mod import C, D",
    ]


def test_same_name_from_different_modules_is_not_merged():
    resolved = _resolve("from one import A, B", "from two import C, A as Z")

    assert resolved == ["from one import A, B", "from two import C, A as Z"]


def test_parenthesised_user_import_overlap_is_split():
    """``additional_imports`` from lhp.yaml arrive verbatim, in any valid form."""
    resolved = _resolve(
        "from pyspark.sql.types import (MapType, StringType)",
        "from pyspark.sql.types import StringType",
    )

    assert resolved == [
        "from pyspark.sql.types import MapType",
        "from pyspark.sql.types import StringType",
    ]


def test_wildcard_still_supersedes_specific_names():
    resolved = _resolve(
        "from mod import *",
        "from mod import A, B",
        "from mod import B",
    )

    assert resolved == ["from mod import *"]


def test_plain_imports_are_untouched():
    resolved = _resolve("import os", "import os, sys", "import json as j")

    assert sorted(resolved) == ["import json as j", "import os", "import os, sys"]


def test_unparseable_line_is_left_alone():
    resolved = _resolve("from mod import A, B", "from mod import B as")

    assert "from mod import A, B" in resolved
    assert "from mod import B as" in resolved


def test_no_name_is_bound_twice_for_the_quarantine_and_schema_import_set():
    """The import set a cloudfiles schema load plus a quarantine action produce."""
    resolved = _resolve(
        "from pyspark import pipelines as dp",
        "from delta.tables import DeltaTable",
        "from pyspark.sql import functions as F",
        "from pyspark.sql.types import MapType, StringType",
        "from pyspark.sql.window import Window",
        "from pyspark.sql.types import StructType",
        "from pyspark.sql.types import StructField",
        "from pyspark.sql.types import LongType",
        "from pyspark.sql.types import StringType",
    )

    assert all(count == 1 for count in _binding_counts(resolved).values())
    assert [line for line in resolved if line.startswith("from pyspark.sql.types")] == [
        f"from pyspark.sql.types import {name}"
        for name in ("LongType", "MapType", "StringType", "StructField", "StructType")
    ]


def test_split_is_idempotent_across_managers():
    """The assembler feeds each generator's consolidated imports into a new manager."""
    first = _resolve(
        "from pyspark.sql.types import MapType, StringType",
        "from pyspark.sql.types import StringType",
    )

    assert _resolve(*first) == first

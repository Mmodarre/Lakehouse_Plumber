"""Isolated unit tests for the wildcard-import expression-adaptation path of
:class:`OperationalMetadataCatalog`.

Covers ``adapt_expressions_for_imports`` and ``_adapt_expression_for_wildcard``
in ``lhp/core/codegen/operational_metadata/metadata.py``. This ~95-line branch
rewrites ``F.func()`` expressions to bare ``func()`` calls when a
``from pyspark.sql.functions import *`` wildcard is present, and otherwise
leaves the ``F.`` prefix intact. Per LOCAL/TEST_AUDIT.md (P0-2) this path was
exercised by zero tests; a regression here produces ``NameError`` in generated
pipelines.

These tests are fully isolated: they construct the catalog directly with an
in-memory project config and a real ``ImportManager``, with no pipeline
generation.
"""

import pytest

from lhp.core.codegen.imports.manager import ImportManager
from lhp.core.codegen.operational_metadata.metadata import (
    OperationalMetadataCatalog,
)
from lhp.models import (
    MetadataColumnConfig,
    ProjectOperationalMetadataConfig,
)

WILDCARD_IMPORT = "from pyspark.sql.functions import *"


def _wildcard_manager() -> ImportManager:
    im = ImportManager()
    im.add_import(WILDCARD_IMPORT)
    return im


def _f_prefix_manager() -> ImportManager:
    """An ImportManager that imports functions as F (no wildcard)."""
    im = ImportManager()
    im.add_import("import pyspark.sql.functions as F")
    return im


# ---------------------------------------------------------------------------
# adapt_expressions_for_imports — wildcard-present branch
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_wildcard_present_rewrites_default_columns_to_bare_calls():
    """With a wildcard import, default column expressions drop the F. prefix."""
    catalog = OperationalMetadataCatalog()
    assert (
        catalog.default_columns["_ingestion_timestamp"].expression
        == "F.current_timestamp()"
    )

    catalog.adapt_expressions_for_imports(_wildcard_manager())

    assert (
        catalog.default_columns["_ingestion_timestamp"].expression
        == "current_timestamp()"
    )
    assert catalog.default_columns["_source_file"].expression == "input_file_name()"
    # lit-based expressions also lose the F. prefix.
    assert catalog.default_columns["_pipeline_name"].expression.startswith("lit(")
    assert "F." not in catalog.default_columns["_pipeline_name"].expression


@pytest.mark.unit
def test_wildcard_present_adapts_project_custom_columns_into_copy():
    """Project custom columns are adapted into ``_adapted_project_columns``."""
    proj = ProjectOperationalMetadataConfig(
        columns={
            "_custom": MetadataColumnConfig(
                expression="F.col('x')", applies_to=["view"]
            )
        }
    )
    catalog = OperationalMetadataCatalog(project_config=proj)

    catalog.adapt_expressions_for_imports(_wildcard_manager())

    assert catalog._adapted_project_columns is not None
    assert catalog._adapted_project_columns["_custom"].expression == "col('x')"


# ---------------------------------------------------------------------------
# adapt_expressions_for_imports — no-wildcard branch
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_no_wildcard_leaves_default_expressions_with_f_prefix():
    """Without a wildcard import the F. prefix is retained and the adapted
    project map is reset to None."""
    catalog = OperationalMetadataCatalog()

    catalog.adapt_expressions_for_imports(_f_prefix_manager())

    assert (
        catalog.default_columns["_ingestion_timestamp"].expression
        == "F.current_timestamp()"
    )
    assert catalog.default_columns["_source_file"].expression == "F.input_file_name()"
    assert catalog._adapted_project_columns is None


@pytest.mark.unit
def test_no_import_manager_is_a_noop():
    """Calling with no import_manager leaves everything untouched and does not
    create the adapted-columns attribute."""
    catalog = OperationalMetadataCatalog()

    catalog.adapt_expressions_for_imports(None)

    assert (
        catalog.default_columns["_ingestion_timestamp"].expression
        == "F.current_timestamp()"
    )
    # Early return before _adapted_project_columns is ever assigned.
    assert not hasattr(catalog, "_adapted_project_columns")


# ---------------------------------------------------------------------------
# No mutation of the shared project_config (copy semantics)
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_project_config_is_not_mutated_by_adaptation():
    """Adaptation must operate on a copy: the original shared project config
    object's expressions are left intact, and the adapted config is a distinct
    object."""
    original_expr = "F.col('x')"
    original_config = MetadataColumnConfig(
        expression=original_expr, applies_to=["view"]
    )
    proj = ProjectOperationalMetadataConfig(columns={"_custom": original_config})
    catalog = OperationalMetadataCatalog(project_config=proj)

    catalog.adapt_expressions_for_imports(_wildcard_manager())

    # The shared project config and its column object are untouched.
    assert proj.columns["_custom"].expression == original_expr
    assert proj.columns["_custom"] is original_config
    # The adapted entry is a *different* object holding the rewritten expression.
    assert catalog._adapted_project_columns["_custom"] is not original_config
    assert catalog._adapted_project_columns["_custom"].expression == "col('x')"


@pytest.mark.unit
def test_unchanged_project_expression_reuses_original_config_object():
    """When an expression needs no rewrite, the adapted map reuses the original
    config object (documented copy-only-when-changed semantics)."""
    proj = ProjectOperationalMetadataConfig(
        columns={
            "_static": MetadataColumnConfig(
                expression="'literal_value'", applies_to=["view"]
            )
        }
    )
    catalog = OperationalMetadataCatalog(project_config=proj)

    catalog.adapt_expressions_for_imports(_wildcard_manager())

    # No F. token to rewrite -> same object is reused, value unchanged.
    assert catalog._adapted_project_columns["_static"] is proj.columns["_static"]
    assert catalog._adapted_project_columns["_static"].expression == "'literal_value'"


# ---------------------------------------------------------------------------
# _adapt_expression_for_wildcard — spot-check the regex table
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    ("expression", "expected"),
    [
        ("F.current_timestamp()", "current_timestamp()"),
        ("F.input_file_name()", "input_file_name()"),
        ("F.col('name')", "col('name')"),
        ("F.lit('value')", "lit('value')"),
        ("F.when(c, 1)", "when(c, 1)"),
        ("F.coalesce(a, b)", "coalesce(a, b)"),
        ("F.concat(a, b)", "concat(a, b)"),
        ("F.upper(x)", "upper(x)"),
        ("F.lower(x)", "lower(x)"),
        ("F.regexp_replace(x, 'a', 'b')", "regexp_replace(x, 'a', 'b')"),
        ("F.to_timestamp(x)", "to_timestamp(x)"),
        ("F.row_number()", "row_number()"),
        ("F.dense_rank()", "dense_rank()"),
        ("F.sum(x)", "sum(x)"),
    ],
)
def test_adapt_expression_strips_f_prefix_for_known_functions(expression, expected):
    """Each known function in the adaptation table has its F. prefix removed."""
    catalog = OperationalMetadataCatalog()
    assert catalog._adapt_expression_for_wildcard(expression) == expected


@pytest.mark.unit
def test_adapt_expression_rewrites_multiple_calls_in_one_expression():
    """A compound expression rewrites every recognized function call."""
    catalog = OperationalMetadataCatalog()
    result = catalog._adapt_expression_for_wildcard(
        "F.coalesce(F.col('a'), F.lit('x'))"
    )
    assert result == "coalesce(col('a'), lit('x'))"


@pytest.mark.unit
def test_adapt_expression_leaves_already_bare_calls_untouched():
    """An expression already using bare calls is returned unchanged (idempotent
    for the no-prefix case)."""
    catalog = OperationalMetadataCatalog()
    assert catalog._adapt_expression_for_wildcard("col('a')") == "col('a')"


@pytest.mark.unit
def test_adapt_expression_does_not_touch_unrelated_substrings():
    """The word-boundary anchors prevent rewriting columns/identifiers that
    merely contain a function name (e.g. ``my_col``) or a different namespace
    (``DF.col``)."""
    catalog = OperationalMetadataCatalog()
    # No bare 'F.' token, so nothing should change.
    assert catalog._adapt_expression_for_wildcard("DF.col('a')") == "DF.col('a')"
    assert catalog._adapt_expression_for_wildcard("F.col(my_col)") == "col(my_col)"

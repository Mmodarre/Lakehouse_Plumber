"""Negative + positive suite for ``TestActionValidator``.

Error strings asserted here are copied verbatim from the validator body
(``src/lhp/core/validators/action/test.py``) — they are load-bearing and must
stay in sync with that source.
"""

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, TestActionValidator
from lhp.models import Action

PREFIX = "Action[0] 'test_action'"


def _make_validator() -> TestActionValidator:
    return TestActionValidator(ActionRegistry(), ConfigFieldValidator())


def _errors_for(**kwargs) -> list:
    """Build a test Action from kwargs and return the validator's error list."""
    validator = _make_validator()
    action = Action(name="test_action", type="test", **kwargs)
    return validator.validate(action, PREFIX)


def _assert_has(errors: list, substring: str) -> None:
    assert any(substring in e for e in errors), (
        f"Expected an error containing {substring!r}; got: {errors}"
    )


def test_invalid_test_type_rejected():
    errors = _errors_for(test_type="not_a_real_type", source="x")
    _assert_has(errors, "Invalid test_type 'not_a_real_type'. Valid values are:")


def test_row_count_source_missing():
    errors = _errors_for(test_type="row_count")
    _assert_has(errors, "Row count test requires 'source' field")


def test_row_count_source_not_a_list():
    errors = _errors_for(test_type="row_count", source="single_table")
    _assert_has(errors, "Row count test requires source to be a list of two tables")


def test_row_count_source_not_exactly_two():
    errors = _errors_for(test_type="row_count", source=["a", "b", "c"])
    _assert_has(errors, "Row count test requires exactly 2 sources to compare, got 3")


def test_uniqueness_source_missing():
    errors = _errors_for(test_type="uniqueness", columns=["id"])
    _assert_has(errors, "Uniqueness test requires 'source' field")


def test_uniqueness_columns_missing():
    errors = _errors_for(test_type="uniqueness", source="v_table")
    _assert_has(
        errors,
        "Uniqueness test requires 'columns' field specifying which columns to check",
    )


def test_referential_integrity_source_missing():
    errors = _errors_for(
        test_type="referential_integrity",
        reference="cat.sch.customers",
        source_columns=["customer_id"],
        reference_columns=["id"],
    )
    _assert_has(errors, "Referential integrity test requires 'source' field")


def test_referential_integrity_reference_missing():
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        source_columns=["customer_id"],
        reference_columns=["id"],
    )
    _assert_has(errors, "Referential integrity test requires 'reference' field")


def test_referential_integrity_source_columns_missing():
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="cat.sch.customers",
        reference_columns=["id"],
    )
    _assert_has(errors, "Referential integrity test requires 'source_columns' field")


def test_referential_integrity_reference_columns_missing():
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="cat.sch.customers",
        source_columns=["customer_id"],
    )
    _assert_has(errors, "Referential integrity test requires 'reference_columns' field")


def test_completeness_source_missing():
    errors = _errors_for(test_type="completeness", required_columns=["id"])
    _assert_has(errors, "Completeness test requires 'source' field")


def test_completeness_required_columns_missing():
    errors = _errors_for(test_type="completeness", source="v_table")
    _assert_has(errors, "Completeness test requires 'required_columns' field")


def test_range_source_missing():
    errors = _errors_for(test_type="range", column="value", min_value=0)
    _assert_has(errors, "Range test requires 'source' field")


def test_range_column_missing():
    errors = _errors_for(test_type="range", source="v_table", min_value=0)
    _assert_has(errors, "Range test requires 'column' field")


def test_range_no_bounds():
    errors = _errors_for(test_type="range", source="v_table", column="value")
    _assert_has(
        errors, "Range test requires at least one of 'min_value' or 'max_value'"
    )


def test_schema_match_source_missing():
    errors = _errors_for(test_type="schema_match", reference="cat.sch.table2")
    _assert_has(errors, "Schema match test requires 'source' field")


def test_schema_match_reference_missing():
    errors = _errors_for(test_type="schema_match", source="cat.sch.table1")
    _assert_has(
        errors, "Schema match test requires 'reference' field to compare schemas"
    )


def test_all_lookups_found_source_missing():
    errors = _errors_for(
        test_type="all_lookups_found",
        lookup_table="cat.sch.dim",
        lookup_columns=["id"],
        lookup_result_columns=["sk"],
    )
    _assert_has(errors, "All lookups found test requires 'source' field")


def test_all_lookups_found_lookup_table_missing():
    errors = _errors_for(
        test_type="all_lookups_found",
        source="v_fact",
        lookup_columns=["id"],
        lookup_result_columns=["sk"],
    )
    _assert_has(errors, "All lookups found test requires 'lookup_table' field")


def test_all_lookups_found_lookup_columns_missing():
    errors = _errors_for(
        test_type="all_lookups_found",
        source="v_fact",
        lookup_table="cat.sch.dim",
        lookup_result_columns=["sk"],
    )
    _assert_has(errors, "All lookups found test requires 'lookup_columns' field")


def test_all_lookups_found_lookup_result_columns_missing():
    errors = _errors_for(
        test_type="all_lookups_found",
        source="v_fact",
        lookup_table="cat.sch.dim",
        lookup_columns=["id"],
    )
    _assert_has(errors, "All lookups found test requires 'lookup_result_columns' field")


def test_custom_sql_neither_source_nor_sql():
    # With neither source nor sql, BOTH the "either source or sql" guard and
    # the unconditional "requires 'sql'" check fire.
    errors = _errors_for(test_type="custom_sql")
    _assert_has(errors, "Custom SQL test requires either 'source' or 'sql' field")


def test_custom_sql_sql_missing():
    errors = _errors_for(test_type="custom_sql", source="v_table")
    _assert_has(errors, "Custom SQL test requires 'sql' field with the query")
    # The "either source or sql" guard must NOT fire when source is present.
    assert not any(
        "Custom SQL test requires either 'source' or 'sql' field" in e for e in errors
    ), f"'either source or sql' should not fire when source is present; got: {errors}"


def test_custom_expectations_source_missing():
    errors = _errors_for(
        test_type="custom_expectations",
        expectations=[{"name": "t", "expression": "true"}],
    )
    _assert_has(errors, "Custom expectations test requires 'source' field")


def test_custom_expectations_expectations_missing():
    errors = _errors_for(test_type="custom_expectations", source="v_table")
    _assert_has(errors, "Custom expectations test requires 'expectations' field")


def test_referential_integrity_reference_not_3part_rejected():
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",
        reference="customers",  # not 3-part
        source_columns=["customer_id"],
        reference_columns=["id"],
    )
    _assert_has(errors, "'reference' must be a 3-part name")


def test_all_lookups_found_lookup_table_not_3part_rejected():
    errors = _errors_for(
        test_type="all_lookups_found",
        source="v_fact",
        lookup_table="dim",  # not 3-part
        lookup_columns=["id"],
        lookup_result_columns=["sk"],
    )
    _assert_has(errors, "'lookup_table' must be a 3-part name")


def test_schema_match_reference_not_3part_rejected():
    errors = _errors_for(
        test_type="schema_match",
        source="cat.sch.table1",
        reference="table2",  # not 3-part
    )
    _assert_has(errors, "'reference' must be a 3-part name")


def test_schema_match_source_not_3part_rejected():
    errors = _errors_for(
        test_type="schema_match",
        source="table1",  # not 3-part
        reference="cat.sch.table2",
    )
    _assert_has(errors, "'source' must be a 3-part name")


# POSITIVE tests: bare-name source accepted (false-confidence closure).
# These prove the validator ACCEPTS bare 1-part in-pipeline relation names as
# ``source`` for every type where ``source`` is NOT required to be 3-part.
# They would FAIL if a blanket ``source`` 3-part rule were ever added —
# locking in the owner's decision.


def test_positive_row_count_bare_sources():
    errors = _errors_for(test_type="row_count", source=["v_left", "v_right"])
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_uniqueness_bare_source():
    errors = _errors_for(test_type="uniqueness", source="v_my_view", columns=["id"])
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_completeness_bare_source():
    errors = _errors_for(
        test_type="completeness", source="v_my_view", required_columns=["id", "name"]
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_range_bare_source():
    errors = _errors_for(
        test_type="range",
        source="v_my_view",
        column="value",
        min_value=0,
        max_value=100,
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_custom_sql_bare_source():
    errors = _errors_for(
        test_type="custom_sql", source="v_my_view", sql="SELECT * FROM v_my_view"
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_custom_expectations_bare_source():
    errors = _errors_for(
        test_type="custom_expectations",
        source="v_my_view",
        expectations=[{"name": "t", "expression": "true"}],
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_referential_integrity_bare_source_3part_reference():
    errors = _errors_for(
        test_type="referential_integrity",
        source="v_orders",  # bare, accepted
        reference="cat.sch.customers",  # 3-part required
        source_columns=["customer_id"],
        reference_columns=["id"],
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_all_lookups_found_bare_source_3part_lookup_table():
    errors = _errors_for(
        test_type="all_lookups_found",
        source="v_fact",  # bare, accepted
        lookup_table="cat.sch.dim",  # 3-part required
        lookup_columns=["id"],
        lookup_result_columns=["sk"],
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


def test_positive_schema_match_3part_source_and_reference():
    errors = _errors_for(
        test_type="schema_match",
        source="cat.sch.table1",  # 3-part required for schema_match
        reference="cat.sch.table2",  # 3-part required
    )
    assert errors == [], f"Expected zero errors; got: {errors}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

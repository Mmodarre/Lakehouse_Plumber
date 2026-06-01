"""Generator-layer behaviour for test actions: genuine errors + edge-case SQL.

Scope and division of labour
----------------------------
Input *validation* (required-field presence, 3-part-name rules, invalid
``test_type``) is owned by ``TestActionValidator`` and is covered
authoritatively by ``tests/core/validators/action/test_test_action.py``.
Generators do NOT validate — so a generator test that feeds invalid/empty
input and then asserts merely that ``def tmp_test_...`` exists asserts
nothing meaningful and duplicates the validator suite. Those false-confidence
tests have been retired.

What remains here is strictly generator-layer behaviour:

* Errors the *generator* itself raises (registry lookup, schema_match's
  strict 3-part contract) — asserting the real error, not that code exists.
* SQL-correctness for inputs that are VALID but exercise an edge branch
  (one-sided range bounds, ``on_violation`` normalization, zip-truncated /
  self-referential join conditions). These pin the rendered SQL so the
  generator path stays covered.
"""

import pytest

from lhp.core.registry import ActionRegistry
from lhp.errors import LHPError, LHPValidationError
from lhp.models import Action, ActionType

# Each test type resolves to its own stateless leaf generator. These tests
# obtain the correct leaf via the registry (keyed by the action's test_type)
# and drive it with the stateless ``generate(action, context)`` contract.
registry = ActionRegistry()


def _generate(action):
    """Resolve the leaf for this action's test_type and generate its code."""
    generator = registry.get_generator(ActionType.TEST, action.test_type)
    return generator.generate(action=action, context={})


class TestTestActionGeneratorErrors:
    """Errors raised by the generator/registry layer itself."""

    def test_invalid_test_type_in_registry(self):
        """An unknown test_type fails fast at registry lookup with a helpful error."""
        with pytest.raises(LHPError) as excinfo:
            registry.get_generator(ActionType.TEST, "invalid_test_type")

        assert "test_type" in str(excinfo.value)
        assert "invalid_test_type" in str(excinfo.value)
        # Should suggest valid test types
        assert "row_count" in str(excinfo.value) or "Valid test_type" in str(
            excinfo.value
        )

    def test_schema_match_without_reference_raises_val_022(self):
        """schema_match without a valid 3-part reference must raise LHP-VAL-022.

        Previously the generator silently emitted SQL that did not match
        anything in information_schema; the contract is now strict, so this
        is generator-layer behaviour (a raised error), not a "code exists"
        assertion.
        """
        action = Action(
            name="test_schema",
            type=ActionType.TEST,
            test_type="schema_match",
            source="table1",
            # Missing 'reference' field
        )

        with pytest.raises(LHPValidationError) as exc_info:
            _generate(action)
        assert "LHP-VAL-022" in str(exc_info.value)

    def test_schema_match_empty_source_list_raises_val_022(self):
        """An empty-list source collapses to "" and must raise LHP-VAL-022.

        ``isinstance([], list)`` is True, so ``source[0] if source else ""``
        takes the else branch (schema_match.py:39) and yields ``""``; the
        empty string then fails the strict 3-part FQN check and raises
        LHP-VAL-022 for the ``source`` field.
        """
        action = Action(
            name="test_schema",
            type=ActionType.TEST,
            test_type="schema_match",
            source=[],
            reference="c.s.t",
        )

        with pytest.raises(LHPValidationError) as exc_info:
            _generate(action)
        assert "LHP-VAL-022" in str(exc_info.value)
        assert "source" in str(exc_info.value)

    def test_schema_match_backticked_source_raises_val_022(self):
        """A backticked source FQN short-circuits the split and raises VAL-022.

        ``invalid`` starts True because the value contains a backtick, so the
        ``if not invalid:`` guard (schema_match.py:81->85) skips the split /
        length check entirely and jumps straight to the LHP-VAL-022 raise.
        """
        action = Action(
            name="test_schema",
            type=ActionType.TEST,
            test_type="schema_match",
            source="cat.`sch`.tbl",
            reference="c.s.t",
        )

        with pytest.raises(LHPValidationError) as exc_info:
            _generate(action)
        assert "LHP-VAL-022" in str(exc_info.value)


class TestTestActionEdgeCaseSql:
    """SQL correctness for valid-but-edge-case inputs (generator-layer coverage)."""

    def test_custom_sql_missing_sql_falls_back_to_source_table(self):
        """When ``sql`` is omitted, custom_sql emits ``spark.table(<source>)``.

        This documented fallback (byte-matching the pre-Phase-9.3 generator)
        is the generator's behaviour for a source-only custom_sql action.
        Required-field rejection is the validator's job (B8); here we pin the
        rendered SQL the generator actually produces.
        """
        action = Action(
            name="test_custom",
            type=ActionType.TEST,
            test_type="custom_sql",
            source="test_table",
        )

        code = _generate(action)

        # The fallback reads the source as a table — not a spark.sql() block.
        assert 'return spark.table("test_table")' in code
        assert "spark.sql(" not in code

    def test_range_lower_bound_only_emits_only_ge_predicate(self):
        """A range test with only ``min_value`` emits a single ``>=`` predicate."""
        action = Action(
            name="test_range",
            type=ActionType.TEST,
            test_type="range",
            source="test_table",
            column="value",
            min_value=0,
            # max_value is None
        )

        code = _generate(action)

        assert '@dp.expect_all_or_fail({"value_in_range": "value >= \'0\'"})' in code
        # No upper-bound predicate when max_value is absent.
        assert "<=" not in code

    def test_range_upper_bound_only_emits_only_le_predicate(self):
        """A range test with only ``max_value`` emits a single ``<=`` predicate."""
        action = Action(
            name="test_range",
            type=ActionType.TEST,
            test_type="range",
            source="test_table",
            column="value",
            max_value=100,
            # min_value is None
        )

        code = _generate(action)

        assert '@dp.expect_all_or_fail({"value_in_range": "value <= \'100\'"})' in code
        # No lower-bound predicate when min_value is absent.
        assert ">=" not in code

    def test_unknown_on_violation_normalizes_to_fail(self):
        """An unrecognised ``on_violation`` value is normalized to ``fail``.

        ``_normalize_on_violation`` coerces anything outside {fail,warn,drop}
        to ``fail``, so the expectation must route through the fail bucket
        (``@dp.expect_all_or_fail``), never warn or drop.
        """
        action = Action(
            name="test_violation",
            type=ActionType.TEST,
            test_type="row_count",
            source=["source", "target"],
            on_violation="invalid",  # not one of fail/warn/drop
        )

        code = _generate(action)

        assert (
            '@dp.expect_all_or_fail({"row_count_match": '
            '"abs(source_count - target_count) <= 0"})' in code
        )
        assert "@dp.expect_all_or_drop(" not in code
        assert "@dp.expect_all(" not in code

    def test_on_violation_drop_routes_to_drop_bucket(self):
        """``on_violation="drop"`` routes the expectation through the DROP bucket.

        ``_bucket_expectations`` puts a ``drop`` expectation in the drop dict
        (``_base.py:97``), which the leaf renders as ``@dp.expect_all_or_drop``
        — never the fail or warn decorators.
        """
        action = Action(
            name="test_drop",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="t",
            columns=["id"],
            on_violation="drop",
        )

        code = _generate(action)

        assert (
            '@dp.expect_all_or_drop({"no_duplicates": "duplicate_count == 0"})' in code
        )
        assert "@dp.expect_all_or_fail(" not in code
        assert "@dp.expect_all(" not in code

    def test_on_violation_warn_routes_to_warn_bucket(self):
        """A ``warn`` expectation routes through the WARN bucket; the warn
        ``elif`` test loops back for a non-matching follower.

        ``_bucket_expectations`` (``_base.py``) reads each expectation's RAW
        ``on_violation`` (no normalization at this layer). A ``warn`` value
        lands in the warn dict (``@dp.expect_all``). A following expectation
        whose ``on_violation`` matches none of fail/drop/warn fails the warn
        ``elif`` test and loops back to the next element (the ``98->90``
        loop-back arc): it is silently dropped from every bucket. The warn
        expectation must render under ``@dp.expect_all`` only — never fail or
        drop — and the fall-through expectation must not appear at all.
        """
        action = Action(
            name="test_warn",
            type=ActionType.TEST,
            test_type="custom_expectations",
            source="t",
            expectations=[
                {"name": "w1", "expression": "x > 0", "on_violation": "warn"},
                # Unrecognised on_violation: falls through fail/drop/warn and
                # loops back (98->90); not placed in any bucket.
                {"name": "ignored", "expression": "y > 0", "on_violation": "skip"},
            ],
        )

        code = _generate(action)

        assert '@dp.expect_all({"w1": "x > 0"})' in code
        assert "@dp.expect_all_or_fail(" not in code
        assert "@dp.expect_all_or_drop(" not in code
        # The fall-through expectation is silently dropped from every bucket.
        assert "ignored" not in code
        assert "y > 0" not in code

    def test_referential_integrity_mismatched_column_counts_zip_truncates(self):
        """Mismatched source/reference column counts: the join zips to the shorter.

        With 2 source columns and 1 reference column, ``zip`` truncates to the
        single overlapping pair, emitting one join predicate (``s.order_id =
        r.id``); the surplus source column is dropped. Validation of the
        mismatch (if any) belongs to the validator layer — this pins the SQL
        the generator emits given the input.
        """
        action = Action(
            name="test_ref",
            type=ActionType.TEST,
            test_type="referential_integrity",
            source="orders",
            reference="customers",
            source_columns=["order_id", "customer_id"],  # 2 columns
            reference_columns=["id"],  # 1 column
        )

        code = _generate(action)

        assert "LEFT JOIN customers r ON s.order_id = r.id" in code
        # The first reference column drives the NOT-NULL expectation.
        assert (
            '@dp.expect_all_or_fail({"referential_integrity": "ref_id IS NOT NULL"})'
            in code
        )
        # The surplus source column is not part of the join condition.
        assert "customer_id" not in code

    def test_referential_integrity_self_reference_joins_table_to_itself(self):
        """A self-referencing referential_integrity test joins the table to itself."""
        action = Action(
            name="test_circular",
            type=ActionType.TEST,
            test_type="referential_integrity",
            source="table_a",
            reference="table_a",  # self-reference
            source_columns=["id"],
            reference_columns=["parent_id"],
        )

        code = _generate(action)

        assert "FROM table_a s" in code
        assert "LEFT JOIN table_a r ON s.id = r.parent_id" in code
        assert (
            '@dp.expect_all_or_fail({"referential_integrity": '
            '"ref_parent_id IS NOT NULL"})' in code
        )

    def test_all_lookups_found_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``.

        ``isinstance([], list)`` is True, so the ternary evaluates its else and
        substitutes the hard-coded fallback table name; the SQL ``FROM`` must
        read ``source_table``, not an empty/blank table reference.
        """
        action = Action(
            name="test_lookup",
            type=ActionType.TEST,
            test_type="all_lookups_found",
            source=[],  # empty list → else fork of the ternary
            lookup_table="dim_lookup",
            lookup_columns=["id"],
            lookup_result_columns=["name"],
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "FROM source_table s" in code
        # The lookup join still renders (proves the rest of the SQL is intact).
        assert "LEFT JOIN dim_lookup l ON s.id = l.id" in code

    def test_completeness_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``."""
        action = Action(
            name="test_complete",
            type=ActionType.TEST,
            test_type="completeness",
            source=[],  # empty list → else fork of the ternary
            required_columns=["x"],
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "FROM source_table" in code
        # The required column drives the projection (proves the SQL is intact).
        assert "SELECT x" in code

    def test_completeness_no_required_columns_selects_star_no_decorator(self):
        """No ``required_columns`` → ``SELECT *`` and no expectation decorator.

        The ``if required_cols:`` fork is false, so ``expectations`` stays empty
        and no ``@dp.expect*`` decorator is emitted; the projection defaults to
        ``*`` rather than a column list.
        """
        action = Action(
            name="test_complete_star",
            type=ActionType.TEST,
            test_type="completeness",
            source="test_table",
            # No required_columns → empty expectations, SELECT *
        )

        code = _generate(action)

        # The defensive empty-columns variant projects everything.
        assert "SELECT *" in code
        # No expectations built → no decorator of any kind.
        assert "@dp.expect" not in code

    def test_custom_expectations_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``."""
        action = Action(
            name="test_custom_exp",
            type=ActionType.TEST,
            test_type="custom_expectations",
            source=[],  # empty list → else fork of the ternary
            expectations=[{"name": "e", "expression": "x > 0", "on_violation": "fail"}],
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "SELECT * FROM source_table" in code
        # The user expectation still routes through the fail bucket.
        assert '@dp.expect_all_or_fail({"e": "x > 0"})' in code

    def test_custom_sql_empty_source_list_no_sql_falls_back_to_source_table(self):
        """An empty ``source=[]`` with no ``sql`` → ``spark.table("source_table")``.

        The fallback literal only reaches the template's ``{% else %}`` when
        ``sql`` is absent; the empty list takes the source ternary's else fork
        and the template emits a table read, not a ``spark.sql()`` block.
        """
        action = Action(
            name="test_custom_sql_empty",
            type=ActionType.TEST,
            test_type="custom_sql",
            source=[],  # empty list → else fork of the source ternary
            # No sql → template emits return spark.table(<source>)
        )

        code = _generate(action)

        # else fork fires: literal fallback table name read as a table.
        assert 'return spark.table("source_table")' in code
        # The no-sql path is taken, not the spark.sql() block.
        assert "spark.sql(" not in code

    def test_referential_integrity_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``."""
        action = Action(
            name="test_ref_empty",
            type=ActionType.TEST,
            test_type="referential_integrity",
            source=[],  # empty list → else fork of the ternary
            reference="customers",
            source_columns=["id"],
            reference_columns=["id"],
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "FROM source_table s" in code
        # The join still renders (proves the rest of the SQL is intact).
        assert "LEFT JOIN customers r ON s.id = r.id" in code

    def test_uniqueness_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``."""
        action = Action(
            name="test_unique_empty",
            type=ActionType.TEST,
            test_type="uniqueness",
            source=[],  # empty list → else fork of the ternary
            columns=["id"],
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "FROM source_table" in code
        # The group-by still renders the column (proves the SQL is intact).
        assert "GROUP BY id" in code
        assert "no_duplicates" in code

    def test_range_no_bounds_emits_no_expectation(self):
        """No ``min_value`` and no ``max_value`` → ``if expressions:`` is false.

        With neither bound set, ``expressions`` stays empty, no ``value_in_range``
        expectation is built and no ``@dp.expect`` decorator is rendered; the
        column is still projected.
        """
        action = Action(
            name="test_range_nobounds",
            type=ActionType.TEST,
            test_type="range",
            source="test_table",
            column="c",
            # No min_value, no max_value → empty expressions
        )

        code = _generate(action)

        # The no-expressions fork fires: nothing routed through any bucket.
        assert "value_in_range" not in code
        assert "@dp.expect" not in code
        # The column projection still renders (proves the SQL is intact).
        assert "SELECT c" in code

    def test_range_empty_source_list_falls_back_to_source_table(self):
        """An empty ``source=[]`` list takes the else fork → literal ``source_table``."""
        action = Action(
            name="test_range_empty",
            type=ActionType.TEST,
            test_type="range",
            source=[],  # empty list → else fork of the ternary
            column="c",
            min_value=0,
        )

        code = _generate(action)

        # else fork fires: literal fallback table name in the FROM clause.
        assert "FROM source_table" in code
        # The lower-bound predicate still renders (proves the SQL is intact).
        assert "c >= '0'" in code


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

import pytest

from lhp.core.registry import ActionRegistry
from lhp.models import Action, ActionType

# Each test type now resolves to its own stateless leaf generator. These tests
# obtain the correct leaf via the registry (keyed by the action's test_type) and
# drive it with the stateless ``generate(action, context)`` contract.
registry = ActionRegistry()


def _generate(action):
    generator = registry.get_generator(ActionType.TEST, action.test_type)
    return generator.generate(action=action, context={})


class TestTestActionOptimization:
    def test_completeness_selects_only_required_columns(self):
        action = Action(
            name="test_completeness",
            type=ActionType.TEST,
            test_type="completeness",
            source="test_table",
            required_columns=["col1", "col2", "col3"],
        )

        code = _generate(action)

        assert "SELECT col1, col2, col3" in code
        assert "SELECT *" not in code
        assert "FROM test_table" in code

    def test_range_selects_only_tested_column(self):
        action = Action(
            name="test_range",
            type=ActionType.TEST,
            test_type="range",
            source="test_table",
            column="date_column",
            min_value="2020-01-01",
            max_value="2024-12-31",
        )

        code = _generate(action)

        assert "SELECT date_column" in code
        assert "SELECT *" not in code
        assert "FROM test_table" in code

    def test_completeness_empty_columns_fallback(self):
        action = Action(
            name="test_completeness",
            type=ActionType.TEST,
            test_type="completeness",
            source="test_table",
            required_columns=[],  # Empty list
        )

        code = _generate(action)

        assert "SELECT *" in code

    def test_range_no_column_fallback(self):
        action = Action(
            name="test_range",
            type=ActionType.TEST,
            test_type="range",
            source="test_table",
            min_value=0,
            max_value=100,
            # No column specified
        )

        code = _generate(action)

        assert "SELECT *" in code

    def test_other_tests_unchanged(self):
        action = Action(
            name="test_row_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["source_table", "target_table"],
        )
        code = _generate(action)
        assert "COUNT(*)" in code

        action = Action(
            name="test_unique",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="test_table",
            columns=["id"],
        )
        code = _generate(action)
        assert "GROUP BY" in code
        assert "HAVING COUNT(*) > 1" in code

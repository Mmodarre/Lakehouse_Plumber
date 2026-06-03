from unittest.mock import Mock, patch

import pytest

from lhp.core.registry import BaseActionGenerator
from lhp.generators.test import RowCountTestGenerator
from lhp.models import Action, ActionType, TestActionType


class TestTestActionGenerator:
    def test_test_generator_inherits_base_generator(self):
        assert issubclass(RowCountTestGenerator, BaseActionGenerator)

        generator = RowCountTestGenerator()
        assert isinstance(generator, BaseActionGenerator)

    def test_generate_method_exists(self):
        generator = RowCountTestGenerator()

        action = Action(
            name="test_row_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_source", "v_target"],
        )

        result = generator.generate(action=action, context={})

        assert isinstance(result, str)
        assert len(result) > 0

    def test_row_count_expectation_rendered(self):
        """The row_count expectation (name, expression, fail routing) is rendered."""
        action = Action(
            name="test_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_source", "v_target"],
            on_violation="fail",
            tolerance=5,
        )

        code = RowCountTestGenerator().generate(action=action, context={})

        assert "row_count_match" in code
        assert "abs(source_count - target_count) <= 5" in code
        # on_violation == "fail" routes the expectation through the fail bucket.
        assert (
            '@dp.expect_all_or_fail({"row_count_match": '
            '"abs(source_count - target_count) <= 5"})' in code
        )


@pytest.mark.unit
class TestTestActionGoldenOutput:
    __test__ = True  # This IS a test class (override the name-based heuristic)

    def test_basic_test_action_golden(self, golden):
        generator = RowCountTestGenerator()
        action = Action(
            name="test_row_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_source", "v_target"],
        )
        code = generator.generate(action=action, context={})
        golden(code, "test_action")

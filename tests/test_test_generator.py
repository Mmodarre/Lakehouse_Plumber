"""Tests for TestActionGenerator class."""

from unittest.mock import Mock, patch

import pytest

from lhp.core.registry import BaseActionGenerator
from lhp.generators.test.test_generator import TestActionGenerator
from lhp.models import Action, ActionType, TestActionType


class TestTestActionGenerator:
    """Test the TestActionGenerator class."""

    def test_test_generator_exists(self):
        """Test that TestActionGenerator class exists and can be instantiated."""
        # Test that TestActionGenerator exists
        assert TestActionGenerator is not None

        # Test that we can create an instance
        config = {
            "name": "test_sample",
            "type": "test",
            "test_type": "row_count",
            "source": ["v_source", "v_target"],
        }
        context = {"pipeline": "test_pipeline"}

        generator = TestActionGenerator(config=config, context=context)
        assert generator is not None
        assert generator.config == config
        assert generator.context == context

    def test_test_generator_inherits_base_generator(self):
        """Test that TestActionGenerator inherits from BaseActionGenerator."""
        # Test inheritance
        assert issubclass(TestActionGenerator, BaseActionGenerator)

        # Create instance and verify it's also a BaseActionGenerator instance
        config = {"name": "test", "test_type": "row_count"}
        context = {}
        generator = TestActionGenerator(config=config, context=context)
        assert isinstance(generator, BaseActionGenerator)

    def test_generate_method_exists(self):
        """Test that generate() method exists and returns a string."""
        generator = TestActionGenerator()

        # Create a test action
        action = Action(
            name="test_row_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_source", "v_target"],
        )

        # Call generate method
        result = generator.generate(action=action, context={})

        # Should return a string
        assert isinstance(result, str)
        assert len(result) > 0

    def test_build_expectations_method(self):
        """Test that _build_expectations() creates proper expectations."""
        generator = TestActionGenerator(
            config={
                "name": "test_count",
                "test_type": "row_count",
                "source": ["v_source", "v_target"],
                "on_violation": "fail",
                "tolerance": 5,
            },
            context={},
        )

        # Build expectations for row_count
        expectations = generator._build_expectations("row_count")

        # Check expectations structure
        assert len(expectations) > 0
        assert expectations[0]["name"] == "row_count_match"
        assert "expression" in expectations[0]
        assert expectations[0]["on_violation"] == "fail"


@pytest.mark.unit
class TestTestActionGoldenOutput:
    """Golden output test for test action generator."""

    __test__ = True  # This IS a test class (override the name-based heuristic)

    def test_basic_test_action_golden(self, golden):
        generator = TestActionGenerator()
        action = Action(
            name="test_row_count",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_source", "v_target"],
        )
        code = generator.generate(action=action, context={})
        golden(code, "test_action")

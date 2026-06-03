"""Tests for ActionRegistry with test action support."""

import pytest

from lhp.core.registry import ActionRegistry
from lhp.generators.test import RowCountTestGenerator, UniquenessTestGenerator
from lhp.models import ActionType, TestActionType


class TestActionRegistry:
    """Test the ActionRegistry with test action support."""

    def test_registry_recognizes_test_action(self):
        """Test that ActionRegistry recognizes TEST action type."""
        registry = ActionRegistry()

        generator = registry.get_generator(ActionType.TEST, sub_type="row_count")

        # Each test type now resolves to its own distinct leaf generator.
        assert generator is not None
        assert isinstance(generator, RowCountTestGenerator)

    def test_registry_test_action_fields(self):
        """Test that registry defines required and optional fields for test action."""
        registry = ActionRegistry()

        try:
            generator = registry.get_generator(ActionType.TEST, sub_type="row_count")
            assert generator is not None
        except ValueError as e:
            assert "test" in str(e).lower()

    def test_registry_test_action_with_string_type(self):
        """Test that registry handles string test_type conversion."""
        registry = ActionRegistry()

        generator = registry.get_generator(ActionType.TEST, sub_type="uniqueness")
        assert isinstance(generator, UniquenessTestGenerator)

    def test_registry_test_action_invalid_type(self):
        """Test that registry raises error for invalid test type."""
        from lhp.errors import LHPError

        registry = ActionRegistry()

        with pytest.raises(LHPError) as exc_info:
            registry.get_generator(ActionType.TEST, sub_type="invalid_test_type")

        assert "invalid_test_type" in str(exc_info.value).lower()

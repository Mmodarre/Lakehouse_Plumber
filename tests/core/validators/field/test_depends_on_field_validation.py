"""Tests for ``depends_on`` shape validation in the field-validator path.

Mirrors ``tests/test_unknown_field_validation.py``: the
:class:`~lhp.core.validators.ConfigFieldValidator` field path RAISES an
``LHPError`` (``LHP-VAL-063``) for a malformed ``depends_on`` entry and passes
silently for well-formed / unset values.
"""

import pytest

from lhp.core.validators import ConfigFieldValidator
from lhp.errors import LHPError


class TestDependsOnFieldValidation:
    """Validate the optional ``depends_on`` escape-hatch field."""

    def setup_method(self):
        self.validator = ConfigFieldValidator()

    def _action(self, depends_on):
        return {"name": "act", "type": "transform", "depends_on": depends_on}

    def test_valid_three_part_reference_passes(self):
        self.validator.validate_action_fields(
            self._action(["my_catalog.my_schema.my_table"]), "act"
        )

    def test_valid_two_and_one_part_references_pass(self):
        self.validator.validate_action_fields(
            self._action(["my_schema.my_table", "my_table"]), "act"
        )

    def test_unset_depends_on_passes(self):
        self.validator.validate_action_fields(self._action(None), "act")

    def test_empty_depends_on_passes(self):
        self.validator.validate_action_fields(self._action([]), "act")

    def test_four_part_reference_rejected(self):
        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_action_fields(self._action(["a.b.c.d"]), "act")
        error = exc_info.value
        assert error.code == "LHP-VAL-063"
        assert "depends_on" in str(error)

    def test_empty_string_entry_rejected(self):
        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_action_fields(self._action([""]), "act")
        assert exc_info.value.code == "LHP-VAL-063"

    def test_blank_dotted_part_rejected(self):
        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_action_fields(self._action(["a..b"]), "act")
        assert exc_info.value.code == "LHP-VAL-063"

    def test_non_string_entry_rejected(self):
        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_action_fields(self._action([123]), "act")
        assert exc_info.value.code == "LHP-VAL-063"

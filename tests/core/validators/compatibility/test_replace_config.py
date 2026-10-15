"""Tests for ReplaceFlowConfigValidator.

Covers:
- Valid replace_config (replace_using list + sequence_by string) → no errors.
- Missing replace_config → error mentioning replace_config.
- Missing replace_using → error mentioning replace_using.
- Empty replace_using list → error mentioning replace_using.
- replace_using not a list → error mentioning replace_using.
- Missing sequence_by → error mentioning sequence_by.
- sequence_by as a list (must be single column) → error mentioning sequence_by.
"""

import pytest

from lhp.core.validators.compatibility import ReplaceFlowConfigValidator
from lhp.models import Action, ActionType


def _replace_action(
    replace_config: dict | None = None,
) -> Action:
    """Build a replace mode write action with custom replace_config."""
    if replace_config is None:
        replace_config = {
            "replace_using": ["order_id"],
            "sequence_by": "updated_at",
        }

    write_target = {
        "type": "streaming_table",
        "mode": "replace",
        "catalog": "cat",
        "schema": "silver",
        "table": "orders_current",
        "create_table": True,
    }
    if replace_config is not None:
        write_target["replace_config"] = replace_config

    return Action(
        name="write_test",
        type=ActionType.WRITE,
        source="v_test",
        write_target=write_target,
    )


def test_replace_config_validator_accepts_valid_config():
    """Valid replace_config (replace_using=[...], sequence_by=...) returns no errors."""
    action = _replace_action()

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert errors == []


def test_replace_config_validator_requires_replace_config():
    """Missing replace_config entirely → error mentioning replace_config."""
    action = _replace_action(replace_config=None)
    action.write_target.pop("replace_config", None)

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("replace_config" in e for e in errors)


def test_replace_config_validator_requires_replace_using():
    """Missing replace_using key → error mentioning replace_using."""
    action = _replace_action(
        replace_config={
            "sequence_by": "updated_at",
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("replace_using" in e for e in errors)


def test_replace_config_validator_rejects_empty_replace_using():
    """Empty replace_using list → error mentioning replace_using."""
    action = _replace_action(
        replace_config={
            "replace_using": [],
            "sequence_by": "updated_at",
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("replace_using" in e for e in errors)


def test_replace_config_validator_rejects_non_list_replace_using():
    """replace_using as a string (not list) → error mentioning replace_using."""
    action = _replace_action(
        replace_config={
            "replace_using": "order_id",
            "sequence_by": "updated_at",
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("replace_using" in e for e in errors)


def test_replace_config_validator_requires_sequence_by():
    """Missing sequence_by → error mentioning sequence_by."""
    action = _replace_action(
        replace_config={
            "replace_using": ["order_id"],
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("sequence_by" in e for e in errors)


def test_replace_config_validator_rejects_list_sequence_by():
    """sequence_by as a list (must be single string column) → error mentioning sequence_by."""
    action = _replace_action(
        replace_config={
            "replace_using": ["order_id"],
            "sequence_by": ["updated_at", "version"],
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert len(errors) > 0
    assert any("sequence_by" in e for e in errors)


def test_replace_config_validator_multiple_replace_using_columns():
    """Multiple columns in replace_using is valid."""
    action = _replace_action(
        replace_config={
            "replace_using": ["order_id", "customer_id"],
            "sequence_by": "updated_at",
        }
    )

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert errors == []


def test_replace_config_validator_prefix_in_error_messages():
    """Error messages include the provided prefix."""
    action = _replace_action(replace_config=None)
    action.write_target.pop("replace_config", None)

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "my_flowgroup.my_action")

    assert len(errors) > 0
    assert any("my_flowgroup.my_action" in e for e in errors)


def test_replace_config_validator_reports_missing_fields_when_config_empty():
    """An explicitly-empty replace_config ({}) reports the missing keys rather
    than claiming the whole block is absent."""
    action = _replace_action(replace_config={})

    validator = ReplaceFlowConfigValidator()
    errors = validator.validate(action, "fg.write_test")

    assert any("replace_using" in e for e in errors)
    assert any("sequence_by" in e for e in errors)
    assert not any("requires 'replace_config'" in e for e in errors)

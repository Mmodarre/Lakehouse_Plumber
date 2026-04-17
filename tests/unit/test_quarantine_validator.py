"""Tests for quarantine mode validation in transform_validator."""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml

from lhp.core.validators.transform_validator import TransformActionValidator
from lhp.models.config import Action, ActionType, QuarantineConfig, TransformType


@pytest.fixture
def validator(tmp_path):
    """Create a TransformActionValidator with mocked dependencies."""
    mock_registry = MagicMock()
    mock_registry.is_generator_available.return_value = True
    mock_field_validator = MagicMock()
    return TransformActionValidator(
        action_registry=mock_registry,
        field_validator=mock_field_validator,
        project_root=tmp_path,
    )


def _make_dq_action(**kwargs):
    """Helper to create a data quality action with defaults."""
    defaults = dict(
        name="test_dq",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_raw",
        target="v_validated",
    )
    defaults.update(kwargs)
    return Action(**defaults)


class TestQuarantineValidation:
    """Tests for quarantine mode validation."""

    def test_quarantine_mode_requires_quarantine_block(self, validator):
        """mode='quarantine' without quarantine block should error."""
        action = _make_dq_action(mode="quarantine")
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("requires" in e and "quarantine" in e for e in errors)

    def test_quarantine_block_requires_quarantine_mode(self, validator):
        """quarantine block without mode='quarantine' should error."""
        action = _make_dq_action(
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("only valid when mode='quarantine'" in e for e in errors)

    def test_quarantine_requires_dlq_table(self, validator):
        """Missing dlq_table in quarantine config should error."""
        action = _make_dq_action(
            mode="quarantine",
            quarantine=QuarantineConfig(dlq_table="", source_table="cat.sch.src"),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("dlq_table" in e for e in errors)

    def test_quarantine_requires_source_table(self, validator):
        """Missing source_table in quarantine config should error."""
        action = _make_dq_action(
            mode="quarantine",
            quarantine=QuarantineConfig(dlq_table="cat.sch.dlq", source_table=""),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("source_table" in e for e in errors)

    def test_quarantine_valid_config(self, validator, tmp_path):
        """Valid quarantine config should produce no errors."""
        # Create a valid expectations file
        exp_file = tmp_path / "expectations.yaml"
        exp_file.write_text(
            yaml.dump(
                {
                    "id IS NOT NULL": {"action": "drop", "name": "id_check"},
                }
            )
        )

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert errors == []

    def test_invalid_mode_value(self, validator):
        """Invalid mode value should error."""
        action = _make_dq_action(mode="invalid_mode")
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("must be 'dqe' or 'quarantine'" in e for e in errors)

    def test_dqe_mode_valid(self, validator):
        """mode='dqe' should be valid without quarantine block."""
        action = _make_dq_action(mode="dqe")
        errors = validator._validate_data_quality_transform(action, "test")
        assert errors == []

    def test_no_mode_field_valid(self, validator):
        """No mode field should be valid (backward compatibility)."""
        action = _make_dq_action()
        errors = validator._validate_data_quality_transform(action, "test")
        assert errors == []

    def test_quarantine_empty_expectations_error(self, validator, tmp_path):
        """Quarantine with empty expectations file should error."""
        exp_file = tmp_path / "empty_expectations.yaml"
        exp_file.write_text(yaml.dump({"expectations": []}))

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("at least one expectation" in e for e in errors)

    def test_quarantine_warns_on_fail_expectations(self, validator, tmp_path, caplog):
        """Quarantine should warn on fail/warn expectations."""
        import logging

        exp_file = tmp_path / "expectations.yaml"
        exp_file.write_text(
            yaml.dump(
                {
                    "col IS NOT NULL": {"action": "fail", "name": "fail_rule"},
                    "col > 0": {"action": "warn", "name": "warn_rule"},
                    "col2 IS NOT NULL": {"action": "drop", "name": "drop_rule"},
                }
            )
        )

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        with caplog.at_level(logging.WARNING):
            errors = validator._validate_data_quality_transform(action, "test")

        # Should not have errors (just warnings)
        assert errors == []
        # Should have warnings for fail and warn rules
        assert any(
            "fail_rule" in r.message and "fail" in r.message for r in caplog.records
        )
        assert any(
            "warn_rule" in r.message and "warn" in r.message for r in caplog.records
        )

    def test_dict_quarantine_bad_dlq_table_name(self, validator):
        """Dict quarantine config with non-3-part dlq_table should error."""
        action = MagicMock()
        action.name = "test_dq"
        action.source = "v_raw"
        action.mode = "quarantine"
        action.quarantine = {
            "dlq_table": "bad_name",
            "source_table": "catalog.schema.table",
        }
        action.expectations_file = None

        errors = validator._validate_data_quality_transform(action, "test")
        assert any("3-part name" in e for e in errors)

    def test_object_quarantine_bad_dlq_table_name(self, validator):
        """QuarantineConfig object with non-3-part dlq_table should error."""
        action = _make_dq_action(
            mode="quarantine",
            quarantine=QuarantineConfig(
                dlq_table="bad_name", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("3-part name" in e for e in errors)

    def test_expectations_file_doesnt_exist(self, validator):
        """Non-existent expectations file should error."""
        action = _make_dq_action(
            mode="quarantine",
            expectations_file="nonexistent.yaml",
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("Failed to load expectations file" in e for e in errors)

    def test_expectations_file_weird_content(self, validator, tmp_path):
        """Expectations file with bare string content should error."""
        exp_file = tmp_path / "weird.yaml"
        exp_file.write_text(yaml.dump("hello"))

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("unexpected format" in e for e in errors)

    def test_list_expectations_with_fail_action_warns(self, validator, tmp_path, caplog):
        """List-format expectations with failureAction: fail should warn."""
        exp_file = tmp_path / "expectations.yaml"
        exp_file.write_text(
            yaml.dump(
                [
                    {
                        "name": "id_check",
                        "constraint": "id IS NOT NULL",
                        "failureAction": "fail",
                    }
                ]
            )
        )

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        with caplog.at_level(logging.WARNING):
            errors = validator._validate_data_quality_transform(action, "test")

        assert errors == []
        assert any(
            "id_check" in r.message and "coerce" in r.message.lower()
            for r in caplog.records
        )

    def test_dict_expectations_all_drop_no_warnings(self, validator, tmp_path, caplog):
        """Dict-format expectations all with action: drop should produce no warnings."""
        exp_file = tmp_path / "expectations.yaml"
        exp_file.write_text(
            yaml.dump(
                {
                    "col IS NOT NULL": {"action": "drop", "name": "drop_rule1"},
                    "col2 > 0": {"action": "drop", "name": "drop_rule2"},
                }
            )
        )

        action = _make_dq_action(
            mode="quarantine",
            expectations_file=str(exp_file),
            quarantine=QuarantineConfig(
                dlq_table="cat.sch.dlq", source_table="cat.sch.src"
            ),
        )

        with caplog.at_level(logging.WARNING):
            errors = validator._validate_data_quality_transform(action, "test")

        assert errors == []
        warning_records = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warning_records) == 0

    def test_invalid_transform_type_after_registry_check(self, validator):
        """TransformType ValueError after registry accepts type should not crash."""
        action = MagicMock()
        action.name = "test_action"
        action.transform_type = "bogus_type"

        with patch(
            "lhp.core.validators.transform_validator.TransformType",
            side_effect=ValueError("invalid"),
        ):
            errors = validator._validate_transform_type(action, "test")

        assert errors == []

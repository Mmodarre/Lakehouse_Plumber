"""
Extended direct tests for DLT CDC validators.

Tests CdcConfigValidator, SnapshotCdcConfigValidator, and CdcSchemaValidator
directly (not via ConfigValidator) to cover uncovered validation paths.
"""

import pytest
from lhp.core.dlt_cdc_validators import (
    CdcConfigValidator,
    CdcSchemaValidator,
    SnapshotCdcConfigValidator,
)
from lhp.models.config import Action, ActionType


class TestCdcConfigValidatorDirect:
    """Direct tests for CdcConfigValidator.validate()."""

    def setup_method(self):
        self.validator = CdcConfigValidator()
        self.prefix = "test_prefix"

    def _make_action(self, write_target):
        return Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target=write_target,
        )

    def test_validate_missing_write_target_returns_empty(self):
        """write_target=None should return no errors."""
        action = self._make_action(write_target=None)
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_missing_cdc_config_returns_error(self):
        """Missing cdc_config key should produce 'requires cdc_config' error."""
        action = self._make_action(write_target={"type": "streaming_table"})
        errors = self.validator.validate(action, self.prefix)
        assert any("cdc mode requires 'cdc_config'" in e for e in errors)

    def test_validate_non_dict_cdc_config_returns_error(self):
        """Non-dict cdc_config should produce 'must be a dictionary' error."""
        action = self._make_action(
            write_target={"type": "streaming_table", "cdc_config": "not_a_dict"}
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'cdc_config' must be a dictionary" in e for e in errors)

    def test_validate_required_fields_missing_keys(self):
        """cdc_config without 'keys' should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"sequence_by": "ts"},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("must have 'keys'" in e for e in errors)

    def test_validate_required_fields_non_list_keys(self):
        """cdc_config with non-list keys should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": "not_a_list"},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'keys' must be a list" in e for e in errors)

    def test_validate_required_fields_non_string_elements_in_keys(self):
        """cdc_config with non-string elements in keys list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["valid_key", 123, True]},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("keys[1] must be a string" in e for e in errors)
        assert any("keys[2] must be a string" in e for e in errors)

    def test_validate_sequence_options_empty_list(self):
        """Empty sequence_by list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "sequence_by": []},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'sequence_by' list cannot be empty" in e for e in errors)

    def test_validate_sequence_options_non_string_non_list(self):
        """Non-string, non-list sequence_by (e.g. int) should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "sequence_by": 42},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'sequence_by' must be a string or list" in e for e in errors)

    def test_validate_sequence_options_list_with_non_string_elements(self):
        """sequence_by list with non-string elements should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "sequence_by": ["col1", 99]},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("sequence_by[1] must be a string" in e for e in errors)

    def test_validate_sequence_options_valid_string(self):
        """Valid string sequence_by should not produce errors for sequence_by."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert not any("sequence_by" in e for e in errors)

    def test_validate_sequence_options_valid_list(self):
        """Valid list sequence_by should not produce errors for sequence_by."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "sequence_by": ["col1", "col2"]},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert not any("sequence_by" in e for e in errors)

    def test_validate_scd_options_apply_as_truncates_non_string(self):
        """Non-string apply_as_truncates should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "apply_as_truncates": 123},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'apply_as_truncates' must be a string expression" in e for e in errors)

    def test_validate_scd_options_apply_as_truncates_with_scd_type_2(self):
        """apply_as_truncates with scd_type=2 should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "apply_as_truncates": "operation = 'TRUNCATE'",
                    "scd_type": 2,
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "'apply_as_truncates' is not supported with SCD Type 2" in e
            for e in errors
        )

    def test_validate_scd_options_track_history_column_list_and_except_mutual_exclusivity(
        self,
    ):
        """Both track_history_column_list and track_history_except_column_list should error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "track_history_column_list": ["col1"],
                    "track_history_except_column_list": ["col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "cannot have both 'track_history_column_list' and 'track_history_except_column_list'"
            in e
            for e in errors
        )

    def test_validate_scd_options_track_history_column_list_non_list(self):
        """Non-list track_history_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "track_history_column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'track_history_column_list' must be a list" in e for e in errors)

    def test_validate_scd_options_track_history_column_list_non_string_elements(self):
        """Non-string elements in track_history_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "track_history_column_list": ["col1", 42],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "track_history_column_list[1] must be a string" in e for e in errors
        )

    def test_validate_scd_options_track_history_except_column_list_non_list(self):
        """Non-list track_history_except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "track_history_except_column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "'track_history_except_column_list' must be a list" in e for e in errors
        )

    def test_validate_scd_options_track_history_except_column_list_non_string_elements(
        self,
    ):
        """Non-string elements in track_history_except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "track_history_except_column_list": [True, "col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "track_history_except_column_list[0] must be a string" in e
            for e in errors
        )

    def test_validate_column_lists_both_present_mutual_exclusivity(self):
        """Both column_list and except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "column_list": ["col1"],
                    "except_column_list": ["col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "cannot have both 'column_list' and 'except_column_list'" in e
            for e in errors
        )

    def test_validate_column_lists_column_list_non_list(self):
        """Non-list column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'column_list' must be a list" in e for e in errors)

    def test_validate_column_lists_column_list_non_string_elements(self):
        """Non-string elements in column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "column_list": ["col1", 99, None],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("column_list[1] must be a string" in e for e in errors)
        assert any("column_list[2] must be a string" in e for e in errors)

    def test_validate_column_lists_except_column_list_non_list(self):
        """Non-list except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "except_column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'except_column_list' must be a list" in e for e in errors)

    def test_validate_column_lists_except_column_list_non_string_elements(self):
        """Non-string elements in except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "except_column_list": [42],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("except_column_list[0] must be a string" in e for e in errors)

    def test_validate_scd_options_invalid_scd_type(self):
        """Invalid scd_type (not 1 or 2) should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "scd_type": 3},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'scd_type' must be 1 or 2" in e for e in errors)

    def test_validate_scd_options_ignore_null_updates_non_bool(self):
        """Non-boolean ignore_null_updates should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "ignore_null_updates": "yes"},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'ignore_null_updates' must be a boolean" in e for e in errors)

    def test_validate_scd_options_apply_as_deletes_non_string(self):
        """Non-string apply_as_deletes should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {"keys": ["id"], "apply_as_deletes": 123},
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'apply_as_deletes' must be a string expression" in e for e in errors)

    def test_validate_valid_full_cdc_config(self):
        """A fully valid cdc_config should produce no errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "updated_at",
                    "scd_type": 1,
                    "ignore_null_updates": True,
                    "apply_as_deletes": "operation = 'DELETE'",
                    "apply_as_truncates": "operation = 'TRUNCATE'",
                    "column_list": ["col1", "col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []


class TestSnapshotCdcConfigValidatorDirect:
    """Direct tests for SnapshotCdcConfigValidator.validate()."""

    def setup_method(self):
        self.validator = SnapshotCdcConfigValidator()
        self.prefix = "test_prefix"

    def _make_action(self, write_target):
        return Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target=write_target,
        )

    def test_validate_missing_write_target_returns_empty(self):
        """write_target=None should return no errors."""
        action = self._make_action(write_target=None)
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_missing_snapshot_cdc_config_returns_error(self):
        """Missing snapshot_cdc_config key should produce error."""
        action = self._make_action(write_target={"type": "streaming_table"})
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "snapshot_cdc mode requires 'snapshot_cdc_config'" in e for e in errors
        )

    def test_validate_non_dict_snapshot_cdc_config_returns_error(self):
        """Non-dict snapshot_cdc_config should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": "not_a_dict",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'snapshot_cdc_config' must be a dictionary" in e for e in errors)

    def test_validate_source_configuration_non_dict_source_function(self):
        """Non-dict source_function should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source_function": "not_a_dict",
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'source_function' must be a dictionary" in e for e in errors)

    def test_validate_source_configuration_source_function_missing_file(self):
        """source_function missing 'file' should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source_function": {"function": "my_func"},
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("source_function must have 'file'" in e for e in errors)

    def test_validate_source_configuration_source_function_missing_function(self):
        """source_function missing 'function' should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source_function": {"file": "my_module.py"},
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("source_function must have 'function'" in e for e in errors)

    def test_validate_source_configuration_neither_source_nor_function(self):
        """Neither source nor source_function should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "must have either 'source' or 'source_function'" in e for e in errors
        )

    def test_validate_source_configuration_both_source_and_function(self):
        """Both source and source_function should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "source_function": {
                        "file": "my_module.py",
                        "function": "my_func",
                    },
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "cannot have both 'source' and 'source_function'" in e for e in errors
        )

    def test_validate_keys_configuration_missing_keys(self):
        """Missing keys should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("snapshot_cdc_config must have 'keys'" in e for e in errors)

    def test_validate_keys_configuration_non_list_keys(self):
        """Non-list keys should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'keys' must be a list" in e for e in errors)

    def test_validate_keys_configuration_non_string_elements(self):
        """Non-string elements in keys should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id", 123],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("keys[1] must be a string" in e for e in errors)

    def test_validate_track_history_column_list_non_list(self):
        """Non-list track_history_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "track_history_column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'track_history_column_list' must be a list" in e for e in errors)

    def test_validate_track_history_column_list_non_string_elements(self):
        """Non-string elements in track_history_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "track_history_column_list": ["col1", 42],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "track_history_column_list[1] must be a string" in e for e in errors
        )

    def test_validate_track_history_except_column_list_non_list(self):
        """Non-list track_history_except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "track_history_except_column_list": "not_a_list",
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "'track_history_except_column_list' must be a list" in e for e in errors
        )

    def test_validate_track_history_except_column_list_non_string_elements(self):
        """Non-string elements in track_history_except_column_list should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "track_history_except_column_list": [True, "col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "track_history_except_column_list[0] must be a string" in e
            for e in errors
        )

    def test_validate_track_history_mutual_exclusivity(self):
        """Both track_history lists present should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "track_history_column_list": ["col1"],
                    "track_history_except_column_list": ["col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any(
            "cannot have both 'track_history_column_list' and 'track_history_except_column_list'"
            in e
            for e in errors
        )

    def test_validate_scd_configuration_invalid_scd_type(self):
        """Invalid stored_as_scd_type should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_table",
                    "keys": ["id"],
                    "stored_as_scd_type": 3,
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("'stored_as_scd_type' must be 1 or 2" in e for e in errors)

    def test_validate_valid_full_snapshot_cdc_config(self):
        """A fully valid snapshot_cdc_config should produce no errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source": "my_snapshot_table",
                    "keys": ["id"],
                    "stored_as_scd_type": 2,
                    "track_history_column_list": ["col1", "col2"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_valid_source_function_config(self):
        """Valid source_function config should produce no errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "snapshot_cdc_config": {
                    "source_function": {
                        "file": "my_module.py",
                        "function": "my_func",
                    },
                    "keys": ["id"],
                },
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []


class TestCdcSchemaValidatorDirect:
    """Direct tests for CdcSchemaValidator.validate()."""

    def setup_method(self):
        self.validator = CdcSchemaValidator()
        self.prefix = "test_prefix"

    def _make_action(self, write_target):
        return Action(
            name="test_action",
            type=ActionType.WRITE,
            source="v_source",
            write_target=write_target,
        )

    def test_validate_no_write_target_returns_empty(self):
        """write_target=None should return no errors."""
        action = self._make_action(write_target=None)
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_no_schema_returns_empty(self):
        """No schema in write_target should return no errors."""
        action = self._make_action(
            write_target={"type": "streaming_table", "database": "db", "table": "t"}
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_schema_missing_start_at(self):
        """Schema without __START_AT should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "table_schema": "id INT, name STRING, __END_AT TIMESTAMP",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("__START_AT" in e for e in errors)
        assert not any("__END_AT" in e for e in errors)

    def test_validate_schema_missing_end_at(self):
        """Schema without __END_AT should produce error."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "table_schema": "id INT, name STRING, __START_AT TIMESTAMP",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("__END_AT" in e for e in errors)
        assert not any("__START_AT" in e for e in errors)

    def test_validate_schema_with_both_columns_no_errors(self):
        """Schema with both __START_AT and __END_AT should produce no errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "table_schema": "id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_schema_missing_both_columns(self):
        """Schema missing both __START_AT and __END_AT should produce two errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "table_schema": "id INT, name STRING",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert any("__START_AT" in e for e in errors)
        assert any("__END_AT" in e for e in errors)
        assert len(errors) == 2

    def test_validate_schema_via_legacy_schema_key(self):
        """Schema provided via 'schema' key (legacy) should also be validated."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "schema": "id INT, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert errors == []

    def test_validate_legacy_schema_key_missing_columns(self):
        """Schema via 'schema' key missing CDC columns should produce errors."""
        action = self._make_action(
            write_target={
                "type": "streaming_table",
                "schema": "id INT, name STRING",
            }
        )
        errors = self.validator.validate(action, self.prefix)
        assert len(errors) == 2

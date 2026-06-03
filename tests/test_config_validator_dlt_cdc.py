"""DLT table options and CDC configuration tests for ConfigValidator."""

import pytest

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType


class TestConfigValidatorDltCdc:
    def test_dlt_table_options_validation_comprehensive(self):
        validator = ConfigValidator()

        action = Action(
            name="test_invalid_spark_conf_keys",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "spark_conf": {
                    123: "invalid_int_key",  # Keys must be strings
                    "valid_key": "valid_value",
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("spark_conf key '123' must be a string" in error for error in errors)

        action = Action(
            name="test_invalid_table_props_keys",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "table_properties": {
                    456: "invalid_int_key",  # Keys must be strings
                    "valid_key": "valid_value",
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "table_properties key '456' must be a string" in error for error in errors
        )

        action = Action(
            name="test_invalid_schema_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "table_schema": {"invalid": "object"},  # Should be string
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'table_schema' must be a string" in error for error in errors)

        action = Action(
            name="test_invalid_row_filter_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "row_filter": 123,  # Should be string
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'row_filter' must be a string" in error for error in errors)

        action = Action(
            name="test_invalid_temporary_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "temporary": "yes",  # Should be boolean
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'temporary' must be a boolean" in error for error in errors)

        action = Action(
            name="test_invalid_partition_cols_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "partition_columns": "column1",  # Should be list
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'partition_columns' must be a list" in error for error in errors)

        action = Action(
            name="test_invalid_partition_col_element",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "partition_columns": [123, "valid_column"],  # Elements must be strings
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("partition_columns[0] must be a string" in error for error in errors)

        action = Action(
            name="test_invalid_cluster_cols_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "cluster_columns": "column1",  # Should be list
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'cluster_columns' must be a list" in error for error in errors)

        action = Action(
            name="test_invalid_cluster_col_element",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "cluster_columns": [456, "valid_column"],  # Elements must be strings
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("cluster_columns[0] must be a string" in error for error in errors)

    def test_cdc_config_validation_comprehensive(self):
        validator = ConfigValidator()

        action = Action(
            name="test_invalid_sequence_by_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": 123,  # Should be string or list
                    "keys": ["id"],
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "'sequence_by' must be a string or list of strings" in error
            for error in errors
        )

        action = Action(
            name="test_invalid_sequence_by_elements",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": [
                        "valid_column",
                        456,
                    ],  # List elements must be strings
                    "keys": ["id"],
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("sequence_by[1] must be a string" in error for error in errors)

        action = Action(
            name="test_invalid_scd_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "scd_type": "invalid_type",  # Should be 1 or 2
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any("'scd_type' must be 1 or 2" in error for error in errors)

        action = Action(
            name="test_invalid_apply_as_deletes_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "apply_as_deletes": 123,  # Should be string
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "'apply_as_deletes' must be a string expression" in error
            for error in errors
        )

        action = Action(
            name="test_invalid_ignore_null_updates",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "ignore_null_updates": "no",  # Should be boolean
                },
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "'ignore_null_updates' must be a boolean" in error for error in errors
        )

        action = Action(
            name="test_valid_apply_as_deletes",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "apply_as_deletes": "DELETE",  # Valid string
                },
            },
        )
        errors = validator.validate_action(action, 0)
        delete_errors = [e for e in errors if "apply_as_deletes" in e]
        assert len(delete_errors) == 0

        action = Action(
            name="test_valid_sequence_by_string",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": "timestamp_col",  # Valid string
                    "keys": ["id"],
                },
            },
        )
        errors = validator.validate_action(action, 0)
        seq_errors = [e for e in errors if "sequence_by" in e]
        assert len(seq_errors) == 0

        action = Action(
            name="test_valid_sequence_by_list",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": ["timestamp_col", "id"],  # Valid list
                    "keys": ["id"],
                },
            },
        )
        errors = validator.validate_action(action, 0)
        seq_errors = [e for e in errors if "sequence_by" in e]
        assert len(seq_errors) == 0

        action = Action(
            name="test_valid_scd_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "scd_type": 2},  # Valid SCD type
            },
        )
        errors = validator.validate_action(action, 0)
        scd_errors = [e for e in errors if "scd_type" in e]
        assert len(scd_errors) == 0

    def test_cdc_schema_validation_comprehensive(self):
        validator = ConfigValidator()

        action = Action(
            name="test_cdc_schema_missing_start_at",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                "table_schema": "id INT, name STRING, __END_AT TIMESTAMP",  # Missing __START_AT
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "CDC schema must include '__START_AT' column with same type as sequence_by"
            in error
            for error in errors
        )

        action = Action(
            name="test_cdc_schema_missing_end_at",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                "table_schema": "id INT, name STRING, __START_AT TIMESTAMP",  # Missing __END_AT
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "CDC schema must include '__END_AT' column with same type as sequence_by"
            in error
            for error in errors
        )

        action = Action(
            name="test_cdc_schema_missing_both",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                "table_schema": "id INT, name STRING",  # Missing both __START_AT and __END_AT
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "CDC schema must include '__START_AT' column with same type as sequence_by"
            in error
            for error in errors
        )
        assert any(
            "CDC schema must include '__END_AT' column with same type as sequence_by"
            in error
            for error in errors
        )

        action = Action(
            name="test_cdc_schema_alternate_missing_both",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                "table_schema": "id INT, name STRING",  # Missing both __START_AT and __END_AT
            },
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "CDC schema must include '__START_AT' column with same type as sequence_by"
            in error
            for error in errors
        )
        assert any(
            "CDC schema must include '__END_AT' column with same type as sequence_by"
            in error
            for error in errors
        )

        action = Action(
            name="test_cdc_schema_valid",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                "table_schema": "id INT, name STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
            },
        )
        errors = validator.validate_action(action, 0)
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0

        action = Action(
            name="test_cdc_no_schema",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {"keys": ["id"], "sequence_by": "timestamp_col"},
                # No schema field - should NOT trigger schema validation
            },
        )
        errors = validator.validate_action(action, 0)
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0

        action = Action(
            name="test_standard_with_schema",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test",
                "table": "test",
                "mode": "standard",  # Not CDC mode
                "table_schema": "id INT, name STRING",  # No CDC columns required
            },
        )
        errors = validator.validate_action(action, 0)
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

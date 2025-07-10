"""
DLT table options and CDC configuration tests for ConfigValidator.
"""

import pytest
from lhp.core.validator import ConfigValidator
from lhp.models.config import Action, ActionType


class TestConfigValidatorDltCdc:
    """DLT table options and CDC configuration tests for ConfigValidator."""
    
    def test_dlt_table_options_validation_comprehensive(self):
        """Test DLT table options validation comprehensively.
        
        Target lines: 405-406, 421, 424, 433, 435, 439, 458, 462, 468, 472, 477, 481-482, 485-486, 491, 493, 495, 499
        Tests spark_conf keys, table_properties keys, schema/row_filter/temporary types, and column validation.
        """
        validator = ConfigValidator()
        
        # Test 1: Invalid spark_conf keys (lines 405-406)
        action = Action(
            name="test_invalid_spark_conf_keys",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "spark_conf": {
                    123: "invalid_int_key",  # Keys must be strings
                    "valid_key": "valid_value"
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("spark_conf key '123' must be a string" in error for error in errors)
        
        # Test 2: Invalid table_properties keys (lines 421, 424, 433, 435, 439)
        action = Action(
            name="test_invalid_table_props_keys",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "table_properties": {
                    456: "invalid_int_key",  # Keys must be strings
                    "valid_key": "valid_value"
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("table_properties key '456' must be a string" in error for error in errors)
        
        # Test 3: Invalid schema type (lines 458, 462)
        action = Action(
            name="test_invalid_schema_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "schema": {"invalid": "object"}  # Should be string
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'table_schema' (or 'schema') must be a string" in error for error in errors)
        
        # Test 4: Invalid row_filter type (lines 468, 472)
        action = Action(
            name="test_invalid_row_filter_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "row_filter": 123  # Should be string
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'row_filter' must be a string" in error for error in errors)
        
        # Test 5: Invalid temporary type (lines 477)
        action = Action(
            name="test_invalid_temporary_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "temporary": "yes"  # Should be boolean
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'temporary' must be a boolean" in error for error in errors)
        
        # Test 6: Invalid partition_columns type (lines 481-482)
        action = Action(
            name="test_invalid_partition_cols_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "partition_columns": "column1"  # Should be list
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'partition_columns' must be a list" in error for error in errors)
        
        # Test 7: Invalid partition_columns element type (lines 485-486)
        action = Action(
            name="test_invalid_partition_col_element",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "partition_columns": [123, "valid_column"]  # Elements must be strings
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("partition_columns[0] must be a string" in error for error in errors)
        
        # Test 8: Invalid cluster_columns type (lines 491, 493)
        action = Action(
            name="test_invalid_cluster_cols_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "cluster_columns": "column1"  # Should be list
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'cluster_columns' must be a list" in error for error in errors)
        
        # Test 9: Invalid cluster_columns element type (lines 495, 499)
        action = Action(
            name="test_invalid_cluster_col_element",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "cluster_columns": [456, "valid_column"]  # Elements must be strings
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("cluster_columns[0] must be a string" in error for error in errors)

    def test_cdc_config_validation_comprehensive(self):
        """Test CDC configuration validation comprehensively.
        
        Target lines: 503->519, 507-516, 520->525, 522, 527-528, 533-534, 539-543
        Tests sequence_by validation, SCD type validation, and boolean/string validations.
        """
        validator = ConfigValidator()
        
        # Test 1: Invalid sequence_by type (lines 503->519, 507-516)
        action = Action(
            name="test_invalid_sequence_by_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": 123,  # Should be string or list
                    "keys": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'sequence_by' must be a string or list of strings" in error for error in errors)
        
        # Test 2: Invalid sequence_by list elements (lines 507-516)
        action = Action(
            name="test_invalid_sequence_by_elements",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": ["valid_column", 456],  # List elements must be strings
                    "keys": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("sequence_by[1] must be a string" in error for error in errors)
        
        # Test 3: Invalid SCD type (lines 520->525, 522)
        action = Action(
            name="test_invalid_scd_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "scd_type": "invalid_type"  # Should be 1 or 2
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'scd_type' must be 1 or 2" in error for error in errors)
        
        # Test 4: Invalid apply_as_deletes type (lines 539-543)
        action = Action(
            name="test_invalid_apply_as_deletes_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "apply_as_deletes": 123  # Should be string
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'apply_as_deletes' must be a string expression" in error for error in errors)
        
        # Test 5: Invalid ignore_null_updates type (lines 533-534)
        action = Action(
            name="test_invalid_ignore_null_updates",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "ignore_null_updates": "no"  # Should be boolean
                }
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("'ignore_null_updates' must be a boolean" in error for error in errors)
        
        # Test 6: Valid apply_as_deletes (should NOT error)
        action = Action(
            name="test_valid_apply_as_deletes",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "apply_as_deletes": "DELETE"  # Valid string
                }
            }
        )
        errors = validator.validate_action(action, 0)
        # This should NOT produce an error since it's a valid string
        delete_errors = [e for e in errors if "apply_as_deletes" in e]
        assert len(delete_errors) == 0
        
        # Test 7: Valid sequence_by string (should NOT error)
        action = Action(
            name="test_valid_sequence_by_string",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": "timestamp_col",  # Valid string
                    "keys": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have sequence_by-related errors
        seq_errors = [e for e in errors if "sequence_by" in e]
        assert len(seq_errors) == 0
        
        # Test 8: Valid sequence_by list (should NOT error)
        action = Action(
            name="test_valid_sequence_by_list",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "sequence_by": ["timestamp_col", "id"],  # Valid list
                    "keys": ["id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have sequence_by-related errors
        seq_errors = [e for e in errors if "sequence_by" in e]
        assert len(seq_errors) == 0
        
        # Test 9: Valid SCD type (should NOT error)
        action = Action(
            name="test_valid_scd_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "scd_type": 2  # Valid SCD type
                }
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have scd_type-related errors
        scd_errors = [e for e in errors if "scd_type" in e]
        assert len(scd_errors) == 0

    def test_cdc_schema_validation_comprehensive(self):
        """Test CDC schema validation comprehensively.
        
        Target lines: 550, 554-560, 564-570, 576, 580, 585, 589
        Tests missing __START_AT and __END_AT columns in CDC schemas.
        """
        validator = ConfigValidator()
        
        # Test 1: CDC schema missing __START_AT column (lines 554-560)
        action = Action(
            name="test_cdc_schema_missing_start_at",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                },
                "schema": "id INT, name STRING, __END_AT TIMESTAMP"  # Missing __START_AT
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("CDC schema must include '__START_AT' column with same type as sequence_by" in error for error in errors)
        
        # Test 2: CDC schema missing __END_AT column (lines 564-570)
        action = Action(
            name="test_cdc_schema_missing_end_at",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                },
                "schema": "id INT, name STRING, __START_AT TIMESTAMP"  # Missing __END_AT
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("CDC schema must include '__END_AT' column with same type as sequence_by" in error for error in errors)
        
        # Test 3: CDC schema missing both __START_AT and __END_AT columns (lines 554-560, 564-570)
        action = Action(
            name="test_cdc_schema_missing_both",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                },
                "schema": "id INT, name STRING"  # Missing both __START_AT and __END_AT
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("CDC schema must include '__START_AT' column with same type as sequence_by" in error for error in errors)
        assert any("CDC schema must include '__END_AT' column with same type as sequence_by" in error for error in errors)
        
        # Test 4: CDC schema with schema field - alternate test (lines 576, 580, 585, 589)
        action = Action(
            name="test_cdc_schema_alternate_missing_both",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                },
                "schema": "id INT, name STRING"  # Missing both __START_AT and __END_AT
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("CDC schema must include '__START_AT' column with same type as sequence_by" in error for error in errors)
        assert any("CDC schema must include '__END_AT' column with same type as sequence_by" in error for error in errors)
        
        # Test 5: CDC schema with valid __START_AT and __END_AT (should NOT error)
        action = Action(
            name="test_cdc_schema_valid",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                },
                "schema": "id INT, name STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have CDC schema-related errors
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0
        
        # Test 6: CDC mode without schema (should NOT trigger schema validation)
        action = Action(
            name="test_cdc_no_schema",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "cdc",
                "cdc_config": {
                    "keys": ["id"],
                    "sequence_by": "timestamp_col"
                }
                # No schema field - should NOT trigger schema validation
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have CDC schema-related errors since no schema is provided
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0
        
        # Test 7: Non-CDC mode with schema (should NOT trigger CDC schema validation)
        action = Action(
            name="test_standard_with_schema",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "standard",  # Not CDC mode
                "schema": "id INT, name STRING"  # No CDC columns required
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have CDC schema-related errors since it's not CDC mode
        cdc_errors = [e for e in errors if "__START_AT" in e or "__END_AT" in e]
        assert len(cdc_errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
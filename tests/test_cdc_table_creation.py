"""Test CDC mode table creation fix."""

import pytest
from pathlib import Path
import tempfile

from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.models.config import Action, ActionType

def test_cdc_mode_creates_table_and_flow():
    """Test that CDC mode generates both table creation and CDC flow."""
    
    # Create a CDC write action
    action = Action(
        name="write_customer_scd",
        type=ActionType.WRITE,
        source="v_customer_cleansed",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "dim_customer",
            "cdc_config": {
                "keys": ["customer_id"],
                "sequence_by": "_commit_timestamp",
                "scd_type": 2,
                "track_history_columns": ["name", "address", "phone"]
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {
        "preset_config": {
            "defaults": {
                "write_actions": {
                    "streaming_table": {
                        "table_properties": {
                            "delta.enableChangeDataFeed": "true",
                            "quality": "silver"
                        }
                    }
                }
            }
        }
    }
    
    code = generator.generate(action, context)
    
    # Verify both table creation and CDC flow are present
    assert "dlt.create_streaming_table(" in code
    assert 'name="catalog.schema.dim_customer"' in code
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'target="catalog.schema.dim_customer"' in code
    assert 'keys=["customer_id"]' in code
    assert "stored_as_scd_type=2" in code
    assert 'track_history_column_list=["name", "address", "phone"]' in code
    
    # Ensure table is created before CDC flow
    table_pos = code.find("dlt.create_streaming_table(")
    cdc_pos = code.find("dlt.create_auto_cdc_flow(")
    assert table_pos < cdc_pos, "Table must be created before CDC flow"

def test_cdc_mode_with_all_parameters():
    """Test CDC mode with all supported parameters including new ones."""
    
    # Create a comprehensive CDC write action
    action = Action(
        name="write_comprehensive_cdc",
        type=ActionType.WRITE,
        source="v_comprehensive_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "comprehensive_table",
            "cdc_config": {
                "keys": ["id", "partition_key"],
                "sequence_by": "_commit_timestamp",
                "scd_type": 1,
                "ignore_null_updates": True,
                "apply_as_deletes": "operation = 'DELETE'",
                "apply_as_truncates": "operation = 'TRUNCATE'",
                "column_list": ["id", "name", "status", "updated_at"],
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    
    code = generator.generate(action, context)
    
    # Verify all parameters are present
    assert "dlt.create_streaming_table(" in code
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'keys=["id", "partition_key"]' in code
    assert 'sequence_by="_commit_timestamp"' in code
    assert "stored_as_scd_type=1" in code
    assert "ignore_null_updates=True" in code
    assert 'apply_as_deletes="operation = \'DELETE\'"' in code
    assert 'apply_as_truncates="operation = \'TRUNCATE\'"' in code
    assert 'column_list=["id", "name", "status", "updated_at"]' in code

def test_cdc_mode_with_except_column_list():
    """Test CDC mode with except_column_list parameter."""
    
    # Create a CDC write action with except_column_list
    action = Action(
        name="write_cdc_except_columns",
        type=ActionType.WRITE,
        source="v_source_with_many_columns",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "filtered_table",
            "cdc_config": {
                "keys": ["id"],
                "sequence_by": "_timestamp",
                "scd_type": 1,
                "except_column_list": ["internal_field1", "internal_field2", "_metadata"],
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    
    code = generator.generate(action, context)
    
    # Verify except_column_list is properly generated
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'except_column_list=["internal_field1", "internal_field2", "_metadata"]' in code

def test_cdc_mode_with_struct_sequence_by():
    """Test CDC mode with struct() for sequence_by using list format."""
    
    # Create a CDC write action with list sequence_by
    action = Action(
        name="write_cdc_struct_sequence",
        type=ActionType.WRITE,
        source="v_events_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "events_table",
            "cdc_config": {
                "keys": ["event_id", "user_id"],
                "sequence_by": ["event_timestamp", "sequence_number"],  # List format -> struct()
                "scd_type": 1,
                "ignore_null_updates": True,
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    
    code = generator.generate(action, context)
    
    # Verify struct() is properly generated and import is added
    assert "from pyspark.sql.functions import struct" in generator.imports
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'sequence_by=struct("event_timestamp", "sequence_number")' in code
    assert 'keys=["event_id", "user_id"]' in code

def test_cdc_mode_string_sequence_by_still_works():
    """Test CDC mode with traditional string sequence_by still works."""
    
    # Create a CDC write action with string sequence_by
    action = Action(
        name="write_cdc_string_sequence",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "test_table",
            "cdc_config": {
                "keys": ["id"],
                "sequence_by": "_timestamp",  # String format (traditional)
                "scd_type": 1,
            }
        }
    )
    
    # Generate code
    generator = StreamingTableWriteGenerator()
    context = {"expectations": []}
    
    code = generator.generate(action, context)
    
    # Verify string sequence_by is properly generated
    assert "dlt.create_auto_cdc_flow(" in code
    assert 'sequence_by="_timestamp"' in code
    # Should not have struct import for string sequence_by
    assert "from pyspark.sql.functions import struct" not in generator.imports 

def test_cdc_schema_validation():
    """Test CDC schema validation for required __START_AT and __END_AT columns."""
    from lhp.core.validator import ConfigValidator
    
    validator = ConfigValidator()
    
    # Test valid CDC schema with required columns
    action = Action(
        name="valid_cdc_with_schema",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "test_table",
            "schema": "id BIGINT, name STRING, __START_AT TIMESTAMP, __END_AT TIMESTAMP",
            "cdc_config": {
                "keys": ["id"],
                "sequence_by": "_timestamp",
                "scd_type": 2,
            }
        }
    )
    
    errors = validator.validate_action(action, 0)
    cdc_errors = [e for e in errors if '__START_AT' in e or '__END_AT' in e]
    assert len(cdc_errors) == 0
    
    # Test invalid CDC schema missing __START_AT
    action = Action(
        name="invalid_cdc_missing_start",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "test_table",
            "schema": "id BIGINT, name STRING, __END_AT TIMESTAMP",
            "cdc_config": {
                "keys": ["id"],
                "sequence_by": "_timestamp",
                "scd_type": 2,
            }
        }
    )
    
    errors = validator.validate_action(action, 0)
    assert any("CDC schema must include '__START_AT'" in error for error in errors)
    
    # Test invalid CDC schema missing __END_AT
    action = Action(
        name="invalid_cdc_missing_end",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "mode": "cdc",
            "database": "catalog.schema",
            "table": "test_table",
            "schema": "id BIGINT, name STRING, __START_AT TIMESTAMP",
            "cdc_config": {
                "keys": ["id"],
                "sequence_by": "_timestamp",
                "scd_type": 2,
            }
        }
    )
    
    errors = validator.validate_action(action, 0)
    assert any("CDC schema must include '__END_AT'" in error for error in errors)
    
    # Test CDC schema validation only applies to CDC mode
    action = Action(
        name="non_cdc_no_validation",
        type=ActionType.WRITE,
        source="v_source",
        write_target={
            "type": "streaming_table",
            "mode": "standard",
            "database": "catalog.schema",
            "table": "test_table",
            "schema": "id BIGINT, name STRING",  # No __START_AT/__END_AT required
        }
    )
    
    errors = validator.validate_action(action, 0)
    cdc_errors = [e for e in errors if '__START_AT' in e or '__END_AT' in e]
    assert len(cdc_errors) == 0 
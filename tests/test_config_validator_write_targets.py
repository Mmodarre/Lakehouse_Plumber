"""
Write target validation tests for ConfigValidator.
"""

import pytest
from lhp.core.validator import ConfigValidator
from lhp.models.config import Action, ActionType


class TestConfigValidatorWriteTargets:
    """Write target validation tests for ConfigValidator."""
    
    def test_streaming_table_source_validation_edge_cases(self):
        """Test streaming table source validation edge cases.
        
        Target lines: 285->327, 298, 302->311, 305, 308
        Tests snapshot_cdc mode vs standard mode source validation.
        """
        validator = ConfigValidator()
        
        # Test 1: Standard streaming table without source (lines 285->327, 298)
        action = Action(
            name="test_streaming_no_source",
            type=ActionType.WRITE,
            # Missing source - should fail for standard mode
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "standard"  # Standard mode requires source
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("Streaming table must have 'source' to read from" in error for error in errors)
        
        # Test 2: Standard streaming table with invalid source type (lines 302->311, 305, 308)
        action = Action(
            name="test_streaming_invalid_source",
            type=ActionType.WRITE,
            source={"invalid": "dict_source"},  # Should be string or list
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "standard"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("source must be a string or list of view names" in error for error in errors)
        
        # Test 3: Snapshot CDC mode without source - should NOT fail (different validation path)
        action = Action(
            name="test_snapshot_cdc_no_source",
            type=ActionType.WRITE,
            # Missing source - OK for snapshot_cdc mode
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "snapshot_cdc",
                "snapshot_cdc_config": {
                    "source": "raw.customer_snapshots",
                    "keys": ["customer_id"]
                }
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors for snapshot_cdc mode
        source_errors = [e for e in errors if "source" in e and "Streaming table must have" in e]
        assert len(source_errors) == 0
        
        # Test 4: Standard streaming table with valid string source
        action = Action(
            name="test_streaming_valid_string_source",
            type=ActionType.WRITE,
            source="v_input_data",  # Valid string source
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "standard"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors
        source_errors = [e for e in errors if "source" in e and "must be a string or list" in e]
        assert len(source_errors) == 0
        
        # Test 5: Standard streaming table with valid list source
        action = Action(
            name="test_streaming_valid_list_source",
            type=ActionType.WRITE,
            source=["v_input1", "v_input2"],  # Valid list source
            write_target={
                "type": "streaming_table",
                "database": "test",
                "table": "test",
                "mode": "standard"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors
        source_errors = [e for e in errors if "source" in e and "must be a string or list" in e]
        assert len(source_errors) == 0

    def test_materialized_view_source_validation_edge_cases(self):
        """Test materialized view source validation edge cases.
        
        Target lines: 324-325, 332, 343, 354
        Tests materialized view source vs SQL validation and invalid source types.
        """
        validator = ConfigValidator()
        
        # Test 1: Materialized view without source and without SQL (lines 324-325)
        action = Action(
            name="test_mv_no_source_no_sql",
            type=ActionType.WRITE,
            # Missing both source and SQL
            write_target={
                "type": "materialized_view",
                "database": "test",
                "table": "test"
                # Missing both source and sql
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("must have either 'source' or 'sql' in write_target" in error for error in errors)
        
        # Test 2: Materialized view with invalid source type (lines 332, 343, 354)
        action = Action(
            name="test_mv_invalid_source",
            type=ActionType.WRITE,
            source={"invalid": "dict_source"},  # Should be string or list, not dict
            write_target={
                "type": "materialized_view",
                "database": "test",
                "table": "test"
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("source must be a string or list of view names" in error for error in errors)
        
        # Test 3: Materialized view with valid string source (should NOT error)
        action = Action(
            name="test_mv_valid_string_source",
            type=ActionType.WRITE,
            source="v_input_data",  # Valid string source
            write_target={
                "type": "materialized_view",
                "database": "test",
                "table": "test"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors
        source_errors = [e for e in errors if "source must be a string or list" in e]
        assert len(source_errors) == 0
        
        # Test 4: Materialized view with valid list source (should NOT error)
        action = Action(
            name="test_mv_valid_list_source",
            type=ActionType.WRITE,
            source=["v_input1", "v_input2"],  # Valid list source
            write_target={
                "type": "materialized_view",
                "database": "test",
                "table": "test"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors
        source_errors = [e for e in errors if "source must be a string or list" in e]
        assert len(source_errors) == 0
        
        # Test 5: Materialized view with SQL and no source (should NOT error)
        action = Action(
            name="test_mv_sql_no_source",
            type=ActionType.WRITE,
            # No source field - OK because SQL is provided
            write_target={
                "type": "materialized_view",
                "database": "test",
                "table": "test",
                "sql": "SELECT COUNT(*) FROM silver.details"
            }
        )
        errors = validator.validate_action(action, 0)
        # Should NOT have source-related errors
        source_errors = [e for e in errors if "must have either 'source' or 'sql'" in e]
        assert len(source_errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
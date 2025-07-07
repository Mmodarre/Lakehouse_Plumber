"""Tests for Configuration Validator - Step 4.4.3."""

import pytest
from lhp.core.validator import ConfigValidator
from lhp.models.config import FlowGroup, Action, ActionType, TransformType


class TestConfigValidator:
    """Test configuration validator functionality."""
    
    def test_valid_flowgroup(self):
        """Test validation of a valid flowgroup."""
        validator = ConfigValidator()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="load_data",
                    type=ActionType.LOAD,
                    target="v_raw_data",
                    source={
                        "type": "cloudfiles",
                        "path": "/mnt/data",
                        "format": "json"
                    }
                ),
                Action(
                    name="transform_data",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="v_raw_data",
                    target="v_clean_data",
                    sql="SELECT * FROM v_raw_data WHERE is_valid = true"
                ),
                Action(
                    name="write_data",
                    type=ActionType.WRITE,
                    source="v_clean_data",
                    write_target={
                        "type": "streaming_table",
                        "database": "silver",
                        "table": "clean_data"
                    }
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        assert len(errors) == 0
    
    def test_missing_required_fields(self):
        """Test validation catches missing required fields."""
        validator = ConfigValidator()
        
        # Missing pipeline name
        flowgroup = FlowGroup(
            pipeline="",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="test", type=ActionType.LOAD, target="v_test", source={"type": "delta", "table": "test"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("pipeline" in error for error in errors)
        
        # Missing flowgroup name
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="",
            actions=[
                Action(name="test", type=ActionType.LOAD, target="v_test", source={"type": "delta", "table": "test"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("flowgroup" in error for error in errors)
        
        # No actions
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("at least one action" in error for error in errors)
    
    def test_duplicate_names(self):
        """Test detection of duplicate action and target names."""
        validator = ConfigValidator()
        
        # Duplicate action names
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="v_data1", source={"type": "delta", "table": "t1"}),
                Action(name="load_data", type=ActionType.LOAD, target="v_data2", source={"type": "delta", "table": "t2"}),
                Action(name="write", type=ActionType.WRITE, source="v_data1", write_target={"type": "streaming_table", "database": "db", "table": "t"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("Duplicate action name" in error and "load_data" in error for error in errors)
        
        # Duplicate target names
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="load1", type=ActionType.LOAD, target="v_data", source={"type": "delta", "table": "t1"}),
                Action(name="load2", type=ActionType.LOAD, target="v_data", source={"type": "delta", "table": "t2"}),
                Action(name="write", type=ActionType.WRITE, source="v_data", write_target={"type": "streaming_table", "database": "db", "table": "t"})
            ]
        )
        errors = validator.validate_flowgroup(flowgroup)
        assert any("Duplicate target name" in error and "v_data" in error for error in errors)
    
    def test_load_action_validation(self):
        """Test validation of load actions."""
        validator = ConfigValidator()
        
        # Valid CloudFiles load
        action = Action(
            name="load_cloudfiles",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "cloudfiles",
                "path": "/mnt/data",
                "format": "json"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing required fields for CloudFiles
        action = Action(
            name="load_cloudfiles",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "cloudfiles"
                # Missing path and format
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("path" in error for error in errors)
        assert any("format" in error for error in errors)
        
        # Valid JDBC load
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "user",
                "password": "pass",
                "driver": "org.postgresql.Driver",
                "table": "customers"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # JDBC missing query or table
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            target="v_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://host:5432/db",
                "user": "user",
                "password": "pass",
                "driver": "org.postgresql.Driver"
                # Missing both query and table
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("query" in error and "table" in error for error in errors)
    
    def test_transform_action_validation(self):
        """Test validation of transform actions."""
        validator = ConfigValidator()
        
        # Valid SQL transform
        action = Action(
            name="transform_sql",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source="v_input",
            target="v_output",
            sql="SELECT * FROM v_input"
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing SQL for SQL transform
        action = Action(
            name="transform_sql",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source="v_input",
            target="v_output"
            # Missing sql or sql_path
        )
        errors = validator.validate_action(action, 0)
        assert any("sql" in error and "sql_path" in error for error in errors)
        
        # Missing transform_type
        action = Action(
            name="transform",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            sql="SELECT * FROM v_input"
            # Missing transform_type
        )
        errors = validator.validate_action(action, 0)
        assert any("transform_type" in error for error in errors)
        
        # Valid Python transform
        action = Action(
            name="transform_python",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            target="v_output",
            source={
                "module_path": "transformations.py",
                "function_name": "transform_data",
                "sources": ["v_input"]
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
    
    def test_write_action_validation(self):
        """Test validation of write actions."""
        validator = ConfigValidator()
        
        # Valid streaming table write
        action = Action(
            name="write_streaming",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "my_table"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
        
        # Missing required fields
        action = Action(
            name="write_streaming",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "streaming_table"
                # Missing database, table
            }
        )
        errors = validator.validate_action(action, 0)
        assert any("database" in error for error in errors)
        assert any("table" in error for error in errors)
        
        # Valid materialized view with SQL
        action = Action(
            name="write_mv",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "database": "gold",
                "table": "summary",
                "sql": "SELECT COUNT(*) FROM silver.details"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0
    
    def test_action_type_validation(self):
        """Test validation of action types."""
        validator = ConfigValidator()
        
        # Missing action name
        action = Action(
            name="",
            type=ActionType.LOAD,
            target="v_data",
            source={"type": "delta", "table": "test"}
        )
        errors = validator.validate_action(action, 0)
        assert any("name" in error for error in errors)
        
        # Invalid source type for load
        action = Action(
            name="load",
            type=ActionType.LOAD,
            target="v_data",
            source={"type": "invalid_type", "path": "/mnt/data"}
        )
        errors = validator.validate_action(action, 0)
        assert any("Unknown load source type" in error and "invalid_type" in error for error in errors)
        
        # Note: We can't test invalid transform_type directly because Pydantic validates the enum
        # at construction time. This is actually good - it prevents invalid data from being created.
        # The validator still checks if the transform type is supported by the registry.
    
    def test_dependency_validation(self):
        """Test that dependency validation is included."""
        validator = ConfigValidator()
        
        # FlowGroup with missing dependencies
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(
                    name="transform",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.SQL,
                    source="v_missing",  # This view doesn't exist
                    target="v_output",
                    sql="SELECT * FROM v_missing"
                ),
                Action(
                    name="write",
                    type=ActionType.WRITE,
                    source="v_output",
                    write_target={
                        "type": "streaming_table",
                        "database": "silver",
                        "table": "output"
                    }
                )
            ]
        )
        
        errors = validator.validate_flowgroup(flowgroup)
        # Should have errors about missing load action and missing dependency
        assert any("Load action" in error for error in errors)
        assert any("v_missing" in error for error in errors)
    
    def test_edge_cases(self):
        """Test edge cases in validation."""
        validator = ConfigValidator()
        
        # Action with non-dict source for load (should fail)
        action = Action(
            name="load",
            type=ActionType.LOAD,
            target="v_data",
            source="string_source"  # Should be dict
        )
        errors = validator.validate_action(action, 0)
        assert any("configuration object" in error for error in errors)
        
        # Write action with target (warning, not error)
        action = Action(
            name="write",
            type=ActionType.WRITE,
            target="v_should_not_have_target",  # Write actions shouldn't have targets
            source="v_data",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "output"
            }
        )
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0  # Should only log warning, not error


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
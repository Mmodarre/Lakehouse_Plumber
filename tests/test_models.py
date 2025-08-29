"""Tests for core data models of LakehousePlumber."""

import pytest
from lhp.models.config import ActionType, LoadSourceType, TransformType, WriteTargetType, Action, FlowGroup, Template, Preset, TestActionType, ViolationAction


class TestModels:
    """Test the core data models."""
    
    def test_action_type_enum(self):
        """Test ActionType enum values."""
        assert ActionType.LOAD.value == "load"
        assert ActionType.TRANSFORM.value == "transform"
        assert ActionType.WRITE.value == "write"
        # Test for new TEST action type
        assert ActionType.TEST.value == "test"
    
    def test_test_type_enum(self):
        """Test TestActionType enum exists with all required test types."""
        # Test that TestActionType enum exists
        assert TestActionType is not None
        
        # Test all 9 test types exist
        assert TestActionType.ROW_COUNT.value == "row_count"
        assert TestActionType.UNIQUENESS.value == "uniqueness"
        assert TestActionType.REFERENTIAL_INTEGRITY.value == "referential_integrity"
        assert TestActionType.COMPLETENESS.value == "completeness"
        assert TestActionType.RANGE.value == "range"
        assert TestActionType.SCHEMA_MATCH.value == "schema_match"
        assert TestActionType.ALL_LOOKUPS_FOUND.value == "all_lookups_found"
        assert TestActionType.CUSTOM_SQL.value == "custom_sql"
        assert TestActionType.CUSTOM_EXPECTATIONS.value == "custom_expectations"
    
    def test_violation_action_enum(self):
        """Test ViolationAction enum exists with required values."""
        # Test that ViolationAction enum exists
        assert ViolationAction is not None
        
        # Test violation action values
        assert ViolationAction.FAIL.value == "fail"
        assert ViolationAction.WARN.value == "warn"
    
    def test_action_model(self):
        """Test Action model creation."""
        action = Action(
            name="test_action",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/test/path"},
            target="test_view",
            description="Test action"
        )
        assert action.name == "test_action"
        assert action.type == ActionType.LOAD
        assert action.target == "test_view"
    
    def test_flowgroup_model(self):
        """Test FlowGroup model creation."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[
                Action(name="load_data", type=ActionType.LOAD, target="raw_data"),
                Action(name="clean_data", type=ActionType.TRANSFORM, source="raw_data", target="clean_data")
            ]
        )
        assert flowgroup.pipeline == "test_pipeline"
        assert len(flowgroup.actions) == 2
        assert flowgroup.presets == ["bronze_layer"]
    
    def test_preset_model(self):
        """Test Preset model creation."""
        preset = Preset(
            name="bronze_layer",
            version="1.0",
            extends="base_preset",
            description="Bronze layer preset",
            defaults={"schema_evolution": "addNewColumns"}
        )
        assert preset.name == "bronze_layer"
        assert preset.extends == "base_preset"
        assert preset.defaults.get("schema_evolution") == "addNewColumns"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
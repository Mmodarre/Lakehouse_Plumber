"""Tests for core data models of LakehousePlumber."""

import pytest
from lhp.models.config import ActionType, LoadSourceType, TransformType, WriteTargetType, Action, FlowGroup, Template, Preset


class TestModels:
    """Test the core data models."""
    
    def test_action_type_enum(self):
        """Test ActionType enum values."""
        assert ActionType.LOAD.value == "load"
        assert ActionType.TRANSFORM.value == "transform"
        assert ActionType.WRITE.value == "write"
    
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
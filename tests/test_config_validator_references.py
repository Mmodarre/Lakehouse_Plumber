"""
Reference and rule validation tests for ConfigValidator.
"""

import pytest
from unittest.mock import patch
from lhp.core.validator import ConfigValidator
from lhp.models.config import FlowGroup, Action, ActionType, TransformType


class TestConfigValidatorReferences:
    """Reference and rule validation tests for ConfigValidator."""
    
    def test_action_references_validation_comprehensive(self):
        """Test action references validation comprehensively.
        
        Target lines: 604-625
        Tests invalid view references and external source handling.
        """
        validator = ConfigValidator()
        
        # Test 1: Invalid view reference (lines 604-625)
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "delta", "table": "bronze.customers"}
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_nonexistent_view",  # Invalid reference
                target="v_transformed_data"
            )
        ]
        
        errors = validator.validate_action_references(actions)
        assert any("references view 'v_nonexistent_view' which is not defined" in error for error in errors)
        
        # Test 2: Valid view reference (should NOT error)
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "delta", "table": "bronze.customers"}
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="v_raw_data",  # Valid reference
                target="v_transformed_data"
            )
        ]
        
        errors = validator.validate_action_references(actions)
        assert len(errors) == 0
        
        # Test 3: External source handling (should NOT error)
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "delta", "table": "bronze.customers"}  # External source
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source="bronze.raw_table",  # External source (not v_*)
                target="v_transformed_data"
            )
        ]
        
        errors = validator.validate_action_references(actions)
        assert len(errors) == 0  # External sources should not cause errors
        
        # Test 4: List source with invalid reference
        actions = [
            Action(
                name="load_data",
                type=ActionType.LOAD,
                target="v_raw_data",
                source={"type": "delta", "table": "bronze.customers"}
            ),
            Action(
                name="transform_data",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,
                source=["v_raw_data", "v_missing_view"],  # Mixed valid and invalid
                target="v_transformed_data"
            )
        ]
        
        errors = validator.validate_action_references(actions)
        assert any("references view 'v_missing_view' which is not defined" in error for error in errors)

    def test_source_extraction_edge_cases(self):
        """Test source extraction edge cases.
        
        Target lines: 629-644
        Tests complex source configurations and nested dict sources.
        """
        validator = ConfigValidator()
        
        # Test 1: Complex dict source with nested references
        action = Action(
            name="complex_source",
            type=ActionType.LOAD,
            target="v_complex",
            source={
                "type": "delta",
                "view": "v_nested_view",
                "sources": ["v_source1", "v_source2"],
                "table": "bronze.raw_data"
            }
        )
        
        # Extract sources using the private method
        sources = validator._extract_all_sources(action)
        
        # Should extract all source references
        assert "v_nested_view" in sources
        assert "v_source1" in sources
        assert "v_source2" in sources
        
        # Test 2: Dict source with 'views' field
        action = Action(
            name="views_source",
            type=ActionType.LOAD,
            target="v_views",
            source={
                "type": "delta",
                "views": ["v_view1", "v_view2"]
            }
        )
        
        sources = validator._extract_all_sources(action)
        assert "v_view1" in sources
        assert "v_view2" in sources

    def test_table_creation_rules_validation(self):
        """Test table creation rules validation.
        
        Target lines: 672, 759-760, 763, 770, 785-792
        Tests multiple creators, table name extraction, and action creates table logic.
        """
        validator = ConfigValidator()
        
        # Test 1: Multiple table creators (should raise LHPError)
        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="creator1",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "database": "test",
                            "table": "duplicate_table",
                            "create_table": True
                        }
                    ),
                    Action(
                        name="creator2",
                        type=ActionType.WRITE,
                        source="v_source2",
                        write_target={
                            "type": "streaming_table",
                            "database": "test",
                            "table": "duplicate_table",
                            "create_table": True
                        }
                    )
                ]
            )
        ]
        
        # Should raise LHPError for multiple creators
        with pytest.raises(Exception) as exc_info:
            validator.validate_table_creation_rules(flowgroups)
        
        assert "Multiple table creators detected" in str(exc_info.value)
        
        # Test 2: Table with no creators (should error)
        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="user_only",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "database": "test",
                            "table": "no_creator_table",
                            "create_table": False
                        }
                    )
                ]
            )
        ]
        
        errors = validator.validate_table_creation_rules(flowgroups)
        assert any("has no creator" in error for error in errors)
        
        # Test 3: Valid table creation (should NOT error)
        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="creator",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "database": "test",
                            "table": "valid_table",
                            "create_table": True
                        }
                    ),
                    Action(
                        name="user",
                        type=ActionType.WRITE,
                        source="v_source2",
                        write_target={
                            "type": "streaming_table",
                            "database": "test",
                            "table": "valid_table",
                            "create_table": False
                        }
                    )
                ]
            )
        ]
        
        errors = validator.validate_table_creation_rules(flowgroups)
        assert len(errors) == 0

    def test_template_usage_warning(self):
        """Test template usage warning.
        
        Target line: 14
        Tests FlowGroup with use_template but no template_parameters.
        """
        validator = ConfigValidator()
        
        # Test 1: FlowGroup with use_template but no template_parameters (should warn)
        with patch.object(validator.logger, 'warning') as mock_warning:
            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                use_template="test_template",  # Has use_template
                # Missing template_parameters
                actions=[
                    Action(
                        name="test_action",
                        type=ActionType.LOAD,
                        target="v_test",
                        source={"type": "delta", "table": "test"}
                    )
                ]
            )
            
            errors = validator.validate_flowgroup(flowgroup)
            
            # Should log warning (line 14)
            mock_warning.assert_called_once()
            warning_call = mock_warning.call_args[0][0]
            assert "FlowGroup uses template 'test_template' but no parameters provided" in warning_call


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
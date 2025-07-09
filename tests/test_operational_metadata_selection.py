"""Tests for operational metadata selection logic."""

import pytest
import tempfile
from pathlib import Path
from typing import Dict, Any

from lhp.models.config import (
    MetadataColumnConfig, 
    ProjectOperationalMetadataConfig,
    OperationalMetadataSelection,
    FlowGroup,
    Action,
    ActionType,
    WriteTarget,
    WriteTargetType
)
from lhp.utils.operational_metadata import OperationalMetadata


class TestOperationalMetadataSelection:
    """Test operational metadata selection logic."""
    
    @pytest.fixture
    def sample_project_config(self):
        """Sample project-level metadata configuration."""
        return ProjectOperationalMetadataConfig(
            columns={
                '_ingestion_timestamp': MetadataColumnConfig(
                    expression='F.current_timestamp()',
                    description='When record was ingested',
                    applies_to=['streaming_table', 'materialized_view']
                ),
                '_source_file': MetadataColumnConfig(
                    expression='F.input_file_name()',
                    description='Source file path',
                    applies_to=['streaming_table']  # Only streaming tables
                ),
                '_pipeline_name': MetadataColumnConfig(
                    expression='F.lit("${pipeline_name}")',
                    description='Pipeline name',
                    applies_to=['streaming_table', 'materialized_view']
                ),
                '_business_quarter': MetadataColumnConfig(
                    expression='F.concat(F.lit("Q"), F.quarter(F.current_date()))',
                    description='Business quarter',
                    applies_to=['streaming_table', 'materialized_view']
                ),
                '_data_classification': MetadataColumnConfig(
                    expression='F.lit("PII")',
                    description='Data classification',
                    applies_to=['streaming_table', 'materialized_view']
                ),
                '_custom_function': MetadataColumnConfig(
                    expression='custom_business_logic(F.col("revenue"))',
                    description='Custom business logic',
                    applies_to=['streaming_table'],
                    additional_imports=['from my_project.logic import custom_business_logic']
                )
            },
            presets={
                'minimal': {
                    'columns': ['_ingestion_timestamp', '_pipeline_name']
                },
                'standard': {
                    'columns': ['_ingestion_timestamp', '_source_file', '_pipeline_name']
                },
                'enhanced': {
                    'columns': ['_ingestion_timestamp', '_source_file', '_pipeline_name', '_business_quarter']
                }
            }
        )
    
    @pytest.fixture
    def metadata_handler(self, sample_project_config):
        """Operational metadata handler with sample config."""
        return OperationalMetadata(project_config=sample_project_config)
    
    @pytest.fixture
    def sample_flowgroup(self):
        """Sample flowgroup for testing."""
        return FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
    
    @pytest.fixture
    def sample_write_action(self):
        """Sample write action for testing."""
        return Action(
            name='write_test_table',
            type=ActionType.WRITE,
            source='v_test_data',
            write_target=WriteTarget(
                type=WriteTargetType.STREAMING_TABLE,
                database='test_catalog.bronze',
                table='test_table'
            )
        )
    
    @pytest.fixture
    def preset_config_standard(self):
        """Sample preset configuration with standard metadata."""
        return {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
    
    @pytest.fixture
    def preset_config_empty(self):
        """Sample preset configuration with no metadata."""
        return {}

    # ========================================================================
    # Basic Additive Logic Tests
    # ========================================================================
    
    def test_preset_only_selection(self, metadata_handler):
        """Test that preset-only selection works correctly."""
        # Simulate preset selecting some columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        # FlowGroup and Action don't add anything
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        # Update context for substitutions
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Resolve selection - should only have preset columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()'
        }
        
        assert columns == expected_columns, f"Expected {expected_columns}, got {columns}"
    
    def test_flowgroup_adds_to_preset(self, metadata_handler):
        """Test that flowgroup adds columns to preset selection."""
        # Preset selects base columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        # FlowGroup adds more columns
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_pipeline_name', '_business_quarter'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should combine preset + flowgroup columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',  # Context substituted
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))'
        }
        
        assert columns == expected_columns, f"Expected {expected_columns}, got {columns}"
    
    def test_action_adds_to_flowgroup_and_preset(self, metadata_handler):
        """Test that action adds columns to both flowgroup and preset selections."""
        # Preset selects base columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp']
        }
        
        # FlowGroup adds some columns
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_source_file'],
            actions=[]
        )
        
        # Action adds even more columns
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_data_classification', '_business_quarter'],
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should combine all: preset + flowgroup + action columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_data_classification': 'F.lit("PII")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))'
        }
        
        assert columns == expected_columns, f"Expected {expected_columns}, got {columns}"
    
    def test_no_duplicates_in_additive_selection(self, metadata_handler):
        """Test that duplicate column selections don't create duplicates."""
        # Preset selects base columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        # FlowGroup adds same columns (should not duplicate)
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_ingestion_timestamp', '_pipeline_name'],  # _ingestion_timestamp duplicated
            actions=[]
        )
        
        # Action also adds same column
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_source_file', '_data_classification'],  # _source_file duplicated
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Should have each column only once
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_data_classification': 'F.lit("PII")'
        }
        
        assert columns == expected_columns, f"Expected {expected_columns}, got {columns}"
        
        # Check that each column appears exactly once
        assert len(columns) == 4, f"Expected 4 unique columns, got {len(columns)}"

    # ========================================================================
    # Action Disable Logic Tests
    # ========================================================================
    
    def test_action_disables_all_metadata(self, metadata_handler):
        """Test that action-level disable (false) overrides all metadata selections."""
        # Preset selects columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        # FlowGroup adds more columns
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_pipeline_name', '_business_quarter'],
            actions=[]
        )
        
        # Action DISABLES all metadata
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=False,  # Disables all metadata
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should have NO columns despite preset and flowgroup selections
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        assert columns == {}, f"Expected no columns when disabled, got {columns}"
    
    def test_action_disable_overrides_large_selection(self, metadata_handler):
        """Test that action disable works even with large preset/flowgroup selections."""
        # Large preset selection
        preset_config = {
            'operational_metadata': [
                '_ingestion_timestamp', '_source_file', '_pipeline_name', 
                '_business_quarter', '_data_classification'
            ]
        }
        
        # FlowGroup adds more
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_custom_function'],
            actions=[]
        )
        
        # Action disables everything
        action = Action(
            name='write_temp_table',
            type=ActionType.WRITE,
            source='v_temp_data',
            operational_metadata=False,
            write_target={'type': 'streaming_table', 'database': 'temp.tables', 'table': 'temp_data'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        assert columns == {}, f"Expected no columns when disabled, got {columns}"
    
    def test_action_inherit_when_not_specified(self, metadata_handler):
        """Test that action inherits flowgroup/preset selection when not specified."""
        # Preset selects columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp']
        }
        
        # FlowGroup adds more columns
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_source_file', '_pipeline_name'],
            actions=[]
        )
        
        # Action doesn't specify operational_metadata (should inherit)
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            # No operational_metadata specified = inherit
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should inherit preset + flowgroup columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns == expected_columns, f"Expected inherited columns, got {columns}"
    
    def test_boolean_false_vs_none_vs_list(self, metadata_handler):
        """Test different ways to specify action-level metadata behavior."""
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_pipeline_name'],
            actions=[]
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Test 1: False = disable all
        action_false = Action(
            name='write_test_false',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=False,
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_false = metadata_handler.resolve_metadata_selection(flowgroup, action_false, preset_config)
        columns_false = metadata_handler.get_selected_columns(selection_false or {}, 'streaming_table')
        assert columns_false == {}, "False should disable all metadata"
        
        # Test 2: None = inherit
        action_none = Action(
            name='write_test_none',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=None,
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_none = metadata_handler.resolve_metadata_selection(flowgroup, action_none, preset_config)
        columns_none = metadata_handler.get_selected_columns(selection_none or {}, 'streaming_table')
        expected_inherit = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        assert columns_none == expected_inherit, "None should inherit flowgroup+preset"
        
        # Test 3: List = add to inheritance
        action_list = Action(
            name='write_test_list',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_data_classification'],
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_list = metadata_handler.resolve_metadata_selection(flowgroup, action_list, preset_config)
        columns_list = metadata_handler.get_selected_columns(selection_list or {}, 'streaming_table')
        expected_additive = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_data_classification': 'F.lit("PII")'
        }
        assert columns_list == expected_additive, "List should add to inheritance"

    # ========================================================================
    # Edge Cases Tests
    # ========================================================================
    
    def test_no_preset_specified(self, metadata_handler):
        """Test behavior when no preset is specified."""
        # No preset configuration
        preset_config = {}
        
        # FlowGroup specifies columns directly
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=[],  # No presets
            operational_metadata=['_ingestion_timestamp', '_pipeline_name'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should work with just flowgroup columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns == expected_columns, f"Expected flowgroup-only columns, got {columns}"
    
    def test_empty_preset_configuration(self, metadata_handler):
        """Test behavior when preset exists but has no operational_metadata."""
        # Preset with no operational metadata
        preset_config = {
            'load_actions': {'cloudfiles': {'format': 'json'}},
            # No 'operational_metadata' key
        }
        
        # FlowGroup adds columns
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['empty_preset'],
            operational_metadata=['_ingestion_timestamp'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should work with just flowgroup columns (preset contributes nothing)
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()'
        }
        
        assert columns == expected_columns, f"Expected flowgroup-only columns, got {columns}"
    
    def test_unknown_column_reference(self, metadata_handler):
        """Test behavior when referencing columns not defined in project config."""
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp']
        }
        
        # FlowGroup tries to add unknown column
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_unknown_column', '_pipeline_name'],  # _unknown_column not in project
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_another_unknown'],  # Another unknown column
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should only include known columns, ignore unknown ones
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Only known columns should be included
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns == expected_columns, f"Expected only known columns, got {columns}"
    
    def test_all_empty_selections(self, metadata_handler):
        """Test behavior when all levels have empty/no selections."""
        # Empty preset
        preset_config = {}
        
        # FlowGroup with no operational metadata
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=[],
            # No operational_metadata specified
            actions=[]
        )
        
        # Action with no operational metadata
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            # No operational_metadata specified
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should result in no columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        assert columns == {}, f"Expected no columns for empty selections, got {columns}"
    
    def test_malformed_operational_metadata_config(self, metadata_handler):
        """Test behavior with malformed operational metadata configurations."""
        # Malformed preset config
        preset_config = {
            'operational_metadata': 'not_a_list'  # Should be a list
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['malformed_preset'],
            operational_metadata=['_ingestion_timestamp'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should gracefully handle malformed config and use flowgroup
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Should fall back to flowgroup selection
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()'
        }
        
        assert columns == expected_columns, f"Expected fallback to flowgroup, got {columns}"
    
    def test_empty_column_list_selections(self, metadata_handler):
        """Test behavior with explicitly empty column lists."""
        # Preset with empty list
        preset_config = {
            'operational_metadata': []  # Explicitly empty
        }
        
        # FlowGroup with empty list
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['empty_list_preset'],
            operational_metadata=[],  # Explicitly empty
            actions=[]
        )
        
        # Action with empty list
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=[],  # Explicitly empty
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should result in no columns (empty + empty + empty = empty)
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        assert columns == {}, f"Expected no columns for empty lists, got {columns}"

    # ========================================================================
    # Target Type Filtering Tests
    # ========================================================================
    
    def test_source_file_only_for_streaming_tables(self, metadata_handler):
        """Test that _source_file column only applies to streaming tables."""
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file', '_pipeline_name']
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Test 1: Streaming table should get all columns including _source_file
        action_streaming = Action(
            name='write_streaming_table',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_streaming = metadata_handler.resolve_metadata_selection(flowgroup, action_streaming, preset_config)
        columns_streaming = metadata_handler.get_selected_columns(selection_streaming or {}, 'streaming_table')
        
        expected_streaming = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',  # Should be included
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns_streaming == expected_streaming, f"Streaming table should include _source_file, got {columns_streaming}"
        
        # Test 2: Materialized view should exclude _source_file
        action_materialized = Action(
            name='write_materialized_view',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'materialized_view', 'database': 'test.silver', 'table': 'test'}
        )
        
        selection_materialized = metadata_handler.resolve_metadata_selection(flowgroup, action_materialized, preset_config)
        columns_materialized = metadata_handler.get_selected_columns(selection_materialized or {}, 'materialized_view')
        
        expected_materialized = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            # _source_file should be excluded
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns_materialized == expected_materialized, f"Materialized view should exclude _source_file, got {columns_materialized}"
    
    def test_custom_function_target_filtering(self, metadata_handler):
        """Test that custom function column with specific target filtering works."""
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_custom_function', '_pipeline_name']
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # _custom_function only applies to streaming_table (per config)
        
        # Test 1: Streaming table should get custom function
        action_streaming = Action(
            name='write_streaming_table',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_streaming = metadata_handler.resolve_metadata_selection(flowgroup, action_streaming, preset_config)
        columns_streaming = metadata_handler.get_selected_columns(selection_streaming or {}, 'streaming_table')
        
        expected_streaming = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_custom_function': 'custom_business_logic(F.col("revenue"))',  # Should be included
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns_streaming == expected_streaming, f"Streaming table should include custom function, got {columns_streaming}"
        
        # Test 2: Materialized view should exclude custom function
        action_materialized = Action(
            name='write_materialized_view',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'materialized_view', 'database': 'test.silver', 'table': 'test'}
        )
        
        selection_materialized = metadata_handler.resolve_metadata_selection(flowgroup, action_materialized, preset_config)
        columns_materialized = metadata_handler.get_selected_columns(selection_materialized or {}, 'materialized_view')
        
        expected_materialized = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            # _custom_function should be excluded (only applies to streaming_table)
            '_pipeline_name': 'F.lit("test_pipeline")'
        }
        
        assert columns_materialized == expected_materialized, f"Materialized view should exclude custom function, got {columns_materialized}"
    
    def test_mixed_target_type_filtering(self, metadata_handler):
        """Test complex scenario with multiple columns having different target type restrictions."""
        # Select columns with various target type restrictions
        preset_config = {
            'operational_metadata': [
                '_ingestion_timestamp',    # Both streaming_table and materialized_view
                '_source_file',           # Only streaming_table
                '_pipeline_name',         # Both streaming_table and materialized_view
                '_business_quarter',      # Both streaming_table and materialized_view
                '_custom_function'        # Only streaming_table
            ]
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Test streaming table gets all applicable columns
        action_streaming = Action(
            name='write_streaming_table',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_streaming = metadata_handler.resolve_metadata_selection(flowgroup, action_streaming, preset_config)
        columns_streaming = metadata_handler.get_selected_columns(selection_streaming or {}, 'streaming_table')
        
        expected_streaming = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            '_custom_function': 'custom_business_logic(F.col("revenue"))'
        }
        
        assert columns_streaming == expected_streaming, f"Streaming table should get all applicable columns, got {columns_streaming}"
        
        # Test materialized view gets only applicable columns
        action_materialized = Action(
            name='write_materialized_view',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'materialized_view', 'database': 'test.silver', 'table': 'test'}
        )
        
        selection_materialized = metadata_handler.resolve_metadata_selection(flowgroup, action_materialized, preset_config)
        columns_materialized = metadata_handler.get_selected_columns(selection_materialized or {}, 'materialized_view')
        
        expected_materialized = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            # _source_file excluded (streaming_table only)
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            # _custom_function excluded (streaming_table only)
        }
        
        assert columns_materialized == expected_materialized, f"Materialized view should exclude streaming-only columns, got {columns_materialized}"
    
    def test_target_type_filtering_with_action_additions(self, metadata_handler):
        """Test that target type filtering works when action adds columns."""
        # Preset selects universal columns
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_pipeline_name']
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_business_quarter'],  # Universal column
            actions=[]
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Action tries to add streaming-only columns
        action_streaming = Action(
            name='write_streaming_table',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_source_file', '_custom_function'],  # Both streaming-only
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_streaming = metadata_handler.resolve_metadata_selection(flowgroup, action_streaming, preset_config)
        columns_streaming = metadata_handler.get_selected_columns(selection_streaming or {}, 'streaming_table')
        
        # Streaming table should get all columns (preset + flowgroup + action)
        expected_streaming = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            '_source_file': 'F.input_file_name()',
            '_custom_function': 'custom_business_logic(F.col("revenue"))'
        }
        
        assert columns_streaming == expected_streaming, f"Streaming table should get all columns, got {columns_streaming}"
        
        # Same action config but materialized view target
        action_materialized = Action(
            name='write_materialized_view',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_source_file', '_custom_function'],  # Both will be filtered out
            write_target={'type': 'materialized_view', 'database': 'test.silver', 'table': 'test'}
        )
        
        selection_materialized = metadata_handler.resolve_metadata_selection(flowgroup, action_materialized, preset_config)
        columns_materialized = metadata_handler.get_selected_columns(selection_materialized or {}, 'materialized_view')
        
        # Materialized view should exclude streaming-only columns
        expected_materialized = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            # _source_file and _custom_function excluded (streaming_table only)
        }
        
        assert columns_materialized == expected_materialized, f"Materialized view should exclude streaming-only columns, got {columns_materialized}" 

    # ========================================================================
    # AST Import Detection Tests
    # ========================================================================
    
    def test_simple_function_import_detection(self, metadata_handler):
        """Test AST import detection for simple PySpark functions."""
        from lhp.utils.operational_metadata import ImportDetector
        
        detector = ImportDetector(strategy='ast')
        
        # Test individual functions
        test_cases = [
            ('F.current_timestamp()', {'from pyspark.sql import functions as F'}),
            ('F.input_file_name()', {'from pyspark.sql import functions as F'}),
            ('F.lit("test")', {'from pyspark.sql import functions as F'}),
            ('F.col("column_name")', {'from pyspark.sql import functions as F'}),
            ('spark.conf.get("key", "default")', set()),  # Built-in, no import needed
        ]
        
        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, f"Expression '{expression}' should import {expected_imports}, got {detected}"
    
    def test_complex_expression_import_detection(self, metadata_handler):
        """Test AST import detection for complex PySpark expressions."""
        from lhp.utils.operational_metadata import ImportDetector
        
        detector = ImportDetector(strategy='ast')
        
        # Complex expressions that use multiple functions
        test_cases = [
            # when/otherwise expression
            ('F.when(F.col("key").isNotNull(), F.lit("high")).otherwise(F.lit("low"))', 
             {'from pyspark.sql import functions as F'}),
            
            # Concatenation with date functions
            ('F.concat(F.lit("Q"), F.quarter(F.current_date()), F.lit("-"), F.year(F.current_date()))',
             {'from pyspark.sql import functions as F'}),
            
            # Complex conditional with regex
            ('F.when(F.col("email").rlike(r"^[^@]+@[^@]+\\.[^@]+$"), F.lit("valid")).otherwise(F.lit("invalid"))',
             {'from pyspark.sql import functions as F'}),
            
            # Mixed with spark config
            ('F.lit(spark.conf.get("pipelines.id", "unknown"))',
             {'from pyspark.sql import functions as F'}),
        ]
        
        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, f"Complex expression should import {expected_imports}, got {detected}"
    
    def test_custom_function_import_detection(self, metadata_handler):
        """Test that custom functions don't get automatic imports (require explicit config)."""
        from lhp.utils.operational_metadata import ImportDetector
        
        detector = ImportDetector(strategy='ast')
        
        # Custom function calls should not generate automatic imports
        test_cases = [
            ('custom_business_logic(F.col("revenue"))', {'from pyspark.sql import functions as F'}),  # Only F. functions
            ('assess_data_quality(F.col("key"), F.col("email"))', {'from pyspark.sql import functions as F'}),
            ('my_udf(F.col("data"))', {'from pyspark.sql import functions as F'}),
        ]
        
        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, f"Custom function should only import PySpark functions, got {detected}"
    
    def test_metadata_handler_import_collection(self, metadata_handler):
        """Test that metadata handler correctly collects imports from selected columns."""
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_business_quarter', '_custom_function']
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Get selected columns
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Get required imports
        imports = metadata_handler.get_required_imports(columns)
        
        expected_imports = {
            'from pyspark.sql import functions as F',  # From F.current_timestamp(), F.concat(), F.quarter(), etc.
            'from my_project.logic import custom_business_logic'  # From additional_imports config
        }
        
        assert imports == expected_imports, f"Should collect all required imports, got {imports}"
    
    def test_no_imports_needed_for_empty_selection(self, metadata_handler):
        """Test that no imports are generated when no columns are selected."""
        preset_config = {}
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=[],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=False,  # Disabled
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # No columns selected
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # No imports needed
        imports = metadata_handler.get_required_imports(columns)
        
        assert imports == set(), f"No imports should be needed for empty selection, got {imports}"
    
    def test_udf_import_detection(self, metadata_handler):
        """Test that UDF-related functions get correct imports."""
        from lhp.utils.operational_metadata import ImportDetector
        
        detector = ImportDetector(strategy='ast')
        
        test_cases = [
            ('udf(lambda x: x.upper(), StringType())', 
             {'from pyspark.sql.functions import udf', 'from pyspark.sql.types import StringType'}),
            
            ('pandas_udf(my_function, StringType())',
             {'from pyspark.sql.functions import pandas_udf', 'from pyspark.sql.types import StringType'}),
             
            ('broadcast(F.col("lookup_table"))',
             {'from pyspark.sql.functions import broadcast', 'from pyspark.sql import functions as F'}),
        ]
        
        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, f"UDF expression should import {expected_imports}, got {detected}"
    
    def test_fallback_regex_detection(self, metadata_handler):
        """Test that malformed expressions fall back to regex detection."""
        from lhp.utils.operational_metadata import ImportDetector
        
        detector = ImportDetector(strategy='ast')
        
        # Malformed Python expressions that can't be parsed by AST
        test_cases = [
            ('F.current_timestamp() +', {'from pyspark.sql import functions as F'}),  # Incomplete expression
            ('F.when(', {'from pyspark.sql import functions as F'}),  # Incomplete parentheses
            ('# F.lit("comment")', set()),  # Comment - should not detect
        ]
        
        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            # Should use fallback regex detection
            assert 'from pyspark.sql import functions as F' in detected or len(expected_imports) == 0, \
                f"Fallback should detect F. functions in '{expression}', got {detected}"
    
    def test_import_deduplication(self, metadata_handler):
        """Test that duplicate imports are correctly deduplicated."""
        preset_config = {
            'operational_metadata': [
                '_ingestion_timestamp',  # F.current_timestamp()
                '_pipeline_name',       # F.lit()
                '_business_quarter'     # F.concat(), F.quarter(), etc.
            ]
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_data_classification'],  # Another F.lit()
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        imports = metadata_handler.get_required_imports(columns)
        
        # Should only have one instance of the F import despite multiple F. functions
        expected_imports = {'from pyspark.sql import functions as F'}
        
        assert imports == expected_imports, f"Should deduplicate imports, got {imports}"
        
        # Verify we don't have duplicate entries
        import_list = list(imports)
        assert len(import_list) == len(set(import_list)), "Should not have duplicate imports"

    # ========================================================================
    # Backward Compatibility Tests
    # ========================================================================
    
    def test_boolean_true_backward_compatibility(self, metadata_handler):
        """Test that operational_metadata: true still works (uses defaults)."""
        # No specific columns configured, should use project defaults
        preset_config = {
            'operational_metadata': True  # Old boolean style
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=True,  # Old boolean style
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=True,  # Old boolean style
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should use default columns from project config
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Should get all applicable project columns for streaming table
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            '_data_classification': 'F.lit("PII")',
            '_custom_function': 'custom_business_logic(F.col("revenue"))'
        }
        
        assert columns == expected_columns, f"Boolean true should use all project columns, got {columns}"
    
    def test_boolean_false_backward_compatibility(self, metadata_handler):
        """Test that operational_metadata: false still works (disables metadata)."""
        # Preset tries to enable metadata
        preset_config = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file']
        }
        
        # FlowGroup tries to add more
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_pipeline_name'],
            actions=[]
        )
        
        # Action disables with boolean false
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=False,  # Old boolean style disable
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should disable all metadata despite preset and flowgroup
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        assert columns == {}, f"Boolean false should disable all metadata, got {columns}"
    
    def test_mixed_boolean_and_list_configurations(self, metadata_handler):
        """Test mixing old boolean and new list configurations."""
        # Preset uses old boolean style
        preset_config = {
            'operational_metadata': True  # Boolean - should use defaults
        }
        
        # FlowGroup uses new list style
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_business_quarter'],  # New list style
            actions=[]
        )
        
        # Action uses new list style
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=True,  # Still boolean
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        # Should combine boolean defaults + list additions
        selection = metadata_handler.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler.get_selected_columns(selection or {}, 'streaming_table')
        
        # Should get all project columns (from boolean true)
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            '_data_classification': 'F.lit("PII")',
            '_custom_function': 'custom_business_logic(F.col("revenue"))'
        }
        
        assert columns == expected_columns, f"Should use all project columns when boolean is true, got {columns}"
    
    def test_no_project_config_backward_compatibility(self, metadata_handler):
        """Test behavior when no project config exists (fallback to hardcoded defaults)."""
        # Create metadata handler without project config
        metadata_handler_no_config = OperationalMetadata(project_config=None)
        
        preset_config = {
            'operational_metadata': True  # Should use hardcoded defaults
        }
        
        flowgroup = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            actions=[]
        )
        
        action = Action(
            name='write_test',
            type=ActionType.WRITE,
            source='v_test',
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler_no_config.update_context('test_pipeline', 'test_flowgroup')
        
        # Should fall back to hardcoded default columns
        selection = metadata_handler_no_config.resolve_metadata_selection(flowgroup, action, preset_config)
        columns = metadata_handler_no_config.get_selected_columns(selection or {}, 'streaming_table')
        
        # Should get hardcoded defaults (fallback when no project config)
        # Note: _source_file now only applies to 'view' target type, not 'streaming_table'
        expected_columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_run_id': 'F.lit(spark.conf.get("pipelines.id", "unknown"))',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_flowgroup_name': 'F.lit("test_flowgroup")'
        }
        
        assert columns == expected_columns, f"Should use hardcoded defaults when no project config, got {columns}"
    
    def test_migration_from_boolean_to_list(self, metadata_handler):
        """Test gradual migration from boolean to list configurations."""
        # Scenario: User is migrating from boolean to list style
        
        # Stage 1: All boolean (old style)
        preset_config_old = {
            'operational_metadata': True
        }
        
        flowgroup_old = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=True,
            actions=[]
        )
        
        action_old = Action(
            name='write_test_old',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=True,
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        metadata_handler.update_context('test_pipeline', 'test_flowgroup')
        
        selection_old = metadata_handler.resolve_metadata_selection(flowgroup_old, action_old, preset_config_old)
        columns_old = metadata_handler.get_selected_columns(selection_old or {}, 'streaming_table')
        
        # Stage 2: Partial migration (flowgroup uses list, others boolean)
        flowgroup_mixed = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_business_quarter'],  # New list style
            actions=[]
        )
        
        action_mixed = Action(
            name='write_test_mixed',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=True,  # Still boolean
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_mixed = metadata_handler.resolve_metadata_selection(flowgroup_mixed, action_mixed, preset_config_old)
        columns_mixed = metadata_handler.get_selected_columns(selection_mixed or {}, 'streaming_table')
        
        # Stage 3: Full migration (all list style)
        preset_config_new = {
            'operational_metadata': ['_ingestion_timestamp', '_source_file', '_pipeline_name']
        }
        
        flowgroup_new = FlowGroup(
            pipeline='test_pipeline',
            flowgroup='test_flowgroup',
            presets=['bronze_layer'],
            operational_metadata=['_business_quarter'],
            actions=[]
        )
        
        action_new = Action(
            name='write_test_new',
            type=ActionType.WRITE,
            source='v_test',
            operational_metadata=['_data_classification'],
            write_target={'type': 'streaming_table', 'database': 'test.bronze', 'table': 'test'}
        )
        
        selection_new = metadata_handler.resolve_metadata_selection(flowgroup_new, action_new, preset_config_new)
        columns_new = metadata_handler.get_selected_columns(selection_new or {}, 'streaming_table')
        
        # All stages should work without errors
        assert len(columns_old) > 0, "Old boolean style should work"
        assert len(columns_mixed) > 0, "Mixed style should work"  
        assert len(columns_new) > 0, "New list style should work"
        
        # Mixed should be additive (boolean defaults + list additions)
        assert '_business_quarter' in columns_mixed, "Mixed style should add to boolean defaults"
        
        # New should have explicit selection only
        assert columns_new == {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_source_file': 'F.input_file_name()',
            '_pipeline_name': 'F.lit("test_pipeline")',
            '_business_quarter': 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            '_data_classification': 'F.lit("PII")'
        }, "New style should have explicit selections only" 
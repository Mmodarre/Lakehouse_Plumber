"""Tests for operational metadata selection logic."""

import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from lhp.core.codegen.operational_metadata import OperationalMetadataCatalog
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    OperationalMetadataSelection,
    ProjectOperationalMetadataConfig,
    WriteTarget,
    WriteTargetType,
)


class TestOperationalMetadataSelection:
    @pytest.fixture
    def sample_project_config(self):
        return ProjectOperationalMetadataConfig(
            columns={
                "_ingestion_timestamp": MetadataColumnConfig(
                    expression="F.current_timestamp()",
                    description="When record was ingested",
                    applies_to=["streaming_table", "materialized_view"],
                ),
                "_source_file": MetadataColumnConfig(
                    expression="F.input_file_name()",
                    description="Source file path",
                    applies_to=["streaming_table"],  # Only streaming tables
                ),
                "_pipeline_name": MetadataColumnConfig(
                    expression='F.lit("${pipeline_name}")',
                    description="Pipeline name",
                    applies_to=["streaming_table", "materialized_view"],
                ),
                "_business_quarter": MetadataColumnConfig(
                    expression='F.concat(F.lit("Q"), F.quarter(F.current_date()))',
                    description="Business quarter",
                    applies_to=["streaming_table", "materialized_view"],
                ),
                "_data_classification": MetadataColumnConfig(
                    expression='F.lit("PII")',
                    description="Data classification",
                    applies_to=["streaming_table", "materialized_view"],
                ),
                "_custom_function": MetadataColumnConfig(
                    expression='custom_business_logic(F.col("revenue"))',
                    description="Custom business logic",
                    applies_to=["streaming_table"],
                    additional_imports=[
                        "from my_project.logic import custom_business_logic"
                    ],
                ),
            },
            presets={
                "minimal": {"columns": ["_ingestion_timestamp", "_pipeline_name"]},
                "standard": {
                    "columns": [
                        "_ingestion_timestamp",
                        "_source_file",
                        "_pipeline_name",
                    ]
                },
                "enhanced": {
                    "columns": [
                        "_ingestion_timestamp",
                        "_source_file",
                        "_pipeline_name",
                        "_business_quarter",
                    ]
                },
            },
        )

    @pytest.fixture
    def metadata_handler(self, sample_project_config):
        return OperationalMetadataCatalog(project_config=sample_project_config)

    @pytest.fixture
    def sample_flowgroup(self):
        return FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

    @pytest.fixture
    def sample_write_action(self):
        return Action(
            name="write_test_table",
            type=ActionType.WRITE,
            source="v_test_data",
            write_target=WriteTarget(
                type=WriteTargetType.STREAMING_TABLE,
                database="test_catalog.bronze",
                table="test_table",
            ),
        )

    @pytest.fixture
    def preset_config_standard(self):
        return {"operational_metadata": ["_ingestion_timestamp", "_source_file"]}

    @pytest.fixture
    def preset_config_empty(self):
        return {}

    def test_preset_only_selection(self, metadata_handler):
        """Test that preset-only selection works correctly."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
        }

        assert columns == expected_columns, (
            f"Expected {expected_columns}, got {columns}"
        )

    def test_flowgroup_adds_to_preset(self, metadata_handler):
        """Test that flowgroup adds columns to preset selection."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_pipeline_name", "_business_quarter"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',  # Context substituted
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
        }

        assert columns == expected_columns, (
            f"Expected {expected_columns}, got {columns}"
        )

    def test_action_adds_to_flowgroup_and_preset(self, metadata_handler):
        """Test that action adds columns to both flowgroup and preset selections."""
        preset_config = {"operational_metadata": ["_ingestion_timestamp"]}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_source_file"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=["_data_classification", "_business_quarter"],
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_data_classification": 'F.lit("PII")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
        }

        assert columns == expected_columns, (
            f"Expected {expected_columns}, got {columns}"
        )

    def test_no_duplicates_in_additive_selection(self, metadata_handler):
        """Test that duplicate column selections don't create duplicates."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=[
                "_ingestion_timestamp",
                "_pipeline_name",
            ],  # _ingestion_timestamp duplicated
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=[
                "_source_file",
                "_data_classification",
            ],  # _source_file duplicated
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_data_classification": 'F.lit("PII")',
        }

        assert columns == expected_columns, (
            f"Expected {expected_columns}, got {columns}"
        )

        # Check that each column appears exactly once
        assert len(columns) == 4, f"Expected 4 unique columns, got {len(columns)}"

    def test_action_disables_all_metadata(self, metadata_handler):
        """Test that action-level disable (false) overrides all metadata selections."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_pipeline_name", "_business_quarter"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=False,  # Disables all metadata
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        assert columns == {}, f"Expected no columns when disabled, got {columns}"

    def test_action_disable_overrides_large_selection(self, metadata_handler):
        """Test that action disable works even with large preset/flowgroup selections."""
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_source_file",
                "_pipeline_name",
                "_business_quarter",
                "_data_classification",
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_custom_function"],
            actions=[],
        )

        action = Action(
            name="write_temp_table",
            type=ActionType.WRITE,
            source="v_temp_data",
            operational_metadata=False,
            write_target={
                "type": "streaming_table",
                "database": "temp.tables",
                "table": "temp_data",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        assert columns == {}, f"Expected no columns when disabled, got {columns}"

    def test_action_inherit_when_not_specified(self, metadata_handler):
        """Test that action inherits flowgroup/preset selection when not specified."""
        preset_config = {"operational_metadata": ["_ingestion_timestamp"]}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_source_file", "_pipeline_name"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            # No operational_metadata specified = inherit
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns == expected_columns, f"Expected inherited columns, got {columns}"

    def test_boolean_false_vs_none_vs_list(self, metadata_handler):
        """Test different ways to specify action-level metadata behavior."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_pipeline_name"],
            actions=[],
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        action_false = Action(
            name="write_test_false",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=False,
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_false = metadata_handler.resolve_metadata_selection(
            flowgroup, action_false, preset_config
        )
        columns_false = metadata_handler.get_selected_columns(
            selection_false or {}, "streaming_table"
        )
        assert columns_false == {}, "False should disable all metadata"

        action_none = Action(
            name="write_test_none",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=None,
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_none = metadata_handler.resolve_metadata_selection(
            flowgroup, action_none, preset_config
        )
        columns_none = metadata_handler.get_selected_columns(
            selection_none or {}, "streaming_table"
        )
        expected_inherit = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
        }
        assert columns_none == expected_inherit, "None should inherit flowgroup+preset"

        action_list = Action(
            name="write_test_list",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=["_data_classification"],
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_list = metadata_handler.resolve_metadata_selection(
            flowgroup, action_list, preset_config
        )
        columns_list = metadata_handler.get_selected_columns(
            selection_list or {}, "streaming_table"
        )
        expected_additive = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_data_classification": 'F.lit("PII")',
        }
        assert columns_list == expected_additive, "List should add to inheritance"

    def test_no_preset_specified(self, metadata_handler):
        """Test behavior when no preset is specified."""
        preset_config = {}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=[],  # No presets
            operational_metadata=["_ingestion_timestamp", "_pipeline_name"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns == expected_columns, (
            f"Expected flowgroup-only columns, got {columns}"
        )

    def test_empty_preset_configuration(self, metadata_handler):
        """Test behavior when preset exists but has no operational_metadata."""
        preset_config = {
            "load_actions": {"cloudfiles": {"format": "json"}},
            # No 'operational_metadata' key
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["empty_preset"],
            operational_metadata=["_ingestion_timestamp"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {"_ingestion_timestamp": "F.current_timestamp()"}

        assert columns == expected_columns, (
            f"Expected flowgroup-only columns, got {columns}"
        )

    def test_unknown_column_reference(self, metadata_handler):
        """Test behavior when referencing columns not defined in project config."""
        preset_config = {"operational_metadata": ["_ingestion_timestamp"]}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=[
                "_unknown_column",
                "_pipeline_name",
            ],  # _unknown_column not in project
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=["_another_unknown"],  # Another unknown column
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns == expected_columns, (
            f"Expected only known columns, got {columns}"
        )

    def test_all_empty_selections(self, metadata_handler):
        """Test behavior when all levels have empty/no selections."""
        preset_config = {}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=[],
            # No operational_metadata specified
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            # No operational_metadata specified
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        assert columns == {}, f"Expected no columns for empty selections, got {columns}"

    def test_malformed_operational_metadata_config(self, metadata_handler):
        """Test behavior with malformed operational metadata configurations."""
        preset_config = {"operational_metadata": "not_a_list"}  # Should be a list

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["malformed_preset"],
            operational_metadata=["_ingestion_timestamp"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {"_ingestion_timestamp": "F.current_timestamp()"}

        assert columns == expected_columns, (
            f"Expected fallback to flowgroup, got {columns}"
        )

    def test_empty_column_list_selections(self, metadata_handler):
        """Test behavior with explicitly empty column lists."""
        preset_config = {"operational_metadata": []}  # Explicitly empty

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["empty_list_preset"],
            operational_metadata=[],  # Explicitly empty
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=[],  # Explicitly empty
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        assert columns == {}, f"Expected no columns for empty lists, got {columns}"

    def test_source_file_only_for_streaming_tables(self, metadata_handler):
        """Test that _source_file column only applies to streaming tables."""
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_source_file",
                "_pipeline_name",
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        action_streaming = Action(
            name="write_streaming_table",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_streaming = metadata_handler.resolve_metadata_selection(
            flowgroup, action_streaming, preset_config
        )
        columns_streaming = metadata_handler.get_selected_columns(
            selection_streaming or {}, "streaming_table"
        )

        expected_streaming = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",  # Should be included
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns_streaming == expected_streaming, (
            f"Streaming table should include _source_file, got {columns_streaming}"
        )

        action_materialized = Action(
            name="write_materialized_view",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "materialized_view",
                "database": "test.silver",
                "table": "test",
            },
        )

        selection_materialized = metadata_handler.resolve_metadata_selection(
            flowgroup, action_materialized, preset_config
        )
        columns_materialized = metadata_handler.get_selected_columns(
            selection_materialized or {}, "materialized_view"
        )

        expected_materialized = {
            "_ingestion_timestamp": "F.current_timestamp()",
            # _source_file should be excluded
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns_materialized == expected_materialized, (
            f"Materialized view should exclude _source_file, got {columns_materialized}"
        )

    def test_custom_function_target_filtering(self, metadata_handler):
        """Test that custom function column with specific target filtering works."""
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_custom_function",
                "_pipeline_name",
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        action_streaming = Action(
            name="write_streaming_table",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_streaming = metadata_handler.resolve_metadata_selection(
            flowgroup, action_streaming, preset_config
        )
        columns_streaming = metadata_handler.get_selected_columns(
            selection_streaming or {}, "streaming_table"
        )

        expected_streaming = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_custom_function": 'custom_business_logic(F.col("revenue"))',  # Should be included
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns_streaming == expected_streaming, (
            f"Streaming table should include custom function, got {columns_streaming}"
        )

        action_materialized = Action(
            name="write_materialized_view",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "materialized_view",
                "database": "test.silver",
                "table": "test",
            },
        )

        selection_materialized = metadata_handler.resolve_metadata_selection(
            flowgroup, action_materialized, preset_config
        )
        columns_materialized = metadata_handler.get_selected_columns(
            selection_materialized or {}, "materialized_view"
        )

        expected_materialized = {
            "_ingestion_timestamp": "F.current_timestamp()",
            # _custom_function should be excluded (only applies to streaming_table)
            "_pipeline_name": 'F.lit("test_pipeline")',
        }

        assert columns_materialized == expected_materialized, (
            f"Materialized view should exclude custom function, got {columns_materialized}"
        )

    def test_mixed_target_type_filtering(self, metadata_handler):
        """Test complex scenario with multiple columns having different target type restrictions."""
        # Select columns with various target type restrictions
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",  # Both streaming_table and materialized_view
                "_source_file",  # Only streaming_table
                "_pipeline_name",  # Both streaming_table and materialized_view
                "_business_quarter",  # Both streaming_table and materialized_view
                "_custom_function",  # Only streaming_table
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        action_streaming = Action(
            name="write_streaming_table",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_streaming = metadata_handler.resolve_metadata_selection(
            flowgroup, action_streaming, preset_config
        )
        columns_streaming = metadata_handler.get_selected_columns(
            selection_streaming or {}, "streaming_table"
        )

        expected_streaming = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            "_custom_function": 'custom_business_logic(F.col("revenue"))',
        }

        assert columns_streaming == expected_streaming, (
            f"Streaming table should get all applicable columns, got {columns_streaming}"
        )

        action_materialized = Action(
            name="write_materialized_view",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "materialized_view",
                "database": "test.silver",
                "table": "test",
            },
        )

        selection_materialized = metadata_handler.resolve_metadata_selection(
            flowgroup, action_materialized, preset_config
        )
        columns_materialized = metadata_handler.get_selected_columns(
            selection_materialized or {}, "materialized_view"
        )

        expected_materialized = {
            "_ingestion_timestamp": "F.current_timestamp()",
            # _source_file excluded (streaming_table only)
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            # _custom_function excluded (streaming_table only)
        }

        assert columns_materialized == expected_materialized, (
            f"Materialized view should exclude streaming-only columns, got {columns_materialized}"
        )

    def test_target_type_filtering_with_action_additions(self, metadata_handler):
        """Test that target type filtering works when action adds columns."""
        # Preset selects universal columns
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_pipeline_name"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_business_quarter"],  # Universal column
            actions=[],
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        action_streaming = Action(
            name="write_streaming_table",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=[
                "_source_file",
                "_custom_function",
            ],  # Both streaming-only
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_streaming = metadata_handler.resolve_metadata_selection(
            flowgroup, action_streaming, preset_config
        )
        columns_streaming = metadata_handler.get_selected_columns(
            selection_streaming or {}, "streaming_table"
        )

        expected_streaming = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            "_source_file": "F.input_file_name()",
            "_custom_function": 'custom_business_logic(F.col("revenue"))',
        }

        assert columns_streaming == expected_streaming, (
            f"Streaming table should get all columns, got {columns_streaming}"
        )

        action_materialized = Action(
            name="write_materialized_view",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=[
                "_source_file",
                "_custom_function",
            ],  # Both will be filtered out
            write_target={
                "type": "materialized_view",
                "database": "test.silver",
                "table": "test",
            },
        )

        selection_materialized = metadata_handler.resolve_metadata_selection(
            flowgroup, action_materialized, preset_config
        )
        columns_materialized = metadata_handler.get_selected_columns(
            selection_materialized or {}, "materialized_view"
        )

        expected_materialized = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            # _source_file and _custom_function excluded (streaming_table only)
        }

        assert columns_materialized == expected_materialized, (
            f"Materialized view should exclude streaming-only columns, got {columns_materialized}"
        )

    def test_simple_function_import_detection(self, metadata_handler):
        """Test AST import detection for simple PySpark functions."""
        from lhp.core.codegen.imports import ImportDetector

        detector = ImportDetector(strategy="ast")

        test_cases = [
            ("F.current_timestamp()", {"from pyspark.sql import functions as F"}),
            ("F.input_file_name()", {"from pyspark.sql import functions as F"}),
            ('F.lit("test")', {"from pyspark.sql import functions as F"}),
            ('F.col("column_name")', {"from pyspark.sql import functions as F"}),
            ('spark.conf.get("key", "default")', set()),  # Built-in, no import needed
        ]

        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, (
                f"Expression '{expression}' should import {expected_imports}, got {detected}"
            )

    def test_complex_expression_import_detection(self, metadata_handler):
        """Test AST import detection for complex PySpark expressions."""
        from lhp.core.codegen.imports import ImportDetector

        detector = ImportDetector(strategy="ast")

        test_cases = [
            # when/otherwise expression
            (
                'F.when(F.col("key").isNotNull(), F.lit("high")).otherwise(F.lit("low"))',
                {"from pyspark.sql import functions as F"},
            ),
            # Concatenation with date functions
            (
                'F.concat(F.lit("Q"), F.quarter(F.current_date()), F.lit("-"), F.year(F.current_date()))',
                {"from pyspark.sql import functions as F"},
            ),
            # Complex conditional with regex
            (
                'F.when(F.col("email").rlike(r"^[^@]+@[^@]+\\.[^@]+$"), F.lit("valid")).otherwise(F.lit("invalid"))',
                {"from pyspark.sql import functions as F"},
            ),
            # Mixed with spark config
            (
                'F.lit(spark.conf.get("pipelines.id", "unknown"))',
                {"from pyspark.sql import functions as F"},
            ),
        ]

        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, (
                f"Complex expression should import {expected_imports}, got {detected}"
            )

    def test_custom_function_import_detection(self, metadata_handler):
        """Test that custom functions don't get automatic imports (require explicit config)."""
        from lhp.core.codegen.imports import ImportDetector

        detector = ImportDetector(strategy="ast")

        test_cases = [
            (
                'custom_business_logic(F.col("revenue"))',
                {"from pyspark.sql import functions as F"},
            ),  # Only F. functions
            (
                'assess_data_quality(F.col("key"), F.col("email"))',
                {"from pyspark.sql import functions as F"},
            ),
            ('my_udf(F.col("data"))', {"from pyspark.sql import functions as F"}),
        ]

        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, (
                f"Custom function should only import PySpark functions, got {detected}"
            )

    def test_metadata_handler_import_collection(self, metadata_handler):
        """Test that metadata handler correctly collects imports from selected columns."""
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_business_quarter",
                "_custom_function",
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )
        imports = metadata_handler.get_required_imports(columns)

        expected_imports = {
            "from pyspark.sql import functions as F",  # From F.current_timestamp(), F.concat(), F.quarter(), etc.
            "from my_project.logic import custom_business_logic",  # From additional_imports config
        }

        assert imports == expected_imports, (
            f"Should collect all required imports, got {imports}"
        )

    def test_no_imports_needed_for_empty_selection(self, metadata_handler):
        """Test that no imports are generated when no columns are selected."""
        preset_config = {}

        flowgroup = FlowGroup(
            pipeline="test_pipeline", flowgroup="test_flowgroup", presets=[], actions=[]
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=False,  # Disabled
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )
        imports = metadata_handler.get_required_imports(columns)

        assert imports == set(), (
            f"No imports should be needed for empty selection, got {imports}"
        )

    def test_udf_import_detection(self, metadata_handler):
        """Test that UDF-related functions get correct imports."""
        from lhp.core.codegen.imports import ImportDetector

        detector = ImportDetector(strategy="ast")

        test_cases = [
            (
                "udf(lambda x: x.upper(), StringType())",
                {
                    "from pyspark.sql.functions import udf",
                    "from pyspark.sql.types import StringType",
                },
            ),
            (
                "pandas_udf(my_function, StringType())",
                {
                    "from pyspark.sql.functions import pandas_udf",
                    "from pyspark.sql.types import StringType",
                },
            ),
            (
                'broadcast(F.col("lookup_table"))',
                {
                    "from pyspark.sql.functions import broadcast",
                    "from pyspark.sql import functions as F",
                },
            ),
        ]

        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert detected == expected_imports, (
                f"UDF expression should import {expected_imports}, got {detected}"
            )

    def test_fallback_regex_detection(self, metadata_handler):
        """Test that malformed expressions fall back to regex detection."""
        from lhp.core.codegen.imports import ImportDetector

        detector = ImportDetector(strategy="ast")

        test_cases = [
            (
                "F.current_timestamp() +",
                {"from pyspark.sql import functions as F"},
            ),  # Incomplete expression
            (
                "F.when(",
                {"from pyspark.sql import functions as F"},
            ),  # Incomplete parentheses
            ('# F.lit("comment")', set()),  # Comment - should not detect
        ]

        for expression, expected_imports in test_cases:
            detected = detector.detect_imports(expression)
            assert (
                "from pyspark.sql import functions as F" in detected
                or len(expected_imports) == 0
            ), f"Fallback should detect F. functions in '{expression}', got {detected}"

    def test_import_deduplication(self, metadata_handler):
        """Test that duplicate imports are correctly deduplicated."""
        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",  # F.current_timestamp()
                "_pipeline_name",  # F.lit()
                "_business_quarter",  # F.concat(), F.quarter(), etc.
            ]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_data_classification"],  # Another F.lit()
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )
        imports = metadata_handler.get_required_imports(columns)

        expected_imports = {"from pyspark.sql import functions as F"}

        assert imports == expected_imports, f"Should deduplicate imports, got {imports}"

        # Verify we don't have duplicate entries
        import_list = list(imports)
        assert len(import_list) == len(set(import_list)), (
            "Should not have duplicate imports"
        )

    def test_all_columns_selection_compatibility(self, metadata_handler):
        """Test that selecting all available columns works (replaces boolean true behavior)."""
        # List all available columns explicitly instead of using boolean True
        all_columns = [
            "_ingestion_timestamp",
            "_source_file",
            "_pipeline_name",
            "_business_quarter",
            "_data_classification",
            "_custom_function",
        ]

        preset_config = {"operational_metadata": all_columns}

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=all_columns,
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=all_columns,
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            "_data_classification": 'F.lit("PII")',
            "_custom_function": 'custom_business_logic(F.col("revenue"))',
        }

        assert columns == expected_columns, (
            f"All columns selection should use all project columns, got {columns}"
        )

    def test_boolean_false_backward_compatibility(self, metadata_handler):
        """Test that operational_metadata: false still works (disables metadata)."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_pipeline_name"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=False,  # Old boolean style disable
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        assert columns == {}, (
            f"Boolean false should disable all metadata, got {columns}"
        )

    def test_additive_list_configurations(self, metadata_handler):
        """Test additive behavior with list configurations across all levels."""
        preset_config = {
            "operational_metadata": ["_ingestion_timestamp", "_source_file"]
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_business_quarter", "_pipeline_name"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=["_data_classification", "_custom_function"],
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler.get_selected_columns(
            selection or {}, "streaming_table"
        )

        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            "_data_classification": 'F.lit("PII")',
            "_custom_function": 'custom_business_logic(F.col("revenue"))',
        }

        assert columns == expected_columns, (
            f"Should combine all list configurations additively, got {columns}"
        )

    def test_no_project_config_backward_compatibility(self, metadata_handler):
        """Test behavior when no project config exists (fallback to hardcoded defaults)."""
        metadata_handler_no_config = OperationalMetadataCatalog(project_config=None)

        preset_config = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_pipeline_run_id",
                "_pipeline_name",
                "_flowgroup_name",
            ]  # Should use hardcoded defaults
        }

        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        action = Action(
            name="write_test",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        metadata_handler_no_config.update_context("test_pipeline", "test_flowgroup")

        selection = metadata_handler_no_config.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        columns = metadata_handler_no_config.get_selected_columns(
            selection or {}, "streaming_table"
        )

        # Note: _source_file only applies to 'view' target type, not 'streaming_table'
        expected_columns = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_run_id": 'F.lit(spark.conf.get("pipelines.id", "unknown"))',
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_flowgroup_name": 'F.lit("test_flowgroup")',
        }

        assert columns == expected_columns, (
            f"Should use hardcoded defaults when no project config, got {columns}"
        )

    def test_various_list_configurations(self, metadata_handler):
        """Test various list-based operational metadata configurations."""
        metadata_handler.update_context("test_pipeline", "test_flowgroup")

        preset_config_simple = {
            "operational_metadata": ["_ingestion_timestamp", "_pipeline_name"]
        }

        flowgroup_simple = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            actions=[],
        )

        action_simple = Action(
            name="write_test_simple",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_simple = metadata_handler.resolve_metadata_selection(
            flowgroup_simple, action_simple, preset_config_simple
        )
        columns_simple = metadata_handler.get_selected_columns(
            selection_simple or {}, "streaming_table"
        )

        preset_config_full = {
            "operational_metadata": [
                "_ingestion_timestamp",
                "_source_file",
                "_pipeline_name",
            ]
        }

        flowgroup_full = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_layer"],
            operational_metadata=["_business_quarter"],
            actions=[],
        )

        action_full = Action(
            name="write_test_full",
            type=ActionType.WRITE,
            source="v_test",
            operational_metadata=["_data_classification"],
            write_target={
                "type": "streaming_table",
                "database": "test.bronze",
                "table": "test",
            },
        )

        selection_full = metadata_handler.resolve_metadata_selection(
            flowgroup_full, action_full, preset_config_full
        )
        columns_full = metadata_handler.get_selected_columns(
            selection_full or {}, "streaming_table"
        )

        assert len(columns_simple) > 0, "Simple preset configuration should work"
        assert len(columns_full) > 0, "Full additive configuration should work"

        expected_simple = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_pipeline_name": 'F.lit("test_pipeline")',
        }
        assert columns_simple == expected_simple, (
            f"Simple config should match expected, got {columns_simple}"
        )

        expected_full = {
            "_ingestion_timestamp": "F.current_timestamp()",
            "_source_file": "F.input_file_name()",
            "_pipeline_name": 'F.lit("test_pipeline")',
            "_business_quarter": 'F.concat(F.lit("Q"), F.quarter(F.current_date()))',
            "_data_classification": 'F.lit("PII")',
        }
        assert columns_full == expected_full, (
            f"Full config should combine all levels, got {columns_full}"
        )

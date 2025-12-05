"""Tests for CodeGenerator service."""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock
from collections import defaultdict

from lhp.core.services.code_generator import CodeGenerator
from lhp.models.config import Action, ActionType, FlowGroup


@pytest.fixture
def mock_action_registry():
    """Create mock action registry."""
    registry = Mock()
    return registry


@pytest.fixture
def mock_dependency_resolver():
    """Create mock dependency resolver."""
    resolver = Mock()
    resolver.resolve_dependencies.return_value = []
    return resolver


@pytest.fixture
def mock_preset_manager():
    """Create mock preset manager."""
    manager = Mock()
    manager.resolve_preset_chain.return_value = {}
    return manager


@pytest.fixture
def code_generator(mock_action_registry, mock_dependency_resolver, mock_preset_manager):
    """Create CodeGenerator instance."""
    return CodeGenerator(
        action_registry=mock_action_registry,
        dependency_resolver=mock_dependency_resolver,
        preset_manager=mock_preset_manager,
        project_config=None,
        project_root=Path("/test")
    )


class TestCodeGeneratorDetermineActionSubtype:
    """Test determine_action_subtype method."""
    
    def test_determine_action_subtype_load_dict_source(self, code_generator):
        """Test determining subtype for load action with dict source."""
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            source={"type": "kafka", "bootstrap_servers": "localhost:9092"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "kafka"
    
    def test_determine_action_subtype_load_string_source(self, code_generator):
        """Test determining subtype for load action with string source."""
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            source="SELECT * FROM table"
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "sql"
    
    def test_determine_action_subtype_load_dict_no_type(self, code_generator):
        """Test determining subtype for load action with dict source without type."""
        action = Action(
            name="load_data",
            type=ActionType.LOAD,
            source={"sql": "SELECT * FROM table"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "sql"
    
    def test_determine_action_subtype_transform_with_type(self, code_generator):
        """Test determining subtype for transform action with transform_type."""
        action = Action(
            name="transform_data",
            type=ActionType.TRANSFORM,
            transform_type="sql"
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "sql"
    
    def test_determine_action_subtype_transform_no_type(self, code_generator):
        """Test determining subtype for transform action without transform_type."""
        action = Action(
            name="transform_data",
            type=ActionType.TRANSFORM
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "sql"
    
    def test_determine_action_subtype_write_streaming_table(self, code_generator):
        """Test determining subtype for write action with streaming_table."""
        action = Action(
            name="write_data",
            type=ActionType.WRITE,
            write_target={"type": "streaming_table", "database": "test", "table": "table1"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "streaming_table"
    
    def test_determine_action_subtype_write_materialized_view(self, code_generator):
        """Test determining subtype for write action with materialized_view."""
        action = Action(
            name="write_data",
            type=ActionType.WRITE,
            write_target={"type": "materialized_view", "database": "test", "table": "view1"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "materialized_view"
    
    def test_determine_action_subtype_write_sink(self, code_generator):
        """Test determining subtype for write action with sink."""
        action = Action(
            name="write_data",
            type=ActionType.WRITE,
            write_target={"type": "sink", "sink_type": "delta", "sink_name": "sink1"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "sink"
    
    def test_determine_action_subtype_write_no_type(self, code_generator):
        """Test determining subtype for write action without type."""
        action = Action(
            name="write_data",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table1"}
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "streaming_table"
    
    def test_determine_action_subtype_write_no_target(self, code_generator):
        """Test determining subtype for write action without write_target."""
        action = Action(
            name="write_data",
            type=ActionType.WRITE
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "streaming_table"
    
    def test_determine_action_subtype_test_with_type(self, code_generator):
        """Test determining subtype for test action with test_type."""
        action = Action(
            name="test_data",
            type=ActionType.TEST,
            test_type="row_count"
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "row_count"
    
    def test_determine_action_subtype_test_no_type(self, code_generator):
        """Test determining subtype for test action without test_type."""
        action = Action(
            name="test_data",
            type=ActionType.TEST
        )
        
        result = code_generator.determine_action_subtype(action)
        assert result == "row_count"
    
    def test_determine_action_subtype_unknown_type(self, code_generator):
        """Test determining subtype for unknown action type."""
        action = Mock()
        action.type = "unknown_type"
        
        with pytest.raises(ValueError) as exc_info:
            code_generator.determine_action_subtype(action)
        
        assert "Unknown action type" in str(exc_info.value)


class TestCodeGeneratorGroupWriteActionsByTarget:
    """Test group_write_actions_by_target method."""
    
    def test_group_write_actions_single_action(self, code_generator):
        """Test grouping single write action."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table1"}
        )
        
        result = code_generator.group_write_actions_by_target([action])
        
        assert len(result) == 1
        assert "test.table1" in result
        assert result["test.table1"] == [action]
    
    def test_group_write_actions_multiple_same_target(self, code_generator):
        """Test grouping multiple actions with same target."""
        action1 = Action(
            name="write_table1_a",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table1"}
        )
        action2 = Action(
            name="write_table1_b",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table1"}
        )
        
        result = code_generator.group_write_actions_by_target([action1, action2])
        
        assert len(result) == 1
        assert "test.table1" in result
        assert len(result["test.table1"]) == 2
        assert action1 in result["test.table1"]
        assert action2 in result["test.table1"]
    
    def test_group_write_actions_different_targets(self, code_generator):
        """Test grouping actions with different targets."""
        action1 = Action(
            name="write_table1",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table1"}
        )
        action2 = Action(
            name="write_table2",
            type=ActionType.WRITE,
            write_target={"database": "test", "table": "table2"}
        )
        
        result = code_generator.group_write_actions_by_target([action1, action2])
        
        assert len(result) == 2
        assert "test.table1" in result
        assert "test.table2" in result
    
    def test_group_write_actions_table_only(self, code_generator):
        """Test grouping actions with table name only."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            write_target={"table": "table1"}
        )
        
        result = code_generator.group_write_actions_by_target([action])
        
        assert len(result) == 1
        assert "table1" in result
    
    def test_group_write_actions_name_fallback(self, code_generator):
        """Test grouping actions using action name as fallback."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            write_target={}
        )
        
        result = code_generator.group_write_actions_by_target([action])
        
        assert len(result) == 1
        assert "write_table1" in result
    
    def test_group_write_actions_empty_list(self, code_generator):
        """Test grouping empty list of actions."""
        result = code_generator.group_write_actions_by_target([])
        
        assert result == {}


class TestCodeGeneratorCreateCombinedWriteAction:
    """Test create_combined_write_action method."""
    
    def test_create_combined_write_action_single_action(self, code_generator):
        """Test creating combined action from single action."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            source="v_source1",
            write_target={"database": "test", "table": "table1", "create_table": True}
        )
        
        combined = code_generator.create_combined_write_action([action], "test.table1")
        
        assert combined.name == action.name
        assert combined.write_target == action.write_target
        assert hasattr(combined, '_action_metadata')
        assert len(combined._action_metadata) == 1
    
    def test_create_combined_write_action_multiple_actions(self, code_generator):
        """Test creating combined action from multiple actions."""
        action1 = Action(
            name="write_table1_a",
            type=ActionType.WRITE,
            source="v_source1",
            write_target={"database": "test", "table": "table1", "create_table": True}
        )
        action2 = Action(
            name="write_table1_b",
            type=ActionType.WRITE,
            source="v_source2",
            write_target={"database": "test", "table": "table1", "create_table": False}
        )
        
        combined = code_generator.create_combined_write_action([action1, action2], "test.table1")
        
        assert combined.name == action1.name  # Uses table creator
        assert hasattr(combined, '_action_metadata')
        assert len(combined._action_metadata) == 2
    
    def test_create_combined_write_action_multiple_sources(self, code_generator):
        """Test creating combined action with multiple sources."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            source=["v_source1", "v_source2"],
            write_target={"database": "test", "table": "table1", "create_table": True}
        )
        
        combined = code_generator.create_combined_write_action([action], "test.table1")
        
        assert hasattr(combined, '_action_metadata')
        assert len(combined._action_metadata) == 2
        assert combined._action_metadata[0]["source_view"] == "v_source1"
        assert combined._action_metadata[1]["source_view"] == "v_source2"
    
    def test_create_combined_write_action_preserves_metadata(self, code_generator):
        """Test that combined action preserves action metadata."""
        action = Action(
            name="write_table1",
            type=ActionType.WRITE,
            source="v_source1",
            description="Test description",
            once=True,
            write_target={"database": "test", "table": "table1", "create_table": True}
        )
        
        combined = code_generator.create_combined_write_action([action], "test.table1")
        
        assert combined._action_metadata[0]["description"] == "Test description"
        assert combined._action_metadata[0]["once"] is True


class TestCodeGeneratorBuildCustomSourceBlock:
    """Test build_custom_source_block method."""
    
    def test_build_custom_source_block_single_section(self, code_generator):
        """Test building custom source block with single section."""
        custom_sections = [{
            "content": "class CustomSource:\n    pass",
            "source_file": Path("/test/source.py"),
            "action_name": "test_action"
        }]
        
        result = code_generator.build_custom_source_block(custom_sections)
        
        assert "CUSTOM DATA SOURCE IMPLEMENTATIONS" in result
        assert "class CustomSource:" in result
        assert "/test/source.py" in result
        assert "test_action" in result
    
    def test_build_custom_source_block_multiple_sections(self, code_generator):
        """Test building custom source block with multiple sections."""
        custom_sections = [
            {
                "content": "class Source1:\n    pass",
                "source_file": Path("/test/source1.py"),
                "action_name": "action1"
            },
            {
                "content": "class Source2:\n    pass",
                "source_file": Path("/test/source2.py"),
                "action_name": "action2"
            }
        ]
        
        result = code_generator.build_custom_source_block(custom_sections)
        
        assert "class Source1:" in result
        assert "class Source2:" in result
        assert "action1" in result
        assert "action2" in result
    
    def test_build_custom_source_block_empty(self, code_generator):
        """Test building custom source block with empty list."""
        result = code_generator.build_custom_source_block([])
        
        assert "CUSTOM DATA SOURCE IMPLEMENTATIONS" in result


class TestCodeGeneratorExtractSourceViewsFromAction:
    """Test _extract_source_views_from_action method."""
    
    def test_extract_source_views_string(self, code_generator):
        """Test extracting source views from string."""
        result = code_generator._extract_source_views_from_action("v_source")
        
        assert result == ["v_source"]
    
    def test_extract_source_views_list_strings(self, code_generator):
        """Test extracting source views from list of strings."""
        result = code_generator._extract_source_views_from_action(["v_source1", "v_source2"])
        
        assert result == ["v_source1", "v_source2"]
    
    def test_extract_source_views_list_dicts(self, code_generator):
        """Test extracting source views from list of dicts."""
        result = code_generator._extract_source_views_from_action([
            {"database": "test", "table": "table1"},
            {"database": "test", "table": "table2"}
        ])
        
        assert result == ["test.table1", "test.table2"]
    
    def test_extract_source_views_list_dicts_with_view(self, code_generator):
        """Test extracting source views from list of dicts with view key."""
        result = code_generator._extract_source_views_from_action([
            {"view": "v_view1"},
            {"view": "v_view2"}
        ])
        
        assert result == ["v_view1", "v_view2"]
    
    def test_extract_source_views_dict_with_database_table(self, code_generator):
        """Test extracting source views from dict with database and table."""
        result = code_generator._extract_source_views_from_action({
            "database": "test",
            "table": "table1"
        })
        
        assert result == ["test.table1"]
    
    def test_extract_source_views_dict_with_table_only(self, code_generator):
        """Test extracting source views from dict with table only."""
        result = code_generator._extract_source_views_from_action({
            "table": "table1"
        })
        
        assert result == ["table1"]
    
    def test_extract_source_views_dict_with_view(self, code_generator):
        """Test extracting source views from dict with view key."""
        result = code_generator._extract_source_views_from_action({
            "view": "v_view1"
        })
        
        assert result == ["v_view1"]
    
    def test_extract_source_views_dict_empty(self, code_generator):
        """Test extracting source views from empty dict."""
        result = code_generator._extract_source_views_from_action({})
        
        assert result == ["source"]  # Generic fallback
    
    def test_extract_source_views_other_type(self, code_generator):
        """Test extracting source views from other type - now returns fallback (Phase 1 refactoring)."""
        result = code_generator._extract_source_views_from_action(123)
        
        assert result == ["source"]  # Fallback for non-standard types


class TestCodeGeneratorGenerateActionSections:
    """Test _generate_action_sections method with test actions."""
    
    def test_generate_action_sections_with_tests_included(self, code_generator):
        """Test generating action sections with test actions when include_tests=True."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="test1", type=ActionType.TEST, test_type="row_count")
            ]
        )
        
        mock_generator = Mock()
        mock_generator.generate.return_value = "# Test code"
        mock_generator.imports = set()
        # Mock get_import_manager to return None (legacy generator)
        mock_generator.get_import_manager = Mock(return_value=None)
        code_generator.action_registry.get_generator.return_value = mock_generator
        
        code_generator.dependency_resolver.resolve_dependencies.return_value = flowgroup.actions
        
        sections, imports, custom = code_generator._generate_action_sections(
            flowgroup, flowgroup.actions, Mock(), {}, None, None, None, None, include_tests=True
        )
        
        assert any("DATA QUALITY TESTS" in section for section in sections)
        code_generator.action_registry.get_generator.assert_called()
    
    def test_generate_action_sections_with_tests_excluded(self, code_generator):
        """Test generating action sections without test actions when include_tests=False."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[
                Action(name="test1", type=ActionType.TEST, test_type="row_count")
            ]
        )
        
        code_generator.dependency_resolver.resolve_dependencies.return_value = flowgroup.actions
        
        sections, imports, custom = code_generator._generate_action_sections(
            flowgroup, flowgroup.actions, Mock(), {}, None, None, None, None, include_tests=False
        )
        
        assert not any("DATA QUALITY TESTS" in section for section in sections)


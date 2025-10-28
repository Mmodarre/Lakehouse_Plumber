"""Tests for generation strategy pattern implementations."""

import pytest
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch
from datetime import datetime

from lhp.core.strategies import (
    GenerationStrategy,
    GenerationContext,
    GenerationFilterResult,
    BaseGenerationStrategy,
    SmartGenerationStrategy,
    ForceGenerationStrategy,
    SelectiveGenerationStrategy,
    FallbackGenerationStrategy,
    GenerationStrategyFactory,
)
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.core.state_manager import StateManager
from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestGenerationContext:
    """Test GenerationContext data class."""
    
    def test_context_creation(self, tmp_path):
        """Test creating a generation context."""
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=True,
            specific_flowgroups=["fg1", "fg2"],
            project_root=tmp_path
        )
        
        assert context.env == "dev"
        assert context.pipeline_identifier == "test_pipeline"
        assert context.include_tests is True
        assert context.specific_flowgroups == ["fg1", "fg2"]
        assert context.project_root == tmp_path
    
    def test_context_defaults(self, tmp_path):
        """Test context with default values."""
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False
        )
        
        assert context.specific_flowgroups == []
        assert context.state_manager is None
        assert context.project_root is None


class TestGenerationFilterResult:
    """Test GenerationFilterResult data class."""
    
    def test_result_creation(self):
        """Test creating filter result."""
        fg1 = Mock(spec=FlowGroup)
        fg2 = Mock(spec=FlowGroup)
        
        result = GenerationFilterResult(
            flowgroups_to_generate=[fg1],
            flowgroups_to_skip=[fg2],
            strategy_name="test_strategy",
            metadata={"key": "value"}
        )
        
        assert len(result.flowgroups_to_generate) == 1
        assert len(result.flowgroups_to_skip) == 1
        assert result.strategy_name == "test_strategy"
        assert result.metadata == {"key": "value"}
    
    def test_has_work_to_do(self):
        """Test has_work_to_do method."""
        fg1 = Mock(spec=FlowGroup)
        
        result_with_work = GenerationFilterResult(
            flowgroups_to_generate=[fg1],
            flowgroups_to_skip=[],
            strategy_name="test"
        )
        assert result_with_work.has_work_to_do() is True
        
        result_no_work = GenerationFilterResult(
            flowgroups_to_generate=[],
            flowgroups_to_skip=[fg1],
            strategy_name="test"
        )
        assert result_no_work.has_work_to_do() is False
    
    def test_get_performance_info(self):
        """Test performance info generation."""
        fg1 = Mock(spec=FlowGroup)
        fg2 = Mock(spec=FlowGroup)
        
        result = GenerationFilterResult(
            flowgroups_to_generate=[fg1],
            flowgroups_to_skip=[fg2],
            strategy_name="smart",
            metadata={"new_count": 1, "stale_count": 2}
        )
        
        perf_info = result.get_performance_info()
        assert perf_info["strategy"] == "smart"
        assert perf_info["total_flowgroups"] == 2
        assert perf_info["selected_flowgroups"] == 1
        assert perf_info["skipped_flowgroups"] == 1
        assert perf_info["new_count"] == 1
        assert perf_info["stale_count"] == 2


class TestBaseGenerationStrategy:
    """Test BaseGenerationStrategy with generation context awareness."""
    
    def test_check_generation_context_staleness_no_state_manager(self, tmp_path):
        """Test staleness check with no state manager."""
        strategy = BaseGenerationStrategy("test")
        
        fg = Mock(spec=FlowGroup)
        fg.flowgroup = "test_fg"
        fg.pipeline = "test_pipeline"
        fg.actions = []
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=True,
            project_root=tmp_path
        )
        
        # With no state manager, should return empty set
        result = strategy._check_generation_context_staleness([fg], context)
        assert result == set()
    
    def test_check_generation_context_staleness_with_test_actions(self, tmp_path):
        """Test staleness detection for flowgroups with test actions."""
        strategy = BaseGenerationStrategy("test")
        
        # Create flowgroup with test action
        test_action = Mock(spec=Action)
        test_action.type = ActionType.TEST
        
        fg = Mock(spec=FlowGroup)
        fg.flowgroup = "test_fg"
        fg.pipeline = "test_pipeline"
        fg.actions = [test_action]
        
        # Mock state manager
        state_manager = Mock(spec=StateManager)
        state_manager.get_generated_files.return_value = {
            "generated/dev/test_pipeline/test_fg.py": Mock(
                generated_path="generated/dev/test_pipeline/test_fg.py",
                file_composite_checksum="old_checksum"
            )
        }
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=True,
            state_manager=state_manager,
            project_root=tmp_path
        )
        
        # Mock _would_composite_checksum_change to return True
        with patch.object(strategy, '_would_composite_checksum_change', return_value=True):
            result = strategy._check_generation_context_staleness([fg], context)
            assert "test_fg" in result
    
    def test_would_composite_checksum_change_missing_file(self, tmp_path):
        """Test checksum change detection with missing source file."""
        strategy = BaseGenerationStrategy("test")
        
        file_state = Mock()
        file_state.source_yaml = "pipelines/missing.yaml"
        file_state.pipeline = "test_pipeline"
        file_state.flowgroup = "test_fg"
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        # With missing file, should return False
        result = strategy._would_composite_checksum_change(file_state, "context", context)
        assert result is False
    
    def test_would_composite_checksum_change_with_error(self, tmp_path):
        """Test checksum change detection with exception."""
        strategy = BaseGenerationStrategy("test")
        
        # Create a real source file
        source_file = tmp_path / "pipelines" / "test.yaml"
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text("test: data")
        
        file_state = Mock()
        file_state.source_yaml = "pipelines/test.yaml"
        file_state.pipeline = "test_pipeline"
        file_state.flowgroup = "test_fg"
        file_state.file_composite_checksum = "old_checksum"
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        # Mock StateDependencyResolver's resolve_file_dependencies to raise exception
        with patch.object(StateDependencyResolver, 'resolve_file_dependencies', side_effect=Exception("Test error")):
            # With error, should return True (assume changed to be safe)
            result = strategy._would_composite_checksum_change(file_state, "context", context)
            assert result is True


class TestSmartGenerationStrategy:
    """Test SmartGenerationStrategy for intelligent state-based generation."""
    
    def test_filter_with_no_state_manager(self, tmp_path):
        """Test fallback behavior when no state manager available."""
        strategy = SmartGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg2 = Mock(spec=FlowGroup)
        all_flowgroups = [fg1, fg2]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Should generate all when no state manager
        assert len(result.flowgroups_to_generate) == 2
        assert len(result.flowgroups_to_skip) == 0
        assert result.metadata["fallback_reason"] == "no_state_manager"
    
    def test_filter_with_new_files(self, tmp_path):
        """Test filtering with new YAML files."""
        strategy = SmartGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg1.flowgroup = "new_fg"
        fg1.actions = []
        
        fg2 = Mock(spec=FlowGroup)
        fg2.flowgroup = "existing_fg"
        fg2.actions = []
        
        all_flowgroups = [fg1, fg2]
        
        # Create actual YAML file since YAMLParser is now imported at module level
        new_yaml_file = tmp_path / "pipelines" / "new.yaml"
        new_yaml_file.parent.mkdir(parents=True, exist_ok=True)
        new_yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: new_fg
actions:
  - name: test_action
    type: load
    target: test_table
""")
        
        # Mock state manager
        state_manager = Mock(spec=StateManager)
        state_manager.get_files_needing_generation.return_value = {
            "new": [new_yaml_file],
            "stale": [],
            "up_to_date": []
        }
        state_manager.get_generated_files.return_value = {}
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            state_manager=state_manager,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Should only generate the new flowgroup
        assert len(result.flowgroups_to_generate) == 1
        assert result.flowgroups_to_generate[0].flowgroup == "new_fg"
        assert result.metadata["new_count"] == 1
    
    def test_filter_with_stale_files(self, tmp_path):
        """Test filtering with stale files."""
        strategy = SmartGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg1.flowgroup = "stale_fg"
        fg1.actions = []
        
        all_flowgroups = [fg1]
        
        # Create mock file state for stale file
        stale_file_state = Mock()
        stale_file_state.flowgroup = "stale_fg"
        
        # Mock state manager
        state_manager = Mock(spec=StateManager)
        state_manager.get_files_needing_generation.return_value = {
            "new": [],
            "stale": [stale_file_state],
            "up_to_date": []
        }
        state_manager.get_generated_files.return_value = {}
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            state_manager=state_manager,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Should generate the stale flowgroup
        assert len(result.flowgroups_to_generate) == 1
        assert result.metadata["stale_count"] == 1
    
    def test_filter_with_parse_error(self, tmp_path):
        """Test filtering handles parse errors gracefully."""
        strategy = SmartGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg1.flowgroup = "fg1"
        fg1.actions = []
        
        all_flowgroups = [fg1]
        
        # Mock state manager
        state_manager = Mock(spec=StateManager)
        state_manager.get_files_needing_generation.return_value = {
            "new": [Path("pipelines/bad.yaml")],
            "stale": [],
            "up_to_date": []
        }
        state_manager.get_generated_files.return_value = {}
        
        # Mock YAMLParser to raise exception - patch at source module
        with patch('lhp.parsers.yaml_parser.YAMLParser') as mock_parser:
            mock_parser_instance = mock_parser.return_value
            mock_parser_instance.parse_flowgroups_from_file.side_effect = Exception("Parse error")
            
            context = GenerationContext(
                env="dev",
                pipeline_identifier="test_pipeline",
                include_tests=False,
                state_manager=state_manager,
                project_root=tmp_path
            )
            
            result = strategy.filter_flowgroups(all_flowgroups, context)
            
            # Should handle error gracefully
            assert isinstance(result, GenerationFilterResult)


class TestForceGenerationStrategy:
    """Test ForceGenerationStrategy for forcing regeneration."""
    
    def test_filter_force_all(self, tmp_path):
        """Test that force strategy generates all flowgroups."""
        strategy = ForceGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg2 = Mock(spec=FlowGroup)
        fg3 = Mock(spec=FlowGroup)
        all_flowgroups = [fg1, fg2, fg3]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        assert len(result.flowgroups_to_generate) == 3
        assert len(result.flowgroups_to_skip) == 0
        assert result.strategy_name == "force"
        assert result.metadata["total_flowgroups"] == 3
        assert result.metadata["force_reason"] == "user_requested"


class TestSelectiveGenerationStrategy:
    """Test SelectiveGenerationStrategy for generating specific flowgroups."""
    
    def test_filter_with_specific_flowgroups(self, tmp_path):
        """Test filtering to specific flowgroups."""
        strategy = SelectiveGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg1.flowgroup = "fg1"
        
        fg2 = Mock(spec=FlowGroup)
        fg2.flowgroup = "fg2"
        
        fg3 = Mock(spec=FlowGroup)
        fg3.flowgroup = "fg3"
        
        all_flowgroups = [fg1, fg2, fg3]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            specific_flowgroups=["fg1", "fg3"],
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        assert len(result.flowgroups_to_generate) == 2
        assert len(result.flowgroups_to_skip) == 1
        assert result.metadata["requested_flowgroups"] == 2
        assert result.metadata["found_flowgroups"] == 2
    
    def test_filter_with_no_specific_flowgroups(self, tmp_path):
        """Test filtering with no specific flowgroups specified."""
        strategy = SelectiveGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        all_flowgroups = [fg1]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Should generate nothing when no specific flowgroups
        assert len(result.flowgroups_to_generate) == 0
        assert len(result.flowgroups_to_skip) == 1
        assert result.metadata["error"] == "no_specific_flowgroups_specified"
    
    def test_filter_with_non_matching_flowgroups(self, tmp_path):
        """Test filtering with flowgroups that don't exist."""
        strategy = SelectiveGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg1.flowgroup = "fg1"
        
        all_flowgroups = [fg1]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            specific_flowgroups=["fg2", "fg3"],  # Non-existent
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Should find no matches
        assert len(result.flowgroups_to_generate) == 0
        assert len(result.flowgroups_to_skip) == 1
        assert result.metadata["requested_flowgroups"] == 2
        assert result.metadata["found_flowgroups"] == 0


class TestFallbackGenerationStrategy:
    """Test FallbackGenerationStrategy for when no state management available."""
    
    def test_filter_fallback_generates_all(self, tmp_path):
        """Test that fallback strategy generates all flowgroups."""
        strategy = FallbackGenerationStrategy()
        
        fg1 = Mock(spec=FlowGroup)
        fg2 = Mock(spec=FlowGroup)
        all_flowgroups = [fg1, fg2]
        
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            project_root=tmp_path
        )
        
        result = strategy.filter_flowgroups(all_flowgroups, context)
        
        assert len(result.flowgroups_to_generate) == 2
        assert len(result.flowgroups_to_skip) == 0
        assert result.strategy_name == "fallback"
        assert result.metadata["total_flowgroups"] == 2
        assert result.metadata["fallback_reason"] == "no_state_management"


class TestGenerationStrategyFactory:
    """Test GenerationStrategyFactory for creating appropriate strategies."""
    
    def test_create_force_strategy(self):
        """Test creating force strategy."""
        strategy = GenerationStrategyFactory.create_strategy(
            force=True,
            specific_flowgroups=None,
            has_state_manager=True
        )
        
        assert isinstance(strategy, ForceGenerationStrategy)
    
    def test_create_selective_strategy(self):
        """Test creating selective strategy."""
        strategy = GenerationStrategyFactory.create_strategy(
            force=False,
            specific_flowgroups=["fg1", "fg2"],
            has_state_manager=True
        )
        
        assert isinstance(strategy, SelectiveGenerationStrategy)
    
    def test_create_smart_strategy(self):
        """Test creating smart strategy."""
        strategy = GenerationStrategyFactory.create_strategy(
            force=False,
            specific_flowgroups=None,
            has_state_manager=True
        )
        
        assert isinstance(strategy, SmartGenerationStrategy)
    
    def test_create_fallback_strategy(self):
        """Test creating fallback strategy."""
        strategy = GenerationStrategyFactory.create_strategy(
            force=False,
            specific_flowgroups=None,
            has_state_manager=False
        )
        
        assert isinstance(strategy, FallbackGenerationStrategy)
    
    def test_get_available_strategies(self):
        """Test getting all available strategies."""
        strategies = GenerationStrategyFactory.get_available_strategies()
        
        assert "smart" in strategies
        assert "force" in strategies
        assert "selective" in strategies
        assert "fallback" in strategies
        
        assert isinstance(strategies["smart"], SmartGenerationStrategy)
        assert isinstance(strategies["force"], ForceGenerationStrategy)
        assert isinstance(strategies["selective"], SelectiveGenerationStrategy)
        assert isinstance(strategies["fallback"], FallbackGenerationStrategy)


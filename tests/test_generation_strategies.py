"""Tests for generation strategy pattern implementations."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock

from lhp.core.strategies import (
    SmartGenerationStrategy, ForceGenerationStrategy, 
    SelectiveGenerationStrategy, FallbackGenerationStrategy,
    GenerationContext, GenerationFilterResult, GenerationStrategyFactory
)
from lhp.core.state_manager import StateManager
from lhp.models.config import FlowGroup, Action, ActionType


class TestGenerationStrategies:
    """Test all generation strategy implementations."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Create test flowgroups
        self.test_flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_fg",
            actions=[Action(name="load", type=ActionType.LOAD, source="table1", target="v_table1")]
        )
        
        self.test_only_flowgroup = FlowGroup(
            pipeline="test_pipeline", 
            flowgroup="test_only_fg",
            actions=[Action(name="test", type=ActionType.TEST, test_type="uniqueness")]
        )
        
        self.mixed_flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="mixed_fg", 
            actions=[
                Action(name="load", type=ActionType.LOAD, source="table1", target="v_table1"),
                Action(name="test", type=ActionType.TEST, test_type="uniqueness")
            ]
        )
    
    def test_force_generation_strategy(self):
        """Test ForceGenerationStrategy."""
        strategy = ForceGenerationStrategy()
        flowgroups = [self.test_flowgroup, self.test_only_flowgroup]
        
        context = GenerationContext("dev", "test_pipeline", True)
        
        result = strategy.filter_flowgroups(flowgroups, context)
        
        # Force strategy should generate all flowgroups
        assert len(result.flowgroups_to_generate) == 2
        assert len(result.flowgroups_to_skip) == 0
        assert result.strategy_name == "force"
        assert result.has_work_to_do() == True
        assert result.metadata["force_reason"] == "user_requested"
    
    def test_selective_generation_strategy(self):
        """Test SelectiveGenerationStrategy."""
        strategy = SelectiveGenerationStrategy()
        flowgroups = [self.test_flowgroup, self.test_only_flowgroup, self.mixed_flowgroup]
        
        # Test with specific flowgroups
        context = GenerationContext(
            "dev", "test_pipeline", True, 
            specific_flowgroups=["test_fg", "mixed_fg"]
        )
        
        result = strategy.filter_flowgroups(flowgroups, context)
        
        # Should select only specified flowgroups
        assert len(result.flowgroups_to_generate) == 2  # test_fg, mixed_fg
        assert len(result.flowgroups_to_skip) == 1     # test_only_fg
        assert result.strategy_name == "selective"
        assert result.has_work_to_do() == True
        assert result.metadata["requested_flowgroups"] == 2
        assert result.metadata["found_flowgroups"] == 2
        
        # Test with no specific flowgroups
        context_empty = GenerationContext("dev", "test_pipeline", True, specific_flowgroups=[])
        result_empty = strategy.filter_flowgroups(flowgroups, context_empty)
        assert len(result_empty.flowgroups_to_generate) == 0
        assert len(result_empty.flowgroups_to_skip) == 3
        assert result_empty.has_work_to_do() == False
    
    def test_fallback_generation_strategy(self):
        """Test FallbackGenerationStrategy.""" 
        strategy = FallbackGenerationStrategy()
        flowgroups = [self.test_flowgroup, self.test_only_flowgroup]
        
        context = GenerationContext("dev", "test_pipeline", False)  # No state manager
        
        result = strategy.filter_flowgroups(flowgroups, context)
        
        # Fallback strategy should generate all flowgroups
        assert len(result.flowgroups_to_generate) == 2
        assert len(result.flowgroups_to_skip) == 0
        assert result.strategy_name == "fallback"
        assert result.has_work_to_do() == True
        assert result.metadata["fallback_reason"] == "no_state_management"
    
    def test_smart_generation_strategy_no_state_manager(self):
        """Test SmartGenerationStrategy fallback when no state manager."""
        strategy = SmartGenerationStrategy()
        flowgroups = [self.test_flowgroup]
        
        context = GenerationContext("dev", "test_pipeline", True)  # No state_manager
        
        result = strategy.filter_flowgroups(flowgroups, context)
        
        # Should fallback to generate all
        assert len(result.flowgroups_to_generate) == 1
        assert len(result.flowgroups_to_skip) == 0
        assert result.strategy_name == "smart"
        assert result.metadata["fallback_reason"] == "no_state_manager"
    
    def test_smart_generation_strategy_with_state(self):
        """Test SmartGenerationStrategy with state management."""
        strategy = SmartGenerationStrategy()
        flowgroups = [self.test_flowgroup, self.test_only_flowgroup]
        
        # Mock state manager
        mock_state_manager = Mock(spec=StateManager)
        mock_state_manager.get_files_needing_generation.return_value = {
            "new": [],
            "stale": [],
            "up_to_date": ["existing_file.py"]
        }
        
        context = GenerationContext(
            "dev", "test_pipeline", False, 
            state_manager=mock_state_manager,
            project_root=self.test_dir
        )
        
        # Mock the context staleness check to return empty
        strategy._check_generation_context_staleness = Mock(return_value=set())
        
        result = strategy.filter_flowgroups(flowgroups, context)
        
        # With no staleness, should skip all flowgroups
        assert len(result.flowgroups_to_generate) == 0
        assert len(result.flowgroups_to_skip) == 2
        assert result.strategy_name == "smart"
        assert result.metadata["new_count"] == 0
        assert result.metadata["stale_count"] == 0


class TestGenerationContext:
    """Test GenerationContext functionality."""
    
    def test_generation_context_creation(self):
        """Test GenerationContext object creation."""
        context = GenerationContext(
            env="dev",
            pipeline_identifier="test_pipeline", 
            include_tests=True,
            specific_flowgroups=["fg1", "fg2"],
            state_manager=Mock(),
            project_root=Path("/test")
        )
        
        assert context.env == "dev"
        assert context.pipeline_identifier == "test_pipeline"
        assert context.include_tests == True
        assert context.specific_flowgroups == ["fg1", "fg2"]
        assert context.state_manager is not None
        assert context.project_root == Path("/test")


class TestGenerationFilterResult:
    """Test GenerationFilterResult functionality."""
    
    def test_generation_filter_result(self):
        """Test GenerationFilterResult object."""
        flowgroup1 = FlowGroup(pipeline="p1", flowgroup="fg1", actions=[])
        flowgroup2 = FlowGroup(pipeline="p1", flowgroup="fg2", actions=[])
        
        result = GenerationFilterResult(
            flowgroups_to_generate=[flowgroup1],
            flowgroups_to_skip=[flowgroup2],
            strategy_name="test_strategy",
            metadata={"custom_info": "test"}
        )
        
        assert result.has_work_to_do() == True
        assert result.strategy_name == "test_strategy"
        
        perf_info = result.get_performance_info()
        assert perf_info["strategy"] == "test_strategy"
        assert perf_info["total_flowgroups"] == 2
        assert perf_info["selected_flowgroups"] == 1
        assert perf_info["skipped_flowgroups"] == 1
        assert perf_info["custom_info"] == "test"
        
        # Test empty result
        empty_result = GenerationFilterResult([], [flowgroup1, flowgroup2], "empty")
        assert empty_result.has_work_to_do() == False


class TestGenerationStrategyFactory:
    """Test GenerationStrategyFactory."""
    
    def test_strategy_factory_creation(self):
        """Test strategy factory creates correct strategies."""
        # Test force strategy
        strategy = GenerationStrategyFactory.create_strategy(
            force=True, specific_flowgroups=None, has_state_manager=True
        )
        assert strategy.name == "force"
        assert isinstance(strategy, ForceGenerationStrategy)
        
        # Test selective strategy
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=["fg1"], has_state_manager=True
        )
        assert strategy.name == "selective"
        assert isinstance(strategy, SelectiveGenerationStrategy)
        
        # Test smart strategy
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=None, has_state_manager=True
        )
        assert strategy.name == "smart"
        assert isinstance(strategy, SmartGenerationStrategy)
        
        # Test fallback strategy
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=None, has_state_manager=False
        )
        assert strategy.name == "fallback"
        assert isinstance(strategy, FallbackGenerationStrategy)
    
    def test_get_available_strategies(self):
        """Test getting all available strategies."""
        strategies = GenerationStrategyFactory.get_available_strategies()
        
        assert len(strategies) == 4
        assert "smart" in strategies
        assert "force" in strategies
        assert "selective" in strategies
        assert "fallback" in strategies
        
        # Verify strategy types
        assert isinstance(strategies["smart"], SmartGenerationStrategy)
        assert isinstance(strategies["force"], ForceGenerationStrategy)
        assert isinstance(strategies["selective"], SelectiveGenerationStrategy)
        assert isinstance(strategies["fallback"], FallbackGenerationStrategy)

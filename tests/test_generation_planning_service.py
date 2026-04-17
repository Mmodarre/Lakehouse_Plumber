"""Tests for GenerationPlanningService."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, MagicMock, patch

from lhp.core.services.generation_planning_service import GenerationPlanningService, GenerationPlan
from lhp.core.services.flowgroup_discoverer import FlowgroupDiscoverer
from lhp.core.state_manager import StateManager
from lhp.core.state_models import FileState
from lhp.models.config import FlowGroup, Action, ActionType


class TestGenerationPlanningService:
    """Test GenerationPlanningService functionality."""
    
    def setup_method(self):
        """Set up test environment."""
        self.test_dir = Path(tempfile.mkdtemp())
        
        # Create minimal project structure
        (self.test_dir / "pipelines").mkdir()
        (self.test_dir / "substitutions").mkdir()
        
        # Mock discoverer
        self.mock_discoverer = Mock(spec=FlowgroupDiscoverer)
        
        # Initialize service
        self.planning_service = GenerationPlanningService(
            self.test_dir, self.mock_discoverer
        )
    
    def test_determine_generation_strategy(self):
        """Test generation strategy determination logic via factory."""
        from lhp.core.strategies import GenerationStrategyFactory
        
        # Test force mode
        strategy = GenerationStrategyFactory.create_strategy(
            force=True, specific_flowgroups=None, has_state_manager=True
        )
        assert strategy.name == "force"
        
        # Test selective mode
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=["test_fg"], has_state_manager=True
        )
        assert strategy.name == "selective"
        
        # Test smart mode
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=None, has_state_manager=True
        )
        assert strategy.name == "smart"
        
        # Test fallback mode
        strategy = GenerationStrategyFactory.create_strategy(
            force=False, specific_flowgroups=None, has_state_manager=False
        )
        assert strategy.name == "fallback"
    
    def test_create_generation_plan_force_mode(self):
        """Test generation plan creation in force mode."""
        # Mock flowgroups
        test_flowgroups = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[]),
            FlowGroup(pipeline="test_pipeline", flowgroup="fg2", actions=[])
        ]
        self.mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = test_flowgroups
        
        # Create plan
        plan = self.planning_service.create_generation_plan(
            env="dev",
            pipeline_identifier="test_pipeline", 
            include_tests=True,
            force=True,
            state_manager=None
        )
        
        # Verify force plan
        assert plan.generation_mode == "force"
        assert len(plan.flowgroups_to_generate) == 2
        assert len(plan.flowgroups_to_skip) == 0
        assert plan.has_work_to_do() == True
        assert plan.performance_info["strategy"] == "force"
        assert plan.performance_info["total_flowgroups"] == 2
    
    def test_create_generation_plan_selective_mode(self):
        """Test generation plan creation in selective mode."""
        # Mock flowgroups
        test_flowgroups = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[]),
            FlowGroup(pipeline="test_pipeline", flowgroup="fg2", actions=[]),
            FlowGroup(pipeline="test_pipeline", flowgroup="fg3", actions=[])
        ]
        self.mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = test_flowgroups
        
        # Create selective plan
        plan = self.planning_service.create_generation_plan(
            env="dev",
            pipeline_identifier="test_pipeline",
            include_tests=False,
            force=False,
            specific_flowgroups=["fg1", "fg3"],
            state_manager=None
        )
        
        # Verify selective plan
        assert plan.generation_mode == "selective"
        assert len(plan.flowgroups_to_generate) == 2  # fg1, fg3
        assert len(plan.flowgroups_to_skip) == 1      # fg2
        assert plan.has_work_to_do() == True
        assert plan.performance_info["strategy"] == "selective"
        # Note: Detailed performance metrics validated via plan attributes above
    
    def test_create_generation_plan_no_flowgroups(self):
        """Test generation plan with no flowgroups found."""
        self.mock_discoverer.discover_flowgroups_by_pipeline_field.return_value = []
        
        plan = self.planning_service.create_generation_plan(
            env="dev",
            pipeline_identifier="nonexistent_pipeline",
            include_tests=True,
            force=False,
            state_manager=Mock()
        )
        
        # Verify empty plan
        assert plan.generation_mode == "empty"  # More accurate for no flowgroups scenario
        assert len(plan.flowgroups_to_generate) == 0
        assert len(plan.flowgroups_to_skip) == 0
        assert plan.has_work_to_do() == False
    
    def test_generation_plan_convenience_methods(self):
        """Test GenerationPlan convenience methods."""
        # Create test flowgroups
        fg1 = FlowGroup(pipeline="pipeline1", flowgroup="fg1", actions=[])
        fg2 = FlowGroup(pipeline="pipeline1", flowgroup="fg2", actions=[])
        fg3 = FlowGroup(pipeline="pipeline2", flowgroup="fg3", actions=[])
        
        plan = GenerationPlan(
            flowgroups_to_generate=[fg1, fg2, fg3],
            flowgroups_to_skip=[],
            generation_mode="smart",
            generation_context_changes={"fg1": "include_tests parameter changed"},
            staleness_summary={"new": 1, "stale": 2, "up_to_date": 0},
            performance_info={},
            detailed_staleness_info={}
        )
        
        # Test has_work_to_do
        assert plan.has_work_to_do() == True
        
        # Test pipeline summary
        summary = plan.get_pipeline_summary()
        assert "pipeline1" in summary
        assert "pipeline2" in summary
        assert len(summary["pipeline1"]["flowgroups"]) == 2
        assert len(summary["pipeline2"]["flowgroups"]) == 1
        assert summary["pipeline1"]["reason"] == "smart"
        
        # Test empty plan
        empty_plan = GenerationPlan(
            flowgroups_to_generate=[],
            flowgroups_to_skip=[fg1],
            generation_mode="smart",
            generation_context_changes={},
            staleness_summary={"new": 0, "stale": 0, "up_to_date": 1},
            performance_info={},
            detailed_staleness_info={}
        )
        
        assert empty_plan.has_work_to_do() == False
        assert empty_plan.get_pipeline_summary() == {}


class TestGenerationPlanningServiceIntegration:
    """Integration tests for GenerationPlanningService with real components."""
    
    def test_discover_flowgroups_for_identifier(self):
        """Test flowgroup discovery for pipeline identifiers."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create real discoverer and planning service
            from lhp.core.project_config_loader import ProjectConfigLoader
            config_loader = ProjectConfigLoader(project_root)
            discoverer = FlowgroupDiscoverer(project_root, config_loader)
            planning_service = GenerationPlanningService(project_root, discoverer)
            
            # Test with non-existent identifier
            flowgroups = planning_service._discover_flowgroups_for_identifier("nonexistent")
            assert len(flowgroups) == 0
            
            # Could add more integration tests with real YAML files if needed

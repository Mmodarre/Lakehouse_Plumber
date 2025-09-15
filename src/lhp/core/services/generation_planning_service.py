"""Generation planning service for LakehousePlumber orchestrator."""

import logging
from pathlib import Path
from typing import Dict, List, Set, Optional, Any
from dataclasses import dataclass

from ..state_manager import StateManager
from ...models.config import FlowGroup, ActionType
from .flowgroup_discoverer import FlowgroupDiscoverer
from ..strategies import (
    GenerationStrategy, GenerationContext, GenerationFilterResult, 
    GenerationStrategyFactory
)


@dataclass
class GenerationPlan:
    """Comprehensive generation plan with all decision information."""
    
    # Core planning decisions
    flowgroups_to_generate: List[FlowGroup]
    flowgroups_to_skip: List[FlowGroup]
    generation_mode: str  # "smart", "force", "selective"
    
    # Context information
    generation_context_changes: Dict[str, str]  # flowgroup_name -> change reason
    staleness_summary: Dict[str, int]  # "new", "stale", "up_to_date" counts
    
    # Metadata for display and coordination
    performance_info: Dict[str, Any]
    detailed_staleness_info: Dict[str, Any]
    
    def has_work_to_do(self) -> bool:
        """Check if any generation work needs to be done."""
        return len(self.flowgroups_to_generate) > 0
    
    def get_pipeline_summary(self) -> Dict[str, Dict]:
        """Get pipeline-level summary for CLI display compatibility."""
        summary = {}
        
        # Group flowgroups by pipeline for summary
        pipeline_groups = {}
        for flowgroup in self.flowgroups_to_generate:
            pipeline = flowgroup.pipeline
            if pipeline not in pipeline_groups:
                pipeline_groups[pipeline] = []
            pipeline_groups[pipeline].append(flowgroup)
        
        # Create summary compatible with existing CLI display logic
        for pipeline, flowgroups in pipeline_groups.items():
            summary[pipeline] = {
                "flowgroups": [fg.flowgroup for fg in flowgroups],
                "reason": self.generation_mode
            }
            
        return summary


class GenerationPlanningService:
    """Centralized generation planning service using strategy pattern."""
    
    def __init__(self, project_root: Path, discoverer: FlowgroupDiscoverer):
        """
        Initialize generation planning service.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
            discoverer: FlowgroupDiscoverer for finding flowgroups
        """
        self.project_root = project_root
        self.discoverer = discoverer
        self.logger = logging.getLogger(__name__)
    
    def create_generation_plan(self, env: str, pipeline_identifier: str, 
                              include_tests: bool, force: bool = False,
                              specific_flowgroups: List[str] = None,
                              state_manager: Optional[StateManager] = None) -> GenerationPlan:
        """
        Create comprehensive generation plan using strategy pattern.
        
        This method uses the strategy pattern to determine generation approach:
        - SmartGenerationStrategy: State-based with context awareness
        - ForceGenerationStrategy: Force all flowgroups
        - SelectiveGenerationStrategy: Only specific flowgroups
        - FallbackGenerationStrategy: No state management available
        
        Args:
            env: Environment to plan for
            pipeline_identifier: Pipeline name or field value
            include_tests: Current include_tests parameter
            force: Force regeneration flag
            specific_flowgroups: Optional list of specific flowgroups to generate
            state_manager: Optional state manager for staleness detection
            
        Returns:
            GenerationPlan object with comprehensive planning information
        """
        # Discover relevant flowgroups
        all_flowgroups = self._discover_flowgroups_for_identifier(pipeline_identifier)
        
        if not all_flowgroups:
            # No flowgroups found - return empty plan
            return GenerationPlan(
                flowgroups_to_generate=[],
                flowgroups_to_skip=[],
                generation_mode="empty",
                generation_context_changes={},
                staleness_summary={"new": 0, "stale": 0, "up_to_date": 0},
                performance_info={"error": "no_flowgroups_found"},
                detailed_staleness_info={}
            )
        
        # Create generation context
        context = GenerationContext(
            env=env,
            pipeline_identifier=pipeline_identifier,
            include_tests=include_tests,
            specific_flowgroups=specific_flowgroups,
            state_manager=state_manager,
            project_root=self.project_root
        )
        
        # Select and execute strategy
        strategy = GenerationStrategyFactory.create_strategy(
            force=force,
            specific_flowgroups=specific_flowgroups,
            has_state_manager=bool(state_manager)
        )
        
        self.logger.debug(f"Using generation strategy: {strategy.name}")
        
        # Execute strategy
        filter_result = strategy.filter_flowgroups(all_flowgroups, context)
        
        # Convert strategy result to GenerationPlan
        return self._convert_filter_result_to_plan(filter_result, state_manager, env)
    
    def _convert_filter_result_to_plan(self, filter_result: GenerationFilterResult, 
                                     state_manager: Optional[StateManager], env: str) -> GenerationPlan:
        """Convert strategy filter result to GenerationPlan."""
        # Extract metadata for generation context changes
        generation_context_changes = {}
        if filter_result.metadata.get("include_tests_context_applied"):
            for flowgroup in filter_result.flowgroups_to_generate:
                if any(action.type == ActionType.TEST for action in flowgroup.actions):
                    generation_context_changes[flowgroup.flowgroup] = "include_tests parameter changed"
        
        # Get detailed staleness info if available
        detailed_staleness_info = {}
        if state_manager:
            try:
                detailed_staleness_info = state_manager.get_detailed_staleness_info(env)
            except Exception as e:
                self.logger.warning(f"Could not get detailed staleness info: {e}")
        
        return GenerationPlan(
            flowgroups_to_generate=filter_result.flowgroups_to_generate,
            flowgroups_to_skip=filter_result.flowgroups_to_skip,
            generation_mode=filter_result.strategy_name,
            generation_context_changes=generation_context_changes,
            staleness_summary=filter_result.metadata.get("staleness_summary", {
                "new": filter_result.metadata.get("new_count", 0),
                "stale": filter_result.metadata.get("stale_count", 0), 
                "up_to_date": filter_result.metadata.get("up_to_date_count", 0)
            }),
            performance_info=filter_result.get_performance_info(),
            detailed_staleness_info=detailed_staleness_info
        )
    
    def analyze_generation_context_staleness(self, flowgroups: List[FlowGroup], 
                                           env: str, include_tests: bool,
                                           state_manager: StateManager) -> Set[str]:
        """
        Analyze staleness due to generation parameter changes.
        
        This method provides backward compatibility for direct calls from orchestrator
        while strategies handle this logic internally.
        
        Args:
            flowgroups: List of flowgroups to analyze
            env: Environment name
            include_tests: Current include_tests parameter 
            state_manager: State manager instance
            
        Returns:
            Set of flowgroup names that are stale due to context changes
        """
        # Create temporary context for strategy
        context = GenerationContext(
            env=env,
            pipeline_identifier="",  # Not needed for this analysis
            include_tests=include_tests,
            state_manager=state_manager,
            project_root=self.project_root
        )
        
        # Use smart strategy to check context staleness
        from ..strategies import SmartGenerationStrategy
        strategy = SmartGenerationStrategy()
        return strategy._check_generation_context_staleness(flowgroups, context)
    
    def _discover_flowgroups_for_identifier(self, pipeline_identifier: str) -> List[FlowGroup]:
        """Discover flowgroups for pipeline identifier (name or field)."""
        # Try as pipeline field first (most common usage)
        flowgroups = self.discoverer.discover_flowgroups_by_pipeline_field(pipeline_identifier)
        
        if not flowgroups:
            # Fallback: try as directory name
            pipeline_dir = self.project_root / "pipelines" / pipeline_identifier
            if pipeline_dir.exists():
                flowgroups = self.discoverer.discover_flowgroups(pipeline_dir)
        
        return flowgroups
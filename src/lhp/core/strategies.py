"""Strategy pattern implementations for LakehousePlumber generation logic."""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Set, Optional, Any, Protocol
import logging

from .state_manager import StateManager
from ..models.config import FlowGroup, ActionType
from .state_dependency_resolver import StateDependencyResolver
from ..parsers.yaml_parser import YAMLParser


class GenerationStrategy(Protocol):
    """Protocol defining the interface for generation strategies."""
    
    def filter_flowgroups(self, all_flowgroups: List[FlowGroup], 
                         context: 'GenerationContext') -> 'GenerationFilterResult':
        """
        Filter flowgroups based on this strategy's logic.
        
        Args:
            all_flowgroups: Complete list of discovered flowgroups
            context: Generation context with environment, parameters, state manager
            
        Returns:
            GenerationFilterResult with flowgroups to generate and metadata
        """
        ...


class GenerationContext:
    """Context object containing all generation parameters and environment info."""
    
    def __init__(self, env: str, pipeline_identifier: str, include_tests: bool,
                 specific_flowgroups: List[str] = None, state_manager: StateManager = None,
                 project_root: Path = None):
        self.env = env
        self.pipeline_identifier = pipeline_identifier
        self.include_tests = include_tests
        self.specific_flowgroups = specific_flowgroups or []
        self.state_manager = state_manager
        self.project_root = project_root


class GenerationFilterResult:
    """Result object from generation strategy filtering."""
    
    def __init__(self, flowgroups_to_generate: List[FlowGroup], 
                 flowgroups_to_skip: List[FlowGroup],
                 strategy_name: str, metadata: Dict[str, Any] = None):
        self.flowgroups_to_generate = flowgroups_to_generate
        self.flowgroups_to_skip = flowgroups_to_skip
        self.strategy_name = strategy_name
        self.metadata = metadata or {}
        
    def has_work_to_do(self) -> bool:
        """Check if any generation work needs to be done."""
        return len(self.flowgroups_to_generate) > 0
    
    def get_performance_info(self) -> Dict[str, Any]:
        """Get performance information for this strategy execution."""
        return {
            "strategy": self.strategy_name,
            "total_flowgroups": len(self.flowgroups_to_generate) + len(self.flowgroups_to_skip),
            "selected_flowgroups": len(self.flowgroups_to_generate),
            "skipped_flowgroups": len(self.flowgroups_to_skip),
            **self.metadata
        }


class BaseGenerationStrategy:
    """Base class for generation strategies with common functionality."""
    
    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _check_generation_context_staleness(self, flowgroups: List[FlowGroup],
                                          context: GenerationContext) -> Set[str]:
        """
        Check for flowgroups that are stale due to generation context changes.
        Shared logic across strategies that need context awareness.
        """
        if not context.state_manager:
            return set()
            
        context_stale = set()
        tracked_files = context.state_manager.get_generated_files(context.env)
        
        for flowgroup in flowgroups:
            # Check if this flowgroup has test actions
            has_test_actions = any(action.type == ActionType.TEST for action in flowgroup.actions)
            
            if has_test_actions:
                # Calculate what the current generation context should be
                current_context = f"include_tests:{context.include_tests}"
                
                # Find the tracked file for this flowgroup
                flowgroup_file_path = f"generated/{context.env}/{flowgroup.pipeline}/{flowgroup.flowgroup}.py"
                
                if flowgroup_file_path in tracked_files:
                    file_state = tracked_files[flowgroup_file_path]
                    
                    # Check if composite checksum would change with current context
                    if self._would_composite_checksum_change(file_state, current_context, context):
                        context_stale.add(flowgroup.flowgroup)
                        self.logger.debug(f"Flowgroup {flowgroup.flowgroup} is stale due to generation context change")
        
        return context_stale
    
    def _would_composite_checksum_change(self, file_state, current_context: str, 
                                       context: GenerationContext) -> bool:
        """Check if composite checksum would change with current generation context."""
        try:
            # Reuse dependency resolver from state_manager's analyzer to avoid creating new instances
            if context.state_manager and hasattr(context.state_manager, 'analyzer'):
                dependency_resolver = context.state_manager.analyzer.dependency_resolver
            else:
                # Fallback: create new instance if state_manager not available
                dependency_resolver = StateDependencyResolver(context.project_root)
            
            source_path = Path(file_state.source_yaml)
            
            if not (context.project_root / source_path).exists():
                return False
                
            file_dependencies = dependency_resolver.resolve_file_dependencies(
                source_path, context.env, file_state.pipeline, file_state.flowgroup
            )
            
            # Calculate current composite checksum with current generation context
            dep_paths = [file_state.source_yaml] + list(file_dependencies.keys())
            if current_context:
                dep_paths.append(current_context)
            
            current_composite_checksum = dependency_resolver.calculate_composite_checksum(dep_paths)
            
            # Compare with stored composite checksum
            return file_state.file_composite_checksum != current_composite_checksum
            
        except Exception as e:
            self.logger.warning(f"Failed to check composite checksum for {file_state.generated_path}: {e}")
            return True  # Assume changed to be safe


class SmartGenerationStrategy(BaseGenerationStrategy):
    """Strategy for intelligent state-based generation with context awareness."""
    
    def __init__(self):
        super().__init__("smart")
    
    def filter_flowgroups(self, all_flowgroups: List[FlowGroup], 
                         context: GenerationContext) -> GenerationFilterResult:
        """Filter based on staleness detection and generation context."""
        if not context.state_manager:
            # Fallback to generating all if no state available
            return GenerationFilterResult(
                flowgroups_to_generate=all_flowgroups,
                flowgroups_to_skip=[],
                strategy_name=self.name,
                metadata={"fallback_reason": "no_state_manager"}
            )
        
        # Get basic staleness information
        generation_info = context.state_manager.get_files_needing_generation(
            context.env, context.pipeline_identifier
        )
        
        # Parse new YAML files to get flowgroup names
        new_flowgroup_names = set()
        for yaml_path in generation_info["new"]:
            try:
                parser = YAMLParser()
                # Parse all flowgroups from file (supports multi-document and array syntax)
                flowgroups = parser.parse_flowgroups_from_file(yaml_path)
                for fg in flowgroups:
                    new_flowgroup_names.add(fg.flowgroup)
            except Exception as e:
                self.logger.warning(f"Could not parse new flowgroup {yaml_path}: {e}")
        
        # Get flowgroups for stale files
        stale_flowgroup_names = {fs.flowgroup for fs in generation_info["stale"]}
        
        # Check for generation context staleness
        generation_context_stale = self._check_generation_context_staleness(all_flowgroups, context)
        
        # Combine all flowgroups that need generation
        flowgroups_needing_generation = new_flowgroup_names | stale_flowgroup_names | generation_context_stale
        
        # Split flowgroups into generate vs skip
        flowgroups_to_generate = [fg for fg in all_flowgroups if fg.flowgroup in flowgroups_needing_generation]
        flowgroups_to_skip = [fg for fg in all_flowgroups if fg.flowgroup not in flowgroups_needing_generation]
        
        return GenerationFilterResult(
            flowgroups_to_generate=flowgroups_to_generate,
            flowgroups_to_skip=flowgroups_to_skip,
            strategy_name=self.name,
            metadata={
                "new_count": len(generation_info["new"]),
                "stale_count": len(generation_info["stale"]),
                "up_to_date_count": len(generation_info["up_to_date"]),
                "context_stale_count": len(generation_context_stale),
                "include_tests_context_applied": bool(generation_context_stale)
            }
        )


class ForceGenerationStrategy(BaseGenerationStrategy):
    """Strategy for forcing regeneration of all flowgroups regardless of state."""
    
    def __init__(self):
        super().__init__("force")
    
    def filter_flowgroups(self, all_flowgroups: List[FlowGroup], 
                         context: GenerationContext) -> GenerationFilterResult:
        """Force generation of all flowgroups."""
        return GenerationFilterResult(
            flowgroups_to_generate=all_flowgroups,
            flowgroups_to_skip=[],
            strategy_name=self.name,
            metadata={
                "total_flowgroups": len(all_flowgroups),
                "force_reason": "user_requested"
            }
        )


class SelectiveGenerationStrategy(BaseGenerationStrategy):
    """Strategy for generating only specific flowgroups."""
    
    def __init__(self):
        super().__init__("selective")
    
    def filter_flowgroups(self, all_flowgroups: List[FlowGroup], 
                         context: GenerationContext) -> GenerationFilterResult:
        """Filter to only specified flowgroups."""
        if not context.specific_flowgroups:
            # If no specific flowgroups specified, generate none
            return GenerationFilterResult(
                flowgroups_to_generate=[],
                flowgroups_to_skip=all_flowgroups,
                strategy_name=self.name,
                metadata={"error": "no_specific_flowgroups_specified"}
            )
        
        flowgroups_to_generate = [
            fg for fg in all_flowgroups 
            if fg.flowgroup in context.specific_flowgroups
        ]
        flowgroups_to_skip = [
            fg for fg in all_flowgroups 
            if fg.flowgroup not in context.specific_flowgroups
        ]
        
        return GenerationFilterResult(
            flowgroups_to_generate=flowgroups_to_generate,
            flowgroups_to_skip=flowgroups_to_skip,
            strategy_name=self.name,
            metadata={
                "requested_flowgroups": len(context.specific_flowgroups),
                "found_flowgroups": len(flowgroups_to_generate),
                "total_flowgroups": len(all_flowgroups)
            }
        )


class FallbackGenerationStrategy(BaseGenerationStrategy):
    """Strategy for fallback generation when no state management is available."""
    
    def __init__(self):
        super().__init__("fallback")
    
    def filter_flowgroups(self, all_flowgroups: List[FlowGroup], 
                         context: GenerationContext) -> GenerationFilterResult:
        """Generate all flowgroups when no intelligent filtering is possible."""
        return GenerationFilterResult(
            flowgroups_to_generate=all_flowgroups,
            flowgroups_to_skip=[],
            strategy_name=self.name,
            metadata={
                "total_flowgroups": len(all_flowgroups),
                "fallback_reason": "no_state_management"
            }
        )


class GenerationStrategyFactory:
    """Factory for creating generation strategies based on context."""
    
    @staticmethod
    def create_strategy(force: bool, specific_flowgroups: List[str], 
                       has_state_manager: bool) -> GenerationStrategy:
        """
        Create appropriate generation strategy based on parameters.
        
        Args:
            force: Force regeneration flag
            specific_flowgroups: List of specific flowgroups (if any)
            has_state_manager: Whether state management is available
            
        Returns:
            Appropriate GenerationStrategy instance
        """
        if force:
            return ForceGenerationStrategy()
        elif specific_flowgroups:
            return SelectiveGenerationStrategy()
        elif has_state_manager:
            return SmartGenerationStrategy()
        else:
            return FallbackGenerationStrategy()
    
    @staticmethod
    def get_available_strategies() -> Dict[str, GenerationStrategy]:
        """Get all available generation strategies."""
        return {
            "smart": SmartGenerationStrategy(),
            "force": ForceGenerationStrategy(), 
            "selective": SelectiveGenerationStrategy(),
            "fallback": FallbackGenerationStrategy()
        }

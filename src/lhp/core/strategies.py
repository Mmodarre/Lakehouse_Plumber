"""Strategy pattern implementations for LakehousePlumber generation logic."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol

from ..models.config import FlowGroup
from ..parsers.yaml_parser import YAMLParser
from .state_manager import StateManager


class GenerationStrategy(Protocol):
    """Protocol defining the interface for generation strategies."""

    def filter_flowgroups(
        self, all_flowgroups: List[FlowGroup], context: "GenerationContext"
    ) -> "GenerationFilterResult":
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

    def __init__(
        self,
        env: str,
        pipeline_identifier: str,
        include_tests: bool,
        specific_flowgroups: List[str] = None,
        state_manager: StateManager = None,
        project_root: Path = None,
    ):
        self.env = env
        self.pipeline_identifier = pipeline_identifier
        self.include_tests = include_tests
        self.specific_flowgroups = specific_flowgroups or []
        self.state_manager = state_manager
        self.project_root = project_root


class GenerationFilterResult:
    """Result object from generation strategy filtering."""

    def __init__(
        self,
        flowgroups_to_generate: List[FlowGroup],
        flowgroups_to_skip: List[FlowGroup],
        strategy_name: str,
        metadata: Dict[str, Any] = None,
    ):
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
            "total_flowgroups": len(self.flowgroups_to_generate)
            + len(self.flowgroups_to_skip),
            "selected_flowgroups": len(self.flowgroups_to_generate),
            "skipped_flowgroups": len(self.flowgroups_to_skip),
            **self.metadata,
        }


class BaseGenerationStrategy:
    """Base class for generation strategies with common functionality."""

    def __init__(self, name: str):
        self.name = name
        self.logger = logging.getLogger(__name__)


class SmartGenerationStrategy(BaseGenerationStrategy):
    """Strategy for intelligent state-based generation with context awareness."""

    def __init__(self):
        super().__init__("smart")

    def filter_flowgroups(
        self, all_flowgroups: List[FlowGroup], context: GenerationContext
    ) -> GenerationFilterResult:
        """Filter based on staleness detection and generation context."""
        if not context.state_manager:
            # Fallback to generating all if no state available
            return GenerationFilterResult(
                flowgroups_to_generate=all_flowgroups,
                flowgroups_to_skip=[],
                strategy_name=self.name,
                metadata={"fallback_reason": "no_state_manager"},
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

        # Env-wide context changes (e.g. include_tests flip) are handled by
        # the display-phase gate in ActionOrchestrator.analyze_generation_requirements
        # — no per-flowgroup R5 composite-checksum work here.
        flowgroups_needing_generation = new_flowgroup_names | stale_flowgroup_names

        flowgroups_to_generate = [
            fg for fg in all_flowgroups if fg.flowgroup in flowgroups_needing_generation
        ]
        flowgroups_to_skip = [
            fg
            for fg in all_flowgroups
            if fg.flowgroup not in flowgroups_needing_generation
        ]

        return GenerationFilterResult(
            flowgroups_to_generate=flowgroups_to_generate,
            flowgroups_to_skip=flowgroups_to_skip,
            strategy_name=self.name,
            metadata={
                "new_count": len(generation_info["new"]),
                "stale_count": len(generation_info["stale"]),
                "up_to_date_count": len(generation_info["up_to_date"]),
            },
        )


class ForceGenerationStrategy(BaseGenerationStrategy):
    """Strategy for forcing regeneration of all flowgroups regardless of state."""

    def __init__(self):
        super().__init__("force")

    def filter_flowgroups(
        self, all_flowgroups: List[FlowGroup], context: GenerationContext
    ) -> GenerationFilterResult:
        """Force generation of all flowgroups."""
        return GenerationFilterResult(
            flowgroups_to_generate=all_flowgroups,
            flowgroups_to_skip=[],
            strategy_name=self.name,
            metadata={
                "total_flowgroups": len(all_flowgroups),
                "force_reason": "user_requested",
            },
        )


class SelectiveGenerationStrategy(BaseGenerationStrategy):
    """Strategy for generating only specific flowgroups."""

    def __init__(self):
        super().__init__("selective")

    def filter_flowgroups(
        self, all_flowgroups: List[FlowGroup], context: GenerationContext
    ) -> GenerationFilterResult:
        """Filter to only specified flowgroups."""
        if not context.specific_flowgroups:
            # If no specific flowgroups specified, generate none
            return GenerationFilterResult(
                flowgroups_to_generate=[],
                flowgroups_to_skip=all_flowgroups,
                strategy_name=self.name,
                metadata={"error": "no_specific_flowgroups_specified"},
            )

        flowgroups_to_generate = [
            fg for fg in all_flowgroups if fg.flowgroup in context.specific_flowgroups
        ]
        flowgroups_to_skip = [
            fg
            for fg in all_flowgroups
            if fg.flowgroup not in context.specific_flowgroups
        ]

        return GenerationFilterResult(
            flowgroups_to_generate=flowgroups_to_generate,
            flowgroups_to_skip=flowgroups_to_skip,
            strategy_name=self.name,
            metadata={
                "requested_flowgroups": len(context.specific_flowgroups),
                "found_flowgroups": len(flowgroups_to_generate),
                "total_flowgroups": len(all_flowgroups),
            },
        )


class FallbackGenerationStrategy(BaseGenerationStrategy):
    """Strategy for fallback generation when no state management is available."""

    def __init__(self):
        super().__init__("fallback")

    def filter_flowgroups(
        self, all_flowgroups: List[FlowGroup], context: GenerationContext
    ) -> GenerationFilterResult:
        """Generate all flowgroups when no intelligent filtering is possible."""
        return GenerationFilterResult(
            flowgroups_to_generate=all_flowgroups,
            flowgroups_to_skip=[],
            strategy_name=self.name,
            metadata={
                "total_flowgroups": len(all_flowgroups),
                "fallback_reason": "no_state_management",
            },
        )


class GenerationStrategyFactory:
    """Factory for creating generation strategies based on context."""

    @staticmethod
    def create_strategy(
        force: bool, specific_flowgroups: List[str], has_state_manager: bool
    ) -> GenerationStrategy:
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
            "fallback": FallbackGenerationStrategy(),
        }

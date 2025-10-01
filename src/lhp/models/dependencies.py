"""Data models for dependency analysis in LakehousePlumber."""

from dataclasses import dataclass
from typing import List, Dict, Any, Optional
import networkx as nx


@dataclass
class DependencyGraphs:
    """Container for dependency graphs at different levels of granularity."""

    action_graph: nx.DiGraph
    flowgroup_graph: nx.DiGraph
    pipeline_graph: nx.DiGraph
    metadata: Dict[str, Any]

    def get_graph_by_level(self, level: str) -> nx.DiGraph:
        """Get graph by level name."""
        level_map = {
            "action": self.action_graph,
            "flowgroup": self.flowgroup_graph,
            "pipeline": self.pipeline_graph
        }
        if level not in level_map:
            raise ValueError(f"Unknown level: {level}. Must be one of: {list(level_map.keys())}")
        return level_map[level]


@dataclass
class PipelineDependency:
    """Represents dependencies for a single pipeline."""

    pipeline: str
    depends_on: List[str]
    flowgroup_count: int
    action_count: int
    external_sources: List[str]
    can_run_parallel: bool = False
    stage: Optional[int] = None


@dataclass
class DependencyAnalysisResult:
    """Complete result of dependency analysis."""

    graphs: DependencyGraphs
    pipeline_dependencies: Dict[str, PipelineDependency]
    execution_stages: List[List[str]]
    circular_dependencies: List[List[str]]
    external_sources: List[str]

    @property
    def total_pipelines(self) -> int:
        """Total number of pipelines analyzed."""
        return len(self.pipeline_dependencies)

    @property
    def total_external_sources(self) -> int:
        """Total number of external sources identified."""
        return len(self.external_sources)

    def get_pipeline_execution_order(self) -> List[str]:
        """Get flat list of pipelines in execution order."""
        execution_order = []
        for stage in self.execution_stages:
            execution_order.extend(stage)
        return execution_order


@dataclass
class ActionDependencyInfo:
    """Information about action-level dependencies."""

    name: str
    type: str
    flowgroup: str
    pipeline: str
    sources: List[str]
    target: Optional[str]
    external_sources: List[str]
    internal_sources: List[str]

    def has_external_dependencies(self) -> bool:
        """Check if action depends on external sources."""
        return len(self.external_sources) > 0

    def has_internal_dependencies(self) -> bool:
        """Check if action depends on other actions."""
        return len(self.internal_sources) > 0


@dataclass
class FlowgroupDependencyInfo:
    """Information about flowgroup-level dependencies."""

    name: str
    pipeline: str
    actions: List[ActionDependencyInfo]
    depends_on_flowgroups: List[str]
    external_sources: List[str]

    @property
    def action_count(self) -> int:
        """Number of actions in this flowgroup."""
        return len(self.actions)

    def get_load_actions(self) -> List[ActionDependencyInfo]:
        """Get all load actions in this flowgroup."""
        return [action for action in self.actions if action.type == "load"]

    def get_write_actions(self) -> List[ActionDependencyInfo]:
        """Get all write actions in this flowgroup."""
        return [action for action in self.actions if action.type == "write"]

    def get_transform_actions(self) -> List[ActionDependencyInfo]:
        """Get all transform actions in this flowgroup."""
        return [action for action in self.actions if action.type == "transform"]
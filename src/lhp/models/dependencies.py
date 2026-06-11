"""Data models for dependency analysis in LakehousePlumber."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import networkx as nx

from ..errors import ErrorFactory, codes


@dataclass(frozen=True)
class DependencyWarning:
    """Advisory record from dependency extraction (LHP-DEP-002 / LHP-DEP-003).

    Warning-only: carried on analysis results for presentation, never raised.
    """

    code: str
    message: str
    flowgroup: str
    action: str
    suggestion: str
    file_path: Optional[str] = None
    line: Optional[int] = None


@dataclass
class DependencyGraphs:
    action_graph: nx.DiGraph
    flowgroup_graph: nx.DiGraph
    pipeline_graph: nx.DiGraph
    metadata: Dict[str, Any]
    extraction_warnings: List[DependencyWarning] = field(default_factory=list)

    def get_graph_by_level(self, level: str) -> nx.DiGraph:
        level_map = {
            "action": self.action_graph,
            "flowgroup": self.flowgroup_graph,
            "pipeline": self.pipeline_graph,
        }
        if level not in level_map:
            raise ErrorFactory.validation_error(
                codes.VAL_020,
                title="Unknown dependency graph level",
                details=f"Unknown level: '{level}'.",
                suggestions=[
                    f"Use one of: {', '.join(level_map.keys())}",
                ],
                context={"Level": level},
            )
        return level_map[level]


@dataclass
class PipelineDependency:
    pipeline: str
    depends_on: List[str]
    flowgroup_count: int
    action_count: int
    external_sources: List[str]
    can_run_parallel: bool = False
    stage: Optional[int] = None


@dataclass
class DependencyAnalysisResult:
    graphs: DependencyGraphs
    pipeline_dependencies: Dict[str, PipelineDependency]
    execution_stages: List[List[str]]
    circular_dependencies: List[List[str]]
    external_sources: List[str]
    warnings: List[DependencyWarning] = field(default_factory=list)

    @property
    def total_pipelines(self) -> int:
        return len(self.pipeline_dependencies)

    @property
    def total_external_sources(self) -> int:
        return len(self.external_sources)

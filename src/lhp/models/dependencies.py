"""Data models for dependency analysis in LakehousePlumber."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import networkx as nx

from ..errors import ErrorFactory, codes


@dataclass(frozen=True)
class AffectedAction:
    """One action affected by an aggregated extraction-warning site.

    ``edit_yaml_path`` names the YAML file where a ``depends_on`` entry for
    this action belongs — the flowgroup YAML, or the blueprint YAML for a
    blueprint-expanded synthetic flowgroup.
    """

    flowgroup: str
    action: str
    edit_yaml_path: Optional[str] = None


@dataclass(frozen=True)
class DependencyWarning:
    """Advisory record from dependency extraction (LHP-DEP-002 / LHP-DEP-003).

    Warning-only: carried on analysis results for presentation, never raised.

    Parsers emit LEAF records (one per opaque read per action;
    ``affected_actions`` empty, ``affected_count`` 1). The graph builder
    aggregates leaves by warning SITE — ``(code, file_path, line, message)``
    — into one record per site whose ``flowgroup`` / ``action`` are the
    first affected action (sorted) and whose ``affected_actions`` /
    ``affected_count`` enumerate every distinct action referencing the site.
    ``edit_yaml_path`` is the YAML file a ``depends_on`` fix belongs in.
    """

    code: str
    message: str
    flowgroup: str
    action: str
    suggestion: str
    file_path: Optional[str] = None
    line: Optional[int] = None
    edit_yaml_path: Optional[str] = None
    affected_actions: Tuple[AffectedAction, ...] = ()
    affected_count: int = 1


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

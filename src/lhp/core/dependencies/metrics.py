"""Placeholder advanced-metrics service for dependency analysis.

Hosts NetworkX-backed metrics (critical path, parallelization, centrality)
that are scheduled for future phases per LOCAL/TARGET_ARCHITECTURE.md §2.
Each method currently raises ``NotImplementedError`` so callers fail
loudly until the implementations land.
"""

from typing import Any, Dict, List

from ...models.dependencies import DependencyGraphs


class DependencyMetricsService:
    """Advanced dependency-graph metrics.

    Stateless. Instantiated by the dependency-analysis composition root
    and exposed as ``self._metrics`` for forwarding from the public
    service surface.
    """

    def __init__(self) -> None:
        """Construct a metrics service. Stateless — no setup required."""

    def get_critical_path(self, graphs: DependencyGraphs) -> List[str]:
        """TODO: Find longest dependency chain through the pipeline graph."""
        # Implementation: nx.dag_longest_path(graphs.pipeline_graph)
        raise NotImplementedError("Critical path analysis - Phase 2")

    def get_parallelization_opportunities(
        self, graphs: DependencyGraphs
    ) -> Dict[str, Any]:
        """TODO: Identify pipelines that can run in parallel."""
        # Implementation: Use topological_generations for parallel groups
        raise NotImplementedError("Parallelization analysis - Phase 2")

    def get_centrality_metrics(self, graphs: DependencyGraphs) -> Dict[str, float]:
        """TODO: Identify hub pipelines with high dependency count."""
        # Implementation: nx.betweenness_centrality(graphs.pipeline_graph)
        raise NotImplementedError("Centrality analysis - Phase 3")

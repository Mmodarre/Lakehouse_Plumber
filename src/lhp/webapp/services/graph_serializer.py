"""Serialize the public dependency-analysis result into the frontend graph format.

Consumes the public, networkx-free
:class:`lhp.api.DependencyAnalysisResult` returned by
:meth:`InspectionFacade.analyze_dependencies` and projects it onto the
React-Flow-facing ``GraphResponse`` / ``GraphNode`` / ``GraphEdge`` schemas in
:mod:`lhp.webapp.schemas.dependency`.

v1 SCOPE — PIPELINE-LEVEL graph only:

* Nodes are the pipelines plus the external (project-unowned) sources.
* Edges are the pipeline-to-pipeline dependencies. An edge ``D -> P`` means
  pipeline ``P`` depends on pipeline ``D`` (data flows ``D -> P``), preserving
  the predecessor-to-node direction of the legacy networkx pipeline graph.

The public result intentionally drops the live graph objects, so cycle
detection, execution-stage assignment, and external-source enumeration are
read straight from the flattened result rather than recomputed from a graph.
Because the public DTO carries only the *global* external-source list (not the
per-pipeline mapping), external sources are emitted as standalone nodes with no
incident edges in v1.
"""

import logging

from lhp.api import DependencyAnalysisResult
from lhp.webapp.schemas.dependency import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    GraphResponse,
)

logger = logging.getLogger(__name__)


def serialize_pipeline_graph(result: DependencyAnalysisResult) -> GraphResponse:
    """Serialize a public dependency-analysis result into the frontend graph format.

    Args:
        result: The public, networkx-free dependency-analysis result returned
            by :meth:`InspectionFacade.analyze_dependencies`.

    Returns:
        A :class:`GraphResponse` with pipeline + external-source nodes,
        pipeline-to-pipeline edges, and metadata (level ``"pipeline"``,
        cycle summary, external sources).
    """
    stage_by_pipeline = _stage_index(result)
    nodes = _build_nodes(result, stage_by_pipeline)
    edges = _build_edges(result)
    metadata = _build_metadata(result, nodes, edges)

    return GraphResponse(nodes=nodes, edges=edges, metadata=metadata)


def _stage_index(result: DependencyAnalysisResult) -> dict[str, int]:
    """Map each pipeline to its execution stage (0-based generation index)."""
    stage_by_pipeline: dict[str, int] = {}
    for stage_idx, stage in enumerate(result.execution_stages):
        for pipeline in stage:
            stage_by_pipeline[pipeline] = stage_idx
    return stage_by_pipeline


def _build_nodes(
    result: DependencyAnalysisResult, stage_by_pipeline: dict[str, int]
) -> list[GraphNode]:
    """Build pipeline nodes plus external-source nodes."""
    # Pipeline names: keys of pipeline_dependencies, plus any pipeline that only
    # ever appears as a dependency target (defensive — should already be a key).
    pipeline_names: list[str] = []
    seen: set[str] = set()
    for name in result.pipeline_dependencies:
        if name not in seen:
            seen.add(name)
            pipeline_names.append(name)
    for deps in result.pipeline_dependencies.values():
        for dep in deps:
            if dep not in seen:
                seen.add(dep)
                pipeline_names.append(dep)

    nodes: list[GraphNode] = []
    for name in pipeline_names:
        depends_on = list(result.pipeline_dependencies.get(name, ()))
        nodes.append(
            GraphNode(
                id=name,
                label=name,
                type="pipeline",
                pipeline=name,
                flowgroup="",
                stage=stage_by_pipeline.get(name, 0),
                metadata={"depends_on_count": len(depends_on)},
            )
        )

    # External sources are standalone nodes (no incident edges in v1).
    for source in result.external_sources:
        nodes.append(
            GraphNode(
                id=source,
                label=source,
                type="external",
                pipeline="",
                flowgroup="",
                stage=0,
                metadata={"external": True},
            )
        )

    return nodes


def _build_edges(result: DependencyAnalysisResult) -> list[GraphEdge]:
    """Build pipeline-to-pipeline edges from the dependency mapping.

    For a pipeline ``P`` with ``depends_on=[D, ...]`` the edge is ``D -> P``
    (source = dependency, target = dependent pipeline), matching the legacy
    networkx pipeline graph where ``depends_on`` was the node's predecessors.
    """
    edges: list[GraphEdge] = []
    for pipeline, deps in result.pipeline_dependencies.items():
        for dep in deps:
            edges.append(GraphEdge(source=dep, target=pipeline, type="pipeline"))
    return edges


def _build_metadata(
    result: DependencyAnalysisResult,
    nodes: list[GraphNode],
    edges: list[GraphEdge],
) -> GraphMetadata:
    """Build graph metadata, reflecting the result's cycle/stage/external state.

    ``circular_dependencies`` is read straight from the public result (each
    entry is the analyzer's formatted cycle description, e.g.
    ``"pipeline level: a -> b -> a"``); ``has_circular`` mirrors whether any
    were detected.
    """
    circular = [list(cycle) for cycle in result.circular_dependencies]

    return GraphMetadata(
        level="pipeline",
        total_nodes=len(nodes),
        total_edges=len(edges),
        stages=len(result.execution_stages),
        has_circular=result.has_cycles,
        circular_dependencies=circular,
        external_sources=list(result.external_sources),
    )

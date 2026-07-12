"""Serialize the public dependency-analysis result into the frontend graph format.

Consumes the public, networkx-free
:class:`lhp.api.DependencyAnalysisResult` returned by
:meth:`InspectionFacade.analyze_dependencies` and projects it onto the
React-Flow-facing ``GraphResponse`` / ``GraphNode`` / ``GraphEdge`` schemas in
:mod:`lhp.webapp.schemas.dependency`.

Three serializers, one per graph level:

* :func:`serialize_pipeline_graph` — nodes are the pipelines plus the external
  (project-unowned) sources; edges are the pipeline-to-pipeline dependencies.
  An edge ``D -> P`` means pipeline ``P`` depends on pipeline ``D`` (data flows
  ``D -> P``), preserving the predecessor-to-node direction of the legacy
  networkx pipeline graph. Built from the flattened summary alone; because the
  public DTO carries only the *global* external-source list (not the
  per-pipeline mapping), external sources are standalone nodes with no
  incident edges at this level.
* :func:`serialize_flowgroup_graph` / :func:`serialize_action_graph` — project
  the opt-in :class:`lhp.api.DependencyGraphView` snapshots (analysis must be
  run with ``include_graphs=True``). Edge types are re-derived from the
  endpoint nodes for frontend styling (``internal`` / ``cross_flowgroup`` /
  ``cross_pipeline``), and per-node ``external_sources`` metadata is expanded
  into standalone external nodes with ``external`` edges into their consumers.

The public result carries no live graph objects, so cycle detection,
execution-stage assignment, and external-source enumeration are read straight
from the flattened result rather than recomputed from a graph.
"""

import logging
from typing import Optional

from lhp.api import (
    DependencyAnalysisResult,
    DependencyGraphNodeView,
    DependencyGraphView,
)
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


def serialize_flowgroup_graph(result: DependencyAnalysisResult) -> GraphResponse:
    """Serialize the flowgroup-level graph snapshot into the frontend format.

    Args:
        result: A dependency-analysis result produced with
            ``analyze_dependencies(include_graphs=True)``.

    Returns:
        A :class:`GraphResponse` with flowgroup + external-source nodes,
        flowgroup-to-flowgroup edges, and metadata (level ``"flowgroup"``,
        cycle summary, external sources).

    Raises:
        ValueError: If ``result`` carries no flowgroup graph snapshot.
    """
    graph = _require_graph(result.flowgroup_graph, "flowgroup")
    return _serialize_graph_view(graph, result)


def serialize_action_graph(result: DependencyAnalysisResult) -> GraphResponse:
    """Serialize the action-level graph snapshot into the frontend format.

    Args:
        result: A dependency-analysis result produced with
            ``analyze_dependencies(include_graphs=True)``.

    Returns:
        A :class:`GraphResponse` with action + external-source nodes
        (action ids are ``{flowgroup}.{action}``), action-to-action edges,
        and metadata (level ``"action"``, cycle summary, external sources).

    Raises:
        ValueError: If ``result`` carries no action graph snapshot.
    """
    graph = _require_graph(result.action_graph, "action")
    return _serialize_graph_view(graph, result)


def _require_graph(
    graph: Optional[DependencyGraphView], level: str
) -> DependencyGraphView:
    """Return the level snapshot or fail fast when the caller skipped opt-in."""
    if graph is None:
        raise ValueError(
            f"analysis result carries no {level!r} graph snapshot; "
            "produce it with analyze_dependencies(include_graphs=True)"
        )
    return graph


def _serialize_graph_view(
    graph: DependencyGraphView, result: DependencyAnalysisResult
) -> GraphResponse:
    """Project a public level snapshot onto the React-Flow-facing schema.

    Stages are a pipeline-level concept, so every node at these levels keeps
    the schema-default ``stage=0`` (the frontend layouts the drill views
    without partitioning).
    """
    nodes = [
        GraphNode(
            id=node.id,
            label=node.label,
            type=node.type,
            pipeline=node.pipeline,
            flowgroup=node.flowgroup,
            stage=0,
            metadata=dict(node.metadata),
        )
        for node in graph.nodes
    ]

    node_by_id = {node.id: node for node in graph.nodes}
    edges = [
        GraphEdge(
            source=edge.source,
            target=edge.target,
            type=_edge_display_type(
                node_by_id.get(edge.source), node_by_id.get(edge.target), graph.level
            ),
        )
        for edge in graph.edges
    ]

    _append_external_sources(graph, nodes, edges)

    metadata = GraphMetadata(
        level=graph.level,
        total_nodes=len(nodes),
        total_edges=len(edges),
        stages=len(result.execution_stages),
        has_circular=result.has_cycles,
        circular_dependencies=[list(cycle) for cycle in result.circular_dependencies],
        external_sources=list(result.external_sources),
    )
    return GraphResponse(nodes=nodes, edges=edges, metadata=metadata)


def _edge_display_type(
    source: Optional[DependencyGraphNodeView],
    target: Optional[DependencyGraphNodeView],
    level: str,
) -> str:
    """Classify an edge for frontend styling and cross-pipeline badges.

    The frontend's external-connection badges key off ``cross_pipeline`` /
    ``external`` edge types, so the pipeline/flowgroup distinction is derived
    from the endpoint nodes rather than the graph's raw dependency kind.
    """
    if source is None or target is None:
        return "internal"
    if source.pipeline != target.pipeline:
        return "cross_pipeline"
    if level == "action" and source.flowgroup != target.flowgroup:
        return "cross_flowgroup"
    return "internal"


def _append_external_sources(
    graph: DependencyGraphView,
    nodes: list[GraphNode],
    edges: list[GraphEdge],
) -> None:
    """Expand per-node ``external_sources`` metadata into nodes and edges.

    Each distinct external source becomes one standalone ``external`` node
    with an ``external`` edge into every consumer. A source whose name
    collides with a real node id is skipped defensively (a duplicate id
    would corrupt the frontend graph model).
    """
    known_ids = {node.id for node in nodes}
    emitted: set[str] = set()

    for view_node in graph.nodes:
        raw_sources = view_node.metadata.get("external_sources")
        if not isinstance(raw_sources, (list, tuple)):
            continue
        for raw_source in raw_sources:
            source = str(raw_source)
            if source in known_ids:
                logger.debug(
                    f"Skipping external source {source!r}: collides with a node id"
                )
                continue
            if source not in emitted:
                emitted.add(source)
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
            edges.append(GraphEdge(source=source, target=view_node.id, type="external"))

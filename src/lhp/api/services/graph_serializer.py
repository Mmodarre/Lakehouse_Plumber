import logging
from typing import Any, Dict, List

import networkx as nx

from lhp.api.schemas.dependency import (
    GraphEdge,
    GraphMetadata,
    GraphNode,
    GraphResponse,
)
from lhp.models.dependencies import DependencyGraphs

logger = logging.getLogger(__name__)


def serialize_graph(graphs: DependencyGraphs, level: str) -> GraphResponse:
    """Serialize a NetworkX DiGraph to the frontend graph format.

    Args:
        graphs: DependencyGraphs containing action/flowgroup/pipeline graphs
        level: One of "action", "flowgroup", "pipeline"

    Returns:
        GraphResponse with nodes, edges, and metadata
    """
    graph = graphs.get_graph_by_level(level)

    nodes = _extract_nodes(graph)
    edges = _extract_edges(graph)
    metadata = _build_metadata(graph, level, nodes, edges)

    return GraphResponse(nodes=nodes, edges=edges, metadata=metadata)


def _extract_nodes(graph: nx.DiGraph) -> List[GraphNode]:
    """Extract nodes with their attributes from the graph."""
    nodes = []
    for node_id, data in graph.nodes(data=True):
        # Parse node_id: typically "pipeline.flowgroup.action" for action level
        parts = str(node_id).split(".")

        nodes.append(
            GraphNode(
                id=str(node_id),
                label=data.get("label", parts[-1] if parts else str(node_id)),
                type=data.get("type", "unknown"),
                pipeline=data.get("pipeline", parts[0] if len(parts) >= 1 else ""),
                flowgroup=data.get("flowgroup", parts[1] if len(parts) >= 2 else ""),
                stage=data.get("stage", 0),
                metadata={
                    k: v
                    for k, v in data.items()
                    if k not in ("label", "type", "pipeline", "flowgroup", "stage")
                },
            )
        )
    return nodes


def _extract_edges(graph: nx.DiGraph) -> List[GraphEdge]:
    """Extract edges with inferred edge types."""
    edges = []
    for source, target, data in graph.edges(data=True):
        edge_type = data.get("type", _infer_edge_type(str(source), str(target)))
        edges.append(
            GraphEdge(source=str(source), target=str(target), type=edge_type)
        )
    return edges


def _infer_edge_type(source: str, target: str) -> str:
    """Infer edge type from node IDs based on shared prefixes."""
    source_parts = source.split(".")
    target_parts = target.split(".")

    if len(source_parts) < 2 or len(target_parts) < 2:
        return "external"

    # Same pipeline and flowgroup → internal
    if source_parts[0] == target_parts[0] and source_parts[1] == target_parts[1]:
        return "internal"
    # Same pipeline, different flowgroup → cross_flowgroup
    elif source_parts[0] == target_parts[0]:
        return "cross_flowgroup"
    # Different pipeline → cross_pipeline
    else:
        return "cross_pipeline"


def _build_metadata(
    graph: nx.DiGraph,
    level: str,
    nodes: List[GraphNode],
    edges: List[GraphEdge],
) -> GraphMetadata:
    """Build graph metadata including cycle detection."""
    # Detect circular dependencies (capped at 20)
    cycles = []
    try:
        for cycle in nx.simple_cycles(graph):
            cycles.append([str(n) for n in cycle])
            if len(cycles) >= 20:
                break
    except nx.NetworkXError:
        pass

    # Collect external sources from graph metadata
    external_sources = list(graph.graph.get("external_sources", []))

    return GraphMetadata(
        level=level,
        total_nodes=len(nodes),
        total_edges=len(edges),
        stages=graph.graph.get("stages", 0),
        has_circular=len(cycles) > 0,
        circular_dependencies=cycles,
        external_sources=external_sources,
    )

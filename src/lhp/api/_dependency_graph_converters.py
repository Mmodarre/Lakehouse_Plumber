"""Private converters projecting networkx dependency graphs onto public DTOs.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the per-level graph projection used by
:meth:`InspectionFacade.analyze_dependencies` when a consumer opts in via
``include_graphs=True``. The projection is a flat walk over the node / edge
attribute dicts stamped by the internal graph builder — no live graph
objects reach the DTOs.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Mapping

from lhp.api.responses import (
    DependencyGraphEdgeView,
    DependencyGraphNodeView,
    DependencyGraphView,
    JSONValue,
)

if TYPE_CHECKING:
    import networkx as nx  # type: ignore[import-untyped]  # annotation-only: networkx ships no stubs / py.typed marker

# Node attributes lifted into dedicated DTO fields; every other attribute
# lands in the node's ``metadata`` mapping.
_PROJECTED_ATTRS = frozenset({"type", "pipeline", "flowgroup", "action_name"})


def _node_to_view(
    node_id: str,
    attrs: Mapping[str, Any],  # nx node-attribute dicts are untyped (JSONValue shapes)
    level: str,
) -> DependencyGraphNodeView:
    """Project one graph node's attribute dict onto the public node view.

    Action nodes carry ``type`` / ``action_name`` attributes; flowgroup and
    pipeline nodes fall back to the level name as their kind and are their
    own flowgroup / pipeline respectively.
    """
    metadata: Dict[str, JSONValue] = {
        key: value
        for key, value in attrs.items()
        if key not in _PROJECTED_ATTRS and value is not None
    }
    return DependencyGraphNodeView(
        id=node_id,
        label=str(attrs.get("action_name") or node_id),
        type=str(attrs.get("type") or level),
        pipeline=str(attrs.get("pipeline") or (node_id if level == "pipeline" else "")),
        flowgroup=str(
            attrs.get("flowgroup") or (node_id if level == "flowgroup" else "")
        ),
        metadata=metadata,
    )


def _graph_to_view(graph: "nx.DiGraph", level: str) -> DependencyGraphView:
    """Project one internal networkx graph onto a frozen level snapshot."""
    nodes = tuple(
        _node_to_view(str(node_id), attrs, level)
        for node_id, attrs in graph.nodes(data=True)
    )
    edges = tuple(
        DependencyGraphEdgeView(
            source=str(source),
            target=str(target),
            type=str(attrs.get("dependency_type") or "internal"),
        )
        for source, target, attrs in graph.edges(data=True)
    )
    return DependencyGraphView(level=level, nodes=nodes, edges=edges)

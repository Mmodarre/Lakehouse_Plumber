"""Dependency-graph and dependency-analysis response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class GraphNode(BaseModel):
    id: str
    label: str
    type: str  # "load", "transform", "write", "test"
    pipeline: str
    flowgroup: str
    stage: int = 0
    metadata: dict[str, Any] = Field(default_factory=dict)


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str  # "internal", "cross_flowgroup", "cross_pipeline", "external"


class GraphMetadata(BaseModel):
    level: str  # "action", "flowgroup", "pipeline"
    total_nodes: int
    total_edges: int
    stages: int
    has_circular: bool
    circular_dependencies: list[list[str]]
    external_sources: list[str]


class GraphResponse(BaseModel):
    """Graph serialization for frontend visualization."""

    nodes: list[GraphNode]
    edges: list[GraphEdge]
    metadata: GraphMetadata


class DependencyResponse(BaseModel):
    """Full dependency analysis result."""

    total_pipelines: int
    total_external_sources: int
    execution_stages: list[list[str]]
    circular_dependencies: list[list[str]]
    external_sources: list[str]
    pipeline_dependencies: dict[str, Any]


class ExecutionOrderResponse(BaseModel):
    stages: list[list[str]]
    total_stages: int
    flat_order: list[str]


class CircularDependencyResponse(BaseModel):
    has_circular: bool
    cycles: list[list[str]]
    total_cycles: int


class ExternalSourcesResponse(BaseModel):
    sources: list[str]
    total: int


class StalenessResponse(BaseModel):
    """Dependency-graph freshness for the serve-stale + manual-refresh model.

    ``stale`` drives the Refresh affordance in the SPA; ``fingerprint`` is the
    last persisted build's identity and ``built_at`` its ISO-8601 UTC timestamp
    (``None`` when nothing has been persisted yet).
    """

    stale: bool
    fingerprint: str
    built_at: str | None = None


class ExportResponse(BaseModel):
    """Exported dependency data in requested format."""

    format: str  # "dot", "json", "text"
    content: str  # The exported content as string


class CrossPipelineConnection(BaseModel):
    """One cross-pipeline (or external) connection for a flowgroup in the queried pipeline.

    Mirrors the frontend ``ExternalConnection`` badge model. ``direction`` is
    relative to the queried pipeline: ``"upstream"`` means the connected node
    feeds a flowgroup here, ``"downstream"`` means a flowgroup here feeds the
    connected node. ``target`` is the connected node's human-readable name (the
    other flowgroup's name, or an external-source name) and ``target_pipeline``
    is that node's owning pipeline (``""`` for a genuine external source).
    """

    direction: str  # "upstream" | "downstream"
    target: str = Field(
        description="Connected node name (flowgroup or external source)"
    )
    target_pipeline: str = Field(
        description="Owning pipeline of the connected node ('' if external)"
    )


class CrossPipelineSummary(BaseModel):
    """Compact cross-pipeline / external badge data for ONE pipeline.

    ``connections`` maps each flowgroup NAME in ``pipeline`` to its list of
    cross-pipeline / external connections. Derived server-side from the FULL
    (unscoped) flowgroup graph so the real ``target_pipeline`` survives — a
    pipeline-scoped graph erases it to an empty external node. Only flowgroups
    with at least one such connection appear as keys, so the frontend badge
    layer can consume it without transferring the whole project graph.
    """

    pipeline: str
    connections: dict[str, list[CrossPipelineConnection]] = Field(default_factory=dict)

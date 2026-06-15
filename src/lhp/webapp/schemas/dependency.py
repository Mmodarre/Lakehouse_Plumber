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


class ExportResponse(BaseModel):
    """Exported dependency data in requested format."""

    format: str  # "dot", "json", "text"
    content: str  # The exported content as string

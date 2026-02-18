from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class GraphNode(BaseModel):
    id: str
    label: str
    type: str                          # "load", "transform", "write", "test"
    pipeline: str
    flowgroup: str
    stage: int = 0
    metadata: Dict[str, Any] = {}


class GraphEdge(BaseModel):
    source: str
    target: str
    type: str                          # "internal", "cross_flowgroup", "cross_pipeline", "external"


class GraphMetadata(BaseModel):
    level: str                         # "action", "flowgroup", "pipeline"
    total_nodes: int
    total_edges: int
    stages: int
    has_circular: bool
    circular_dependencies: List[List[str]]
    external_sources: List[str]


class GraphResponse(BaseModel):
    """Graph serialization for frontend visualization."""
    nodes: List[GraphNode]
    edges: List[GraphEdge]
    metadata: GraphMetadata


class DependencyResponse(BaseModel):
    """Full dependency analysis result."""
    total_pipelines: int
    total_external_sources: int
    execution_stages: List[List[str]]
    circular_dependencies: List[List[str]]
    external_sources: List[str]
    pipeline_dependencies: Dict[str, Any]


class ExecutionOrderResponse(BaseModel):
    stages: List[List[str]]
    total_stages: int
    flat_order: List[str]


class CircularDependencyResponse(BaseModel):
    has_circular: bool
    cycles: List[List[str]]
    total_cycles: int


class ExternalSourcesResponse(BaseModel):
    sources: List[str]
    total: int


class ExportResponse(BaseModel):
    """Exported dependency data in requested format."""
    format: str                                # "dot", "json", "text"
    content: str                               # The exported content as string

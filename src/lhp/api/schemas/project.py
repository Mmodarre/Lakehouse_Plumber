from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class ResourceCounts(BaseModel):
    pipelines: int
    flowgroups: int
    presets: int
    templates: int
    environments: int


class ProjectInfoResponse(BaseModel):
    name: str
    version: str
    description: Optional[str] = None
    author: Optional[str] = None
    resource_counts: ResourceCounts


class ProjectConfigResponse(BaseModel):
    """Raw lhp.yaml content as structured JSON."""
    config: Dict[str, Any]


class ProjectStatsResponse(BaseModel):
    """Pipeline statistics and complexity metrics."""
    total_pipelines: int
    total_flowgroups: int
    total_actions: int
    actions_by_type: Dict[str, int]     # e.g. {"load": 5, "transform": 10, ...}
    pipelines: List[Dict[str, Any]]     # Per-pipeline breakdown

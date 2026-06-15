"""Project info / config / stats response schemas."""

from __future__ import annotations

from typing import Any

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
    description: str | None = None
    author: str | None = None
    resource_counts: ResourceCounts


class ProjectConfigResponse(BaseModel):
    """Raw lhp.yaml content as structured JSON."""

    config: dict[str, Any]


class ProjectStatsResponse(BaseModel):
    """Pipeline statistics and complexity metrics."""

    total_pipelines: int
    total_flowgroups: int
    total_actions: int
    actions_by_type: dict[str, int]  # e.g. {"load": 5, "transform": 10, ...}
    pipelines: list[dict[str, Any]]  # Per-pipeline breakdown

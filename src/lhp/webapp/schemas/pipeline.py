"""Pipeline list / detail / config response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class PipelineSummary(BaseModel):
    name: str
    flowgroup_count: int
    action_count: int


class PipelineListResponse(BaseModel):
    pipelines: list[PipelineSummary]
    total: int


class PipelineDetailResponse(BaseModel):
    name: str
    flowgroup_count: int
    flowgroups: list[str]  # flowgroup names
    config: dict[str, Any]  # Merged pipeline config (defaults + overrides)


class PipelineConfigResponse(BaseModel):
    pipeline: str
    config: dict[str, Any]  # serverless, edition, channel, etc.

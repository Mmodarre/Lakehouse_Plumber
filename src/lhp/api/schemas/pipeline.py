from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class PipelineSummary(BaseModel):
    name: str
    flowgroup_count: int
    action_count: int


class PipelineListResponse(BaseModel):
    pipelines: List[PipelineSummary]
    total: int


class PipelineDetailResponse(BaseModel):
    name: str
    flowgroup_count: int
    flowgroups: List[str]              # flowgroup names
    config: Dict[str, Any]             # Merged pipeline config (defaults + overrides)


class PipelineConfigResponse(BaseModel):
    pipeline: str
    config: Dict[str, Any]             # serverless, edition, channel, etc.

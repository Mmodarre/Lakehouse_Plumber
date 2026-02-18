from typing import Any, Dict, List, Optional
from pydantic import BaseModel

from lhp.models.config import FlowGroup, Action


class FlowgroupSummary(BaseModel):
    name: str
    pipeline: str
    action_count: int
    action_types: List[str]            # e.g. ["load", "transform", "write"]
    source_file: str                   # Relative path to YAML source
    presets: List[str]
    template: Optional[str] = None


class FlowgroupListResponse(BaseModel):
    flowgroups: List[FlowgroupSummary]
    total: int


class PipelineFlowgroupsResponse(BaseModel):
    """Flowgroups belonging to a specific pipeline (used by pipelines router)."""
    flowgroups: List[FlowgroupSummary]
    total: int


class FlowgroupDetailResponse(BaseModel):
    """Raw flowgroup config before template/preset/substitution processing.

    NOTE: FlowGroup is the internal Pydantic model from lhp.models.config.
    Embedding it directly couples the API schema to the internal model — any
    field change in FlowGroup becomes a breaking API change. This is an
    acceptable trade-off for Phase 1 (read-only). Phase 2 should introduce a
    dedicated FlowgroupSchema that maps from FlowGroup, decoupling the API
    contract from internal model evolution.
    """
    flowgroup: FlowGroup
    source_file: str


class ResolvedFlowgroupResponse(BaseModel):
    """Flowgroup after template expansion, preset merging, and substitution.

    Same FlowGroup coupling caveat as FlowgroupDetailResponse above.
    """
    flowgroup: FlowGroup
    environment: str
    applied_presets: List[str]
    applied_template: Optional[str] = None

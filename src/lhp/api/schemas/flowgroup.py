from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

from pydantic import BaseModel, Field

from lhp.models.config import FlowGroup, Action

if TYPE_CHECKING:
    from lhp.core.template_engine import TemplateEngine


class FlowgroupSummary(BaseModel):
    name: str
    pipeline: str
    action_count: int
    action_types: List[str]            # e.g. ["load", "transform", "write"]
    source_file: str                   # Relative path to YAML source
    presets: List[str]
    template: Optional[str] = None


def build_flowgroup_summary(
    fg: FlowGroup,
    template_engine: Optional[TemplateEngine],
    source_file: str = "",
) -> FlowgroupSummary:
    """Build a FlowgroupSummary with template-aware action counts.

    For template-based flowgroups, the inline ``fg.actions`` list is empty at
    discovery time because actions are defined in the template file.  This
    helper peeks into the template (via the already-cached TemplateEngine) to
    supplement the count and type set without running full resolution.
    """
    action_count = len(fg.actions)
    action_types: set[str] = {a.type.value for a in fg.actions}

    if fg.use_template and template_engine:
        template = template_engine.get_template(fg.use_template)
        if template:
            action_count += len(template.actions)
            for a in template.actions:
                if isinstance(a, dict):
                    t = a.get("type", "")
                else:
                    t = a.type.value if hasattr(a.type, "value") else str(a.type)
                if t:
                    action_types.add(t)

    return FlowgroupSummary(
        name=fg.flowgroup,
        pipeline=fg.pipeline,
        action_count=action_count,
        action_types=sorted(action_types),
        source_file=source_file,
        presets=fg.presets,
        template=fg.use_template,
    )


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


class RelatedFileInfo(BaseModel):
    """A file referenced by a flowgroup action."""

    path: str
    category: str  # "sql" | "python" | "schema" | "expectations"
    action_name: str
    field: str
    exists: bool


class FlowgroupRelatedFilesResponse(BaseModel):
    """Response for the related-files endpoint."""

    flowgroup: str
    source_file: RelatedFileInfo
    related_files: List[RelatedFileInfo]
    environment: str


class FlowgroupCreateRequest(BaseModel):
    """Request body for creating a flowgroup."""

    pipeline: str = Field(..., description="Pipeline name")
    flowgroup: str = Field(..., description="Flowgroup name")
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Additional flowgroup configuration",
    )


class FlowgroupUpdateRequest(BaseModel):
    """Request body for updating a flowgroup."""

    config: Dict[str, Any] = Field(
        ..., description="Full flowgroup configuration to replace"
    )


class FlowgroupMutationResponse(BaseModel):
    """Response model for flowgroup create/update/delete."""

    success: bool
    path: str
    multi_flowgroup_file: bool = False
    etag: Optional[str] = None

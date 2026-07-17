"""Flowgroup list / detail / resolved response schemas.

Composed purely from the public view DTOs in :mod:`lhp.api.views`
(:class:`FlowgroupView`, :class:`ActionView`, :class:`ProcessedFlowgroupView`).
No internal Pydantic models may reach this surface.

These field names are a public HTTP contract consumed by the React web IDE
(``FlowgroupSummary`` / ``FlowgroupDetailResponse`` / ``ResolvedFlowgroupResponse``
in the frontend ``types/api.ts``). Renaming a field is a breaking change.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from lhp.api.views import (
    ActionView,
    FlowgroupView,
    ProcessedFlowgroupView,
)
from lhp.webapp.services.related_files_extractor import RelatedFile


class FlowgroupSummary(BaseModel):
    """Lightweight flowgroup row for list / table views.

    Flattens the per-type action counts on :class:`FlowgroupView` into a
    single ``action_count`` plus a sorted ``action_types`` list — the shape
    the frontend table filters and renders.
    """

    name: str
    pipeline: str
    action_count: int
    action_types: list[str]  # e.g. ["load", "transform", "write"]
    source_file: str  # Relative path to YAML source
    presets: list[str]
    template: str | None = None

    @classmethod
    def from_view(cls, view: FlowgroupView, source_file: str = "") -> FlowgroupSummary:
        """Build a summary from a :class:`FlowgroupView`.

        ``action_count`` is the sum of the four per-type counts;
        ``action_types`` is the sorted set of types with a non-zero count.
        """
        type_counts = {
            "load": view.load_action_count,
            "transform": view.transform_action_count,
            "write": view.write_action_count,
            "test": view.test_action_count,
        }
        return cls(
            name=view.name,
            pipeline=view.pipeline,
            action_count=sum(type_counts.values()),
            action_types=sorted(t for t, n in type_counts.items() if n > 0),
            source_file=source_file,
            presets=list(view.presets),
            template=view.template,
        )


class FlowgroupListResponse(BaseModel):
    flowgroups: list[FlowgroupSummary]
    total: int

    @classmethod
    def from_views(
        cls, views: list[tuple[FlowgroupView, str]]
    ) -> FlowgroupListResponse:
        """Build a list response from ``(view, source_file)`` pairs."""
        summaries = [FlowgroupSummary.from_view(v, src) for v, src in views]
        return cls(flowgroups=summaries, total=len(summaries))


class PipelineFlowgroupsResponse(BaseModel):
    """Flowgroups belonging to a specific pipeline (used by pipelines router)."""

    flowgroups: list[FlowgroupSummary]
    total: int

    @classmethod
    def from_views(
        cls, views: list[tuple[FlowgroupView, str]]
    ) -> PipelineFlowgroupsResponse:
        """Build the per-pipeline response from ``(view, source_file)`` pairs."""
        summaries = [FlowgroupSummary.from_view(v, src) for v, src in views]
        return cls(flowgroups=summaries, total=len(summaries))


class FlowgroupActionSummary(BaseModel):
    """A single action inside a detail / resolved flowgroup payload.

    The frontend reads ``type`` / ``name`` / ``target`` off each action, so
    the public field is named ``type`` (mapped from the view's
    ``action_type``). Write-metadata fields (``write_mode`` / ``scd_type`` /
    ``target_full_name``) are populated only for write actions; the rest are
    ``None`` when not applicable.
    """

    name: str
    type: str  # "load" | "transform" | "write" | "test"
    target: str | None = None
    description: str | None = None
    transform_type: str | None = None
    test_type: str | None = None
    write_mode: str | None = None  # "standard" | "cdc" | "snapshot_cdc" | None
    scd_type: int | None = None  # 1 | 2 | None
    target_full_name: str | None = None

    @classmethod
    def from_view(cls, view: ActionView) -> FlowgroupActionSummary:
        """Build an action summary from an :class:`ActionView`."""
        return cls(
            name=view.name,
            type=view.action_type,
            target=view.target,
            description=view.description,
            transform_type=view.transform_type,
            test_type=view.test_type,
            write_mode=view.write_mode,
            scd_type=view.scd_type,
            target_full_name=view.target_full_name,
        )


class FlowgroupConfig(BaseModel):
    """Public projection of a processed flowgroup's config.

    The frontend consumes this as an opaque ``Record<string, unknown>`` and
    digs into ``actions`` for the per-action ``type`` / ``name`` / ``target``.
    Built from :class:`ProcessedFlowgroupView` so no internal model leaks.
    """

    name: str
    pipeline: str
    presets: list[str]
    template: str | None = None
    job_name: str | None = None
    actions: list[FlowgroupActionSummary]
    variables: dict[str, Any] = Field(default_factory=dict)

    @classmethod
    def from_view(cls, view: ProcessedFlowgroupView) -> FlowgroupConfig:
        """Build the config projection from a :class:`ProcessedFlowgroupView`."""
        fg = view.flowgroup
        return cls(
            name=fg.name,
            pipeline=fg.pipeline,
            presets=list(fg.presets),
            template=fg.template,
            job_name=view.job_name or fg.job_name,
            actions=[FlowgroupActionSummary.from_view(a) for a in view.actions],
            variables=dict(view.variables),
        )


class FlowgroupDetailResponse(BaseModel):
    """A flowgroup's processed config plus its source file path."""

    flowgroup: FlowgroupConfig
    source_file: str

    @classmethod
    def from_view(
        cls, view: ProcessedFlowgroupView, source_file: str = ""
    ) -> FlowgroupDetailResponse:
        """Build the detail response from a :class:`ProcessedFlowgroupView`."""
        return cls(
            flowgroup=FlowgroupConfig.from_view(view),
            source_file=source_file,
        )


class ResolvedFlowgroupResponse(BaseModel):
    """A flowgroup after template expansion, preset merging, and substitution."""

    flowgroup: FlowgroupConfig
    environment: str
    applied_presets: list[str]
    applied_template: str | None = None

    @classmethod
    def from_view(
        cls, view: ProcessedFlowgroupView, environment: str
    ) -> ResolvedFlowgroupResponse:
        """Build the resolved response from a :class:`ProcessedFlowgroupView`.

        ``applied_presets`` / ``applied_template`` are read from the embedded
        :class:`FlowgroupView` — after resolution these reflect what was
        actually merged in.
        """
        fg = view.flowgroup
        return cls(
            flowgroup=FlowgroupConfig.from_view(view),
            environment=environment,
            applied_presets=list(fg.presets),
            applied_template=fg.template,
        )


class RelatedFileInfo(BaseModel):
    """A file referenced by a flowgroup action."""

    path: str
    category: str  # "sql" | "python" | "schema" | "expectations" | "tags"
    action_name: str
    field: str
    exists: bool

    @classmethod
    def from_related_file(cls, rf: RelatedFile) -> RelatedFileInfo:
        """Build from a ``RelatedFile`` record."""
        return cls(
            path=rf.path,
            category=rf.category,
            action_name=rf.action_name,
            field=rf.field,
            exists=rf.exists,
        )


class FlowgroupRelatedFilesResponse(BaseModel):
    """Response for the related-files endpoint."""

    flowgroup: str
    source_file: RelatedFileInfo
    related_files: list[RelatedFileInfo]
    environment: str

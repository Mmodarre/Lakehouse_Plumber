"""Pipelines router for the ``lhp web`` local IDE backend.

Three read-only endpoints over the public :class:`InspectionFacade`:

- ``GET /api/pipelines`` — list every pipeline with flowgroup / action counts.
- ``GET /api/pipelines/{name}`` — pipeline detail (flowgroup names + count).
- ``GET /api/pipelines/{name}/flowgroups`` — the flowgroup summaries for one
  pipeline.

A "pipeline" is not a first-class config object in the public API — it is the
distinct set of ``pipeline`` fields across discovered flowgroups. Per-pipeline
merged config is not part of ``lhp.api``, so the ``config`` field on the detail
response is returned empty (the field stays in the HTTP contract the SPA types
against).

Per the ``webapp-uses-public-api`` import contract this module imports only
``lhp.api`` (via the webapp DI helpers) — no ``lhp`` internals.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from lhp.api import InspectionFacade
from lhp.api.views import FlowgroupView
from lhp.webapp.dependencies import get_inspection
from lhp.webapp.schemas.flowgroup import (
    FlowgroupSummary,
    PipelineFlowgroupsResponse,
)
from lhp.webapp.schemas.pipeline import (
    PipelineDetailResponse,
    PipelineListResponse,
    PipelineSummary,
)

router = APIRouter(prefix="/pipelines", tags=["pipelines"])


def _source_file(view: FlowgroupView) -> str:
    """Return the flowgroup's source YAML path as a string (empty if unknown)."""
    return str(view.file_path) if view.file_path is not None else ""


@router.get("", response_model=PipelineListResponse)
def list_pipelines(
    inspection: InspectionFacade = Depends(get_inspection),
) -> PipelineListResponse:
    """List all pipelines with flowgroup and action counts."""
    flowgroups = inspection.list_flowgroups()

    pipeline_map: dict[str, list[FlowgroupView]] = {}
    for fg in flowgroups:
        pipeline_map.setdefault(fg.pipeline, []).append(fg)

    summaries = [
        PipelineSummary(
            name=name,
            flowgroup_count=len(fgs),
            action_count=sum(
                fg.load_action_count
                + fg.transform_action_count
                + fg.write_action_count
                + fg.test_action_count
                for fg in fgs
            ),
        )
        for name, fgs in sorted(pipeline_map.items())
    ]

    return PipelineListResponse(pipelines=summaries, total=len(summaries))


@router.get("/{name}", response_model=PipelineDetailResponse)
def get_pipeline(
    name: str,
    inspection: InspectionFacade = Depends(get_inspection),
) -> PipelineDetailResponse:
    """Pipeline detail with its flowgroup list.

    ``config`` is returned empty: per-pipeline merged config is not exposed by
    the public inspection API (see the module docstring).
    """
    pipeline_fgs = inspection.list_flowgroups(pipeline_filter=name)
    if not pipeline_fgs:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    return PipelineDetailResponse(
        name=name,
        flowgroup_count=len(pipeline_fgs),
        flowgroups=[fg.name for fg in pipeline_fgs],
        config={},
    )


@router.get("/{name}/flowgroups", response_model=PipelineFlowgroupsResponse)
def get_pipeline_flowgroups(
    name: str,
    inspection: InspectionFacade = Depends(get_inspection),
) -> PipelineFlowgroupsResponse:
    """List the flowgroups belonging to a specific pipeline."""
    pipeline_fgs = inspection.list_flowgroups(pipeline_filter=name)
    if not pipeline_fgs:
        raise HTTPException(404, f"Pipeline '{name}' not found")

    summaries = [
        FlowgroupSummary.from_view(fg, _source_file(fg)) for fg in pipeline_fgs
    ]
    return PipelineFlowgroupsResponse(flowgroups=summaries, total=len(summaries))

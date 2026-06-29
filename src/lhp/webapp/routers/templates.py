"""Template read endpoints for the LHP web IDE backend.

Read-only: ``GET /api/templates`` (list) and ``GET /api/templates/{name}``
(detail incl. declared parameters). Template create/update/delete are
intentionally not ported — the local IDE edits YAML through the file-write
surface, not a dedicated template CUD API.

Both endpoints are backed by the read-only inspection facade
(:meth:`InspectionFacade.list_templates`), which projects each template to a
frozen :class:`~lhp.api.TemplateView`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import InspectionFacade, TemplateView
from lhp.webapp.dependencies import get_inspection
from lhp.webapp.schemas.template import (
    TemplateDetailResponse,
    TemplateInfoResponse,
    TemplateListDetailResponse,
    TemplateListResponse,
    TemplateSummary,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/templates", tags=["templates"])


@router.get("")
async def list_templates(
    detail: bool = Query(False, description="Include summary metadata per template"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> TemplateListResponse | TemplateListDetailResponse:
    """List all available templates.

    With ``detail=false`` (default) returns a simple list of names.
    With ``detail=true`` returns summaries with description and counts.
    """
    views = await asyncio.to_thread(inspection.list_templates)

    if not detail:
        names = [v.name for v in views]
        return TemplateListResponse(templates=names, total=len(names))

    summaries = [
        TemplateSummary(
            name=v.name,
            description=v.description,
            parameter_count=v.parameter_count,
            action_count=v.action_count,
            action_types=[],
        )
        for v in views
    ]
    return TemplateListDetailResponse(templates=summaries, total=len(summaries))


@router.get(
    "/{name}",
    response_model=TemplateDetailResponse,
    response_model_exclude_none=True,
)
async def get_template(
    name: str,
    inspection: InspectionFacade = Depends(get_inspection),
) -> TemplateDetailResponse:
    """Return a single template's metadata including its declared parameters."""
    views = await asyncio.to_thread(inspection.list_templates)
    view = next((v for v in views if v.name == name), None)
    if view is None:
        raise HTTPException(404, f"Template '{name}' not found")

    return TemplateDetailResponse(name=name, template=_view_to_info(view))


def _view_to_info(view: TemplateView) -> TemplateInfoResponse:
    """Project a frozen :class:`TemplateView` to the response schema."""
    parameters: list[dict[str, Any]] = [
        {
            "name": p.name,
            "type": p.type_,
            "required": p.required,
            "description": p.description,
            "default": p.default,
        }
        for p in view.parameters
    ]
    return TemplateInfoResponse(
        name=view.name,
        version=view.version,
        description=view.description or "",
        parameters=parameters,
        action_count=view.action_count,
    )

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException

from lhp.api.dependencies import get_template_engine
from lhp.api.schemas.template import (
    TemplateDetailResponse,
    TemplateInfoResponse,
    TemplateListResponse,
)
from lhp.core.template_engine import TemplateEngine

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/templates", tags=["templates"])


@router.get("", response_model=TemplateListResponse)
async def list_templates(
    engine: TemplateEngine = Depends(get_template_engine),
) -> TemplateListResponse:
    """#48: List all available templates."""
    templates = await asyncio.to_thread(engine.list_templates)
    return TemplateListResponse(templates=templates, total=len(templates))


@router.get("/{name}", response_model=TemplateDetailResponse)
async def get_template(
    name: str,
    engine: TemplateEngine = Depends(get_template_engine),
) -> TemplateDetailResponse:
    """#49: Get template details including parameters."""
    info = await asyncio.to_thread(engine.get_template_info, name)
    if not info:
        raise HTTPException(404, f"Template '{name}' not found")

    return TemplateDetailResponse(
        name=name,
        template=TemplateInfoResponse(**info),
    )

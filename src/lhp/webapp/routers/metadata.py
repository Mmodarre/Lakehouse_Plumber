"""Operational-metadata read endpoint for the LHP web IDE backend.

``GET /api/operational-metadata`` returns the operational-metadata columns and
presets available to the current project, resolved with the generator's REPLACE
semantics (project-declared columns suppress the five built-ins; otherwise the
built-ins) via the public inspection facade
(:meth:`InspectionFacade.get_operational_metadata`). The web IDE offers exactly
the columns generation would apply.

This is a read-only endpoint; operational-metadata editing lives in the
``lhp.yaml`` config surface, not here.
"""

from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException

from lhp.api import InspectionFacade
from lhp.webapp.dependencies import get_inspection
from lhp.webapp.schemas.metadata import (
    OperationalMetadataColumnSummary,
    OperationalMetadataPresetSummary,
    OperationalMetadataResponse,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/operational-metadata", tags=["operational-metadata"])


@router.get("", response_model=OperationalMetadataResponse)
async def get_operational_metadata(
    inspection: InspectionFacade = Depends(get_inspection),
) -> OperationalMetadataResponse:
    """Operational-metadata columns/presets available for the current project."""
    try:
        view = await asyncio.to_thread(inspection.get_operational_metadata)
    except Exception as e:
        logger.exception("Failed to resolve operational metadata")
        raise HTTPException(500, "Failed to resolve operational metadata") from e

    return OperationalMetadataResponse(
        columns=[
            OperationalMetadataColumnSummary(
                name=col.name,
                expression=col.expression,
                description=col.description,
                applies_to=list(col.applies_to),
                source=col.source,
            )
            for col in view.columns
        ],
        presets=[
            OperationalMetadataPresetSummary(
                name=preset.name,
                columns=list(preset.columns),
                description=preset.description,
            )
            for preset in view.presets
        ],
    )

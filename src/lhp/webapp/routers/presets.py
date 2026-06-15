"""Preset read endpoints for the LHP web IDE backend.

Read-only: ``GET /api/presets`` (list) and ``GET /api/presets/{name}``
(raw file content). Preset create/update/delete are intentionally not
ported — the local IDE edits YAML through the file-write surface, not a
dedicated preset CUD API.

The detail endpoint returns the **raw** preset YAML only. The ``resolved``
inheritance-merged chain is deferred to a future revision (the inspection
facade does not expose a preset-resolution surface).
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import InspectionFacade
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.preset import (
    PresetDetailResponse,
    PresetListDetailResponse,
    PresetListResponse,
    PresetSummary,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/presets", tags=["presets"])


@router.get("")
async def list_presets(
    detail: bool = Query(False, description="Include summary metadata per preset"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> PresetListResponse | PresetListDetailResponse:
    """List all available presets.

    With ``detail=false`` (default) returns a simple list of names.
    With ``detail=true`` returns summaries with description and extends.
    """
    views = await asyncio.to_thread(inspection.list_presets)

    if not detail:
        names = [v.name for v in views]
        return PresetListResponse(presets=names, total=len(names))

    summaries = [
        PresetSummary(name=v.name, description=v.description, extends=v.extends)
        for v in views
    ]
    return PresetListDetailResponse(presets=summaries, total=len(summaries))


@router.get(
    "/{name}",
    response_model=PresetDetailResponse,
    response_model_exclude_none=True,
)
async def get_preset(
    name: str,
    project_root: Path = Depends(get_project_root),
) -> PresetDetailResponse:
    """Return the raw YAML content of a single preset.

    The resolved inheritance chain is deferred — see module docstring.
    """
    file_path = project_root / "presets" / f"{name}.yaml"
    if not file_path.exists():
        raise HTTPException(404, f"Preset '{name}' not found")

    raw = await asyncio.to_thread(_load_yaml, file_path)
    return PresetDetailResponse(name=name, raw=raw, resolved=None)


def _load_yaml(file_path: Path) -> dict[str, Any]:
    """Load a YAML file as a dict (empty dict for an empty file)."""
    data = yaml.safe_load(file_path.read_text()) or {}
    if not isinstance(data, dict):
        return {}
    return data

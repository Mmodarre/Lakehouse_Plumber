import asyncio
import logging

from fastapi import APIRouter, Depends, HTTPException

from lhp.api.dependencies import get_preset_manager
from lhp.api.schemas.preset import PresetDetailResponse, PresetListResponse
from lhp.presets.preset_manager import PresetManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/presets", tags=["presets"])


@router.get("", response_model=PresetListResponse)
async def list_presets(
    preset_mgr: PresetManager = Depends(get_preset_manager),
) -> PresetListResponse:
    """#43: List all available presets."""
    presets = await asyncio.to_thread(preset_mgr.list_presets)
    return PresetListResponse(presets=presets, total=len(presets))


@router.get("/{name}", response_model=PresetDetailResponse)
async def get_preset(
    name: str,
    preset_mgr: PresetManager = Depends(get_preset_manager),
) -> PresetDetailResponse:
    """#44: Get preset details including inheritance chain resolution."""
    preset = await asyncio.to_thread(preset_mgr.get_preset, name)
    if preset is None:
        raise HTTPException(404, f"Preset '{name}' not found")

    # Resolve full inheritance chain
    resolved = await asyncio.to_thread(preset_mgr.resolve_preset_chain, [name])

    return PresetDetailResponse(
        name=name,
        raw=preset.model_dump(),
        resolved=resolved,
    )

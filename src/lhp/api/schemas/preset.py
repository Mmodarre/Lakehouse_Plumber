from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class PresetSummary(BaseModel):
    """Lightweight preset summary for list views."""
    name: str
    description: Optional[str] = None
    extends: Optional[str] = None


class PresetListResponse(BaseModel):
    """List of available preset names."""
    presets: List[str]
    total: int


class PresetListDetailResponse(BaseModel):
    """List of presets with summary metadata."""
    presets: List[PresetSummary]
    total: int


class PresetDetailResponse(BaseModel):
    """Preset with raw definition and resolved (inheritance-merged) config."""
    name: str
    raw: Dict[str, Any]                # Preset model serialized (name, version, extends, defaults)
    resolved: Dict[str, Any]           # Result of resolve_preset_chain — fully merged config

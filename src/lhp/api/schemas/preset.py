from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class PresetListResponse(BaseModel):
    """List of available preset names."""
    presets: List[str]
    total: int


class PresetDetailResponse(BaseModel):
    """Preset with raw definition and resolved (inheritance-merged) config."""
    name: str
    raw: Dict[str, Any]                # Preset model serialized (name, version, extends, defaults)
    resolved: Dict[str, Any]           # Result of resolve_preset_chain — fully merged config

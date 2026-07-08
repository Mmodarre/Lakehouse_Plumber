"""Preset list / detail response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class PresetSummary(BaseModel):
    """Lightweight preset summary for list views."""

    name: str
    description: str | None = None
    extends: str | None = None


class PresetListResponse(BaseModel):
    """List of available preset names."""

    presets: list[str]
    total: int


class PresetListDetailResponse(BaseModel):
    """List of presets with summary metadata."""

    presets: list[PresetSummary]
    total: int


class PresetDetailResponse(BaseModel):
    """Preset detail — raw YAML content plus the resolved inheritance merge.

    ``resolved`` carries the inheritance-merged ``defaults`` payload from
    ``InspectionFacade.resolve_preset`` (base→leaf deep merge; more-derived
    values win per key, ``operational_metadata`` lists concatenate with
    dedup). ``chain`` lists the preset names base→leaf — the requested
    preset is last.
    """

    name: str
    raw: dict[str, Any]  # Raw preset YAML (name, version, extends, defaults)
    resolved: dict[str, Any] | None = None  # Inheritance-merged defaults
    chain: list[str] = []  # Preset names base→leaf

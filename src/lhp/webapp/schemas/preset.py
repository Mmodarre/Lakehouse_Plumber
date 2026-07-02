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
    """Preset detail — raw YAML content of the preset file.

    ``resolved`` (the inheritance-merged chain) is deferred: the read-only
    inspection facade exposes no preset-resolution surface, so the web IDE
    returns only the raw file content. The field is retained as optional so
    a future revision can populate it without a schema break.
    """

    name: str
    raw: dict[str, Any]  # Raw preset YAML (name, version, extends, defaults)
    resolved: dict[str, Any] | None = None  # Deferred: inheritance-merged config

"""Operational-metadata response schemas for the web IDE backend."""

from __future__ import annotations

from pydantic import BaseModel, Field


class OperationalMetadataColumnSummary(BaseModel):
    """One operational-metadata column available to the project.

    ``source`` is ``"builtin"`` (one of the five default columns) or
    ``"project"`` (declared in ``lhp.yaml``). ``expression`` carries any
    substitution tokens verbatim.
    """

    name: str
    expression: str
    description: str | None = None
    applies_to: list[str]
    source: str


class OperationalMetadataPresetSummary(BaseModel):
    """One named operational-metadata preset declared in ``lhp.yaml``."""

    name: str
    columns: list[str]
    description: str | None = None


class OperationalMetadataResponse(BaseModel):
    """Operational-metadata columns and presets available for the project.

    ``columns`` follows the generator's REPLACE semantics: project-declared
    columns suppress the five built-ins wholesale, otherwise the built-ins
    are returned. ``presets`` is empty when the project declares none.
    """

    columns: list[OperationalMetadataColumnSummary]
    presets: list[OperationalMetadataPresetSummary] = Field(default_factory=list)

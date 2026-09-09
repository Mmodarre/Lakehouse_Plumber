"""Authoritative saved-settings preview response for the configuration editor."""

from typing import Any, Literal

from pydantic import BaseModel, Field


class ConfigurationPreviewResponse(BaseModel):
    path: str
    kind: Literal["pipeline", "job"]
    env: str
    target: str
    targets: list[str]
    values: dict[str, Any] = Field(
        description="Resolved saved settings from LHP's production resolvers"
    )
    tiers: list[str]
    warnings: list[str]
    source: Literal["saved"]

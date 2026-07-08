"""Blueprint list response schemas."""

from __future__ import annotations

from pydantic import BaseModel, Field


class BlueprintInstanceSummary(BaseModel):
    """One blueprint instance file and the flowgroups it expands to."""

    instance_file_path: str
    flowgroup_count: int
    pipelines: list[str]


class BlueprintSummary(BaseModel):
    """Summary of a single blueprint discovered in the project.

    ``instances`` is populated only when the caller requests
    ``include_instances=true``; the default listing leaves it empty.
    """

    name: str
    version: str
    description: str | None = None
    parameter_count: int
    flowgroup_count: int
    instance_count: int
    instances: list[BlueprintInstanceSummary] = Field(default_factory=list)


class BlueprintListResponse(BaseModel):
    """List of blueprints with summary metadata."""

    blueprints: list[BlueprintSummary]
    total: int

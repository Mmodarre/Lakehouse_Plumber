"""Project info / config / stats / init / environment response schemas."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class ResourceCounts(BaseModel):
    pipelines: int
    flowgroups: int
    presets: int
    templates: int
    environments: int


class ProjectInfoResponse(BaseModel):
    name: str
    version: str
    description: str | None = None
    author: str | None = None
    resource_counts: ResourceCounts


class ProjectConfigResponse(BaseModel):
    """Raw lhp.yaml content as structured JSON."""

    config: dict[str, Any]


class ProjectStatsResponse(BaseModel):
    """Pipeline statistics and complexity metrics."""

    total_pipelines: int
    total_flowgroups: int
    total_actions: int
    actions_by_type: dict[str, int]  # e.g. {"load": 5, "transform": 10, ...}
    pipelines: list[dict[str, Any]]  # Per-pipeline breakdown


class InitProjectRequest(BaseModel):
    """Body for ``POST /api/project/init``.

    ``project_name`` defaults to the project-root directory name when omitted
    (the public bootstrap's own default).
    """

    project_name: str | None = None
    bundle: bool = True


class InitProjectResponse(BaseModel):
    """Outcome of scaffolding via ``POST /api/project/init``.

    Mirrors the public :class:`~lhp.api.InitProjectResult`: a scaffolding
    failure surfaces ``success=False`` plus ``error_message`` /
    ``error_code`` rather than an HTTP error. Paths are reported relative
    to the project root.
    """

    success: bool
    created_files: list[str]
    created_dirs: list[str]
    bundle_enabled: bool
    error_message: str | None = None
    error_code: str | None = None


class SecretReferenceSummary(BaseModel):
    """One ``${secret:scope/key}`` reference observed in an environment."""

    scope: str
    key: str


class SubstitutionResolvedResponse(BaseModel):
    """Resolved substitution context for one environment.

    ``tokens`` is the flat string projection; ``raw_mappings`` preserves
    nested dict/list values as-is. ``secret_references`` is populated only
    after substitution has actually processed some payload, so a freshly
    resolved view typically reports an empty list.
    """

    env: str
    tokens: dict[str, str]
    raw_mappings: dict[str, Any]
    default_secret_scope: str | None = None
    secret_references: list[SecretReferenceSummary] = Field(default_factory=list)

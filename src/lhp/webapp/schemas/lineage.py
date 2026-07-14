"""Dataset lineage response schema for the ``GET /api/lineage`` endpoint.

Mirrors the public :class:`lhp.api.DatasetView` DTO (and its nested
:class:`~lhp.api.LineageNodeView` / :class:`~lhp.api.LineageEdgeView` /
:class:`~lhp.api.DatasetConsumerView`) as plain pydantic models, plus two
webapp-only fields:

* ``warnings`` — the project-wide index warnings passed through VERBATIM
  (per-flowgroup env-resolution failures and action-name positional-fallback
  notices), followed by any multi-writer collision warning for the looked-up
  FQN. v1 deliberately passes the project-wide warnings through as-is rather
  than scoping them to the requested dataset.
* ``stale`` — the serve-stale ``app.state.graph_stale`` flag, mirroring
  ``GET /api/dependencies/staleness``: ``True`` after a graph-relevant edit
  until an explicit ``POST /api/dependencies/refresh``.

Kept as pure pydantic (no ``lhp.api`` import) like ``schemas/dependency.py``;
the router maps the DTO onto these models.
"""

from __future__ import annotations

from pydantic import BaseModel, Field


class LineageNode(BaseModel):
    id: str
    kind: str  # "load" | "transform" | "write" | "test" | "dataset" | "external"
    label: str
    pipeline: str = ""
    flowgroup: str = ""
    dataset_fqn: str = ""


class LineageEdge(BaseModel):
    source: str
    target: str


class DatasetConsumer(BaseModel):
    dataset_fqn: str
    pipeline: str
    flowgroup: str
    action_name: str


class DatasetLineageResponse(BaseModel):
    """Lineage of one produced dataset (table or sink) with its consumers."""

    fqn: str
    kind: str  # "table" | "sink"
    pipeline: str
    flowgroup: str
    action_name: str
    write_mode: str
    scd_type: int | None = None
    source_file: str
    nodes: list[LineageNode]
    edges: list[LineageEdge]
    consumers: list[DatasetConsumer]
    warnings: list[str] = Field(default_factory=list)
    stale: bool = False

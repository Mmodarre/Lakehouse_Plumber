"""Run-history response schemas for the ``/api/runs`` endpoints.

Webapp-owned HTTP contract models (NOT ``lhp.api`` DTOs): they mirror the
rows persisted by :mod:`lhp.webapp.services.run_history`. ``summary`` is the
parsed ``summary_json`` written at run completion; ``events`` are the raw
NDJSON frame dicts, present only when the detail endpoint is asked for them.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class RunSummary(BaseModel):
    run_id: str
    kind: str  # "validate" | "generate"
    env: str
    pipeline: Optional[str]
    status: str  # "running" | "completed" | "failed"
    started_at: str  # ISO-8601 UTC
    finished_at: Optional[str]
    summary: Optional[dict[str, Any]]  # parsed summary_json (success flag, counts)


class RunIssue(BaseModel):
    severity: str
    code: Optional[str]
    message: str
    file: Optional[str]
    line: Optional[int]


class RunDetail(RunSummary):
    issues: list[RunIssue]
    events: Optional[list[dict[str, Any]]]  # raw frames; only with include_events


class RunListResponse(BaseModel):
    runs: list[RunSummary]
    total: int

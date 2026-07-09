"""Run-history router for the ``lhp web`` local IDE backend.

Two read-only endpoints over the SQLite run history recorded by
:mod:`lhp.webapp.services.run_recorder` around the validate / generate
streams:

- ``GET /api/runs?limit=N`` — newest-first run summaries (``limit`` clamped
  to 1..200, default 50).
- ``GET /api/runs/{run_id}?include_events=bool`` — one run's summary +
  extracted issues, optionally with the full recorded frame list.

Handlers are sync ``def`` (FastAPI threadpools them) because the underlying
:mod:`run_history` layer is synchronous sqlite3.

The database is only initialized for a real project (the lifespan skips DB
setup in ``no_project`` state), so both endpoints 409 when no project is
loaded rather than materializing ``.lhp/`` in a non-project directory.

ROUTER CONVENTION: routes carry their sub-path under ``/runs``; the app
mounts this router with ``prefix="/api"``.
"""

from __future__ import annotations

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from lhp.webapp.dependencies import get_project_root
from lhp.webapp.routers._guards import assert_project_loaded
from lhp.webapp.schemas.runs import RunDetail, RunListResponse, RunSummary
from lhp.webapp.services import run_history

router = APIRouter(prefix="/runs", tags=["runs"])

_MIN_LIMIT = 1
_MAX_LIMIT = 200


@router.get("", response_model=RunListResponse)
def list_runs(
    request: Request,
    limit: int = Query(default=50, description="Max runs returned (clamped 1..200)."),
    project_root: Path = Depends(get_project_root),
) -> RunListResponse:
    """List recorded runs, newest first."""
    assert_project_loaded(request, "run history is unavailable")
    clamped = max(_MIN_LIMIT, min(_MAX_LIMIT, limit))
    rows = run_history.list_runs(project_root, limit=clamped)
    runs = [RunSummary(**row) for row in rows]
    return RunListResponse(runs=runs, total=len(runs))


@router.get("/{run_id}", response_model=RunDetail)
def get_run(
    run_id: str,
    request: Request,
    include_events: bool = Query(
        default=False, description="Include the recorded NDJSON frames."
    ),
    project_root: Path = Depends(get_project_root),
) -> RunDetail:
    """Return one run's summary + issues (+ frames when ``include_events``)."""
    assert_project_loaded(request, "run history is unavailable")
    detail = run_history.get_run(project_root, run_id, include_events=include_events)
    if detail is None:
        raise HTTPException(404, f"Run '{run_id}' not found")
    return RunDetail(**detail)

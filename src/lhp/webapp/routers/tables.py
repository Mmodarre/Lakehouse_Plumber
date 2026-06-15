"""Read-only table-catalog endpoint for the LHP local web IDE backend.

``GET /api/tables`` flattens every *write* action across the project's
flowgroups into a table-centric view — one row per write target (streaming
table, materialized view, or sink).

Write metadata is read as flat fields off the public
:class:`~lhp.api.views.ActionView` (``write_mode`` / ``scd_type`` /
``target_full_name``), populated by the inspection converter — this router
does not reach into internal domain models.

- ``target_full_name`` is the canonical ``catalog.schema.table`` (falling
  back to ``database.table``).
- ``target_type`` collapses to ``"sink"`` (detected by the ``sink:`` prefix
  the converter writes) vs ``"streaming_table"`` for every table target.
  The public :class:`ActionView` does not carry the streaming-table /
  materialized-view distinction, so it reports the streaming-table default
  for all tables.

Per the ``webapp-uses-public-api`` import contract, this module imports only
:mod:`lhp.api` from the ``lhp`` package, plus the webapp's own DI / schema
modules.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import InspectionFacade
from lhp.api.views import ActionView
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.table import (
    TableListResponse,
    TableSummary,
    build_table_summary,
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/tables", tags=["tables"])


def _relative_source(file_path: Optional[Path], project_root: Path) -> str:
    """Return ``file_path`` relative to ``project_root``, or ``""`` if unknown."""
    if file_path is None:
        return ""
    try:
        return str(file_path.relative_to(project_root))
    except ValueError:
        return ""


def _table_summary_for_write(
    action: ActionView,
    *,
    pipeline: str,
    flowgroup: str,
    source_file: str,
) -> TableSummary:
    """Build a :class:`TableSummary` from a public write :class:`ActionView`.

    ``target_type`` is derived from the ``sink:`` prefix the converter writes
    for sink targets; all other targets report the ``"streaming_table"``
    default (the public surface does not carry the streaming-table /
    materialized-view distinction).
    """
    full_name = action.target_full_name or action.name
    target_type = "sink" if full_name.startswith("sink:") else "streaming_table"
    return build_table_summary(
        full_name=full_name,
        target_type=target_type,
        pipeline=pipeline,
        flowgroup=flowgroup,
        write_mode=action.write_mode,
        scd_type=action.scd_type,
        source_file=source_file,
    )


@router.get("", response_model=TableListResponse)
def list_tables(
    env: str = Query(
        ..., description="Environment for substitution resolution (required)"
    ),
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> TableListResponse:
    """List all write targets (tables/sinks) across the project.

    Enumerates flowgroups (optionally filtered by ``pipeline``), processes each
    through the inspection facade for ``env`` to expand templates, merge
    presets, and resolve substitutions, then flattens every write action into a
    :class:`TableSummary` row. Per-flowgroup resolution failures are collected
    on ``warnings`` rather than failing the whole request.
    """
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        raise HTTPException(
            status_code=400,
            detail=f"Environment '{env}' not found (no substitutions/{env}.yaml)",
        )

    views = inspection.list_flowgroups(pipeline_filter=pipeline)

    tables: list[TableSummary] = []
    warnings: list[str] = []

    for view in views:
        source_file = _relative_source(view.file_path, project_root)
        try:
            processed = inspection.process_flowgroup(view.name, env=env)
        except Exception as exc:
            msg = f"Failed to resolve flowgroup '{view.name}': {exc}"
            logger.warning(msg)
            warnings.append(msg)
            continue

        for action in processed.actions:
            if action.action_type != "write":
                continue
            tables.append(
                _table_summary_for_write(
                    action,
                    pipeline=view.pipeline,
                    flowgroup=view.name,
                    source_file=source_file,
                )
            )

    return TableListResponse(tables=tables, total=len(tables), warnings=warnings)

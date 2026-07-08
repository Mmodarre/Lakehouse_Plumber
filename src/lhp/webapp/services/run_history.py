"""Synchronous CRUD over the webapp run-history tables.

Thin data-access functions over :mod:`lhp.webapp.services.sqlite_store` for
the ``runs`` / ``run_events`` / ``run_issues`` tables. Everything here is
synchronous and opens one connection per call; async callers (the run
recorder, the runs router's threadpooled handlers) bridge via
``asyncio.to_thread`` or FastAPI's sync-handler threadpool.

Row shapes returned to callers are plain dicts (``summary_json`` is parsed
into a ``summary`` dict; event ``frame_json`` is parsed back into the frame
dict) so the router can map them straight onto its Pydantic response models.
"""

from __future__ import annotations

import json
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional, Sequence

from lhp.webapp.services.sqlite_store import connect, utc_now_iso

#: Ordering shared by ``list_runs`` and ``prune``: newest first, with rowid as
#: the tie-breaker for runs created within the same timestamp resolution.
_NEWEST_FIRST = "ORDER BY started_at DESC, rowid DESC"


@dataclass(frozen=True)
class IssueRecord:
    """One extracted run issue, ready for insertion into ``run_issues``."""

    severity: str
    code: Optional[str]
    message: str
    file: Optional[str] = None
    line: Optional[int] = None


def create_run(
    project_root: Path,
    run_id: str,
    kind: str,
    env: str,
    pipeline: Optional[str] = None,
) -> None:
    """Insert a new run row with status ``running`` and ``started_at`` = now."""
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "INSERT INTO runs (run_id, kind, env, pipeline, status, started_at) "
            "VALUES (?, ?, ?, ?, 'running', ?)",
            (run_id, kind, env, pipeline, utc_now_iso()),
        )


def append_events(
    project_root: Path,
    run_id: str,
    events: Sequence[tuple[int, str]],
) -> None:
    """Batch-insert ``(seq, frame_json)`` pairs for ``run_id``."""
    if not events:
        return
    with closing(connect(project_root)) as conn, conn:
        conn.executemany(
            "INSERT INTO run_events (run_id, seq, frame_json) VALUES (?, ?, ?)",
            [(run_id, seq, frame_json) for seq, frame_json in events],
        )


def complete_run(
    project_root: Path,
    run_id: str,
    status: str,
    summary_json: Optional[str] = None,
) -> None:
    """Set the terminal ``status`` / ``finished_at`` / ``summary_json`` for a run."""
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE runs SET status = ?, finished_at = ?, summary_json = ? "
            "WHERE run_id = ?",
            (status, utc_now_iso(), summary_json, run_id),
        )


def add_issues(
    project_root: Path,
    run_id: str,
    issues: Sequence[IssueRecord],
) -> None:
    """Insert the extracted issues for a run."""
    if not issues:
        return
    with closing(connect(project_root)) as conn, conn:
        conn.executemany(
            "INSERT INTO run_issues (run_id, severity, code, message, file, line) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [(run_id, i.severity, i.code, i.message, i.file, i.line) for i in issues],
        )


def _summary_row(row: Any) -> dict[str, Any]:
    """Map a ``runs`` row onto the summary dict shape the router consumes.

    ``row`` is a ``sqlite3.Row``; typed ``Any`` because sqlite3's stubs give
    ``Row.__getitem__`` an ``Any`` return anyway.
    """
    raw_summary = row["summary_json"]
    return {
        "run_id": row["run_id"],
        "kind": row["kind"],
        "env": row["env"],
        "pipeline": row["pipeline"],
        "status": row["status"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "summary": json.loads(raw_summary) if raw_summary else None,
    }


def list_runs(project_root: Path, limit: int = 50) -> list[dict[str, Any]]:
    """Return up to ``limit`` run summaries, newest first."""
    with closing(connect(project_root)) as conn:
        rows = conn.execute(
            f"SELECT * FROM runs {_NEWEST_FIRST} LIMIT ?", (limit,)
        ).fetchall()
    return [_summary_row(row) for row in rows]


def get_run(
    project_root: Path,
    run_id: str,
    include_events: bool = False,
) -> Optional[dict[str, Any]]:
    """Return one run's summary + issues (+ ordered frames), or ``None`` if unknown."""
    with closing(connect(project_root)) as conn:
        row = conn.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        detail = _summary_row(row)
        issue_rows = conn.execute(
            "SELECT severity, code, message, file, line FROM run_issues "
            "WHERE run_id = ? ORDER BY rowid",
            (run_id,),
        ).fetchall()
        detail["issues"] = [dict(issue) for issue in issue_rows]
        if include_events:
            event_rows = conn.execute(
                "SELECT frame_json FROM run_events WHERE run_id = ? ORDER BY seq",
                (run_id,),
            ).fetchall()
            detail["events"] = [json.loads(event["frame_json"]) for event in event_rows]
        else:
            detail["events"] = None
    return detail


def prune(
    project_root: Path,
    keep_last: int = 100,
    max_age_days: int = 30,
) -> int:
    """Delete runs beyond the newest ``keep_last`` OR older than ``max_age_days``.

    ``run_events`` / ``run_issues`` rows for the deleted runs are removed in
    the same transaction (manual cascade — the schema declares no FK actions).
    Returns the number of runs deleted.
    """
    # ISO-8601 UTC strings compare chronologically as strings, so the age
    # cutoff is a plain lexicographic comparison against started_at.
    cutoff = (datetime.now(timezone.utc) - timedelta(days=max_age_days)).isoformat()
    with closing(connect(project_root)) as conn, conn:
        doomed = conn.execute(
            "SELECT run_id FROM runs WHERE started_at < ? "
            f"OR run_id NOT IN (SELECT run_id FROM runs {_NEWEST_FIRST} LIMIT ?)",
            (cutoff, keep_last),
        ).fetchall()
        run_ids = [row["run_id"] for row in doomed]
        for run_id in run_ids:
            conn.execute("DELETE FROM run_events WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM run_issues WHERE run_id = ?", (run_id,))
            conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
    return len(run_ids)

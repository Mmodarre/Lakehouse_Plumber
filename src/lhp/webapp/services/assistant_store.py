"""Synchronous CRUD over the webapp assistant tables.

Thin data-access functions over :mod:`lhp.webapp.services.sqlite_store` for
the ``assistant_sessions`` / ``assistant_config`` tables. Everything here is
synchronous and opens one connection per call; async callers bridge via
``asyncio.to_thread`` or FastAPI's sync-handler threadpool.

Sessions track omnigent conversations (``session_id`` = ``conv_...``,
``agent_id`` = ``ag_...``) under a single-active invariant: at most one row
has ``status = 'active'``. :func:`insert_session` archives the previous
active session in the same transaction that activates the new one.

Config rows are a JSON key/value store (keys ``executor`` / ``agent``).
No key material is EVER persisted here: in ``api_key`` executor mode the
``executor`` config dict stores the NAME of the environment variable that
holds the key, never the key value itself.
"""

from __future__ import annotations

import json
import logging
from contextlib import closing
from pathlib import Path
from typing import Any, Optional

from lhp.webapp.services.sqlite_store import connect, utc_now_iso

logger = logging.getLogger(__name__)

#: Ordering used by ``list_sessions``: most recently used first, with rowid
#: as the tie-breaker for sessions touched within the same timestamp
#: resolution.
_MOST_RECENT_FIRST = "ORDER BY last_used_at DESC, rowid DESC"


def get_config(project_root: Path, key: str) -> Optional[dict[str, Any]]:
    """Return the parsed config value stored under ``key``, or ``None`` if unset."""
    with closing(connect(project_root)) as conn:
        row = conn.execute(
            "SELECT value_json FROM assistant_config WHERE key = ?", (key,)
        ).fetchone()
    if row is None:
        return None
    value: dict[str, Any] = json.loads(row["value_json"])
    return value


def put_config(project_root: Path, key: str, value: dict[str, Any]) -> None:
    """Upsert ``value`` as JSON under ``key``, stamping ``updated_at`` = now."""
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "INSERT INTO assistant_config (key, value_json, updated_at) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET "
            "value_json = excluded.value_json, updated_at = excluded.updated_at",
            (key, json.dumps(value), utc_now_iso()),
        )


def get_active_session(project_root: Path) -> Optional[dict[str, Any]]:
    """Return the single ``status = 'active'`` session row, or ``None``."""
    with closing(connect(project_root)) as conn:
        row = conn.execute(
            "SELECT * FROM assistant_sessions WHERE status = 'active'"
        ).fetchone()
    return dict(row) if row is not None else None


def insert_session(
    project_root: Path,
    session_id: str,
    agent_id: str,
    host_id: str,
    agent_bundle_hash: str,
    title: Optional[str] = None,
) -> None:
    """Insert a new ``active`` session, archiving any previously active one.

    Demotion and insertion happen in one transaction so the single-active
    invariant holds even if the process dies between the two statements.
    """
    now = utc_now_iso()
    with closing(connect(project_root)) as conn, conn:
        demoted = conn.execute(
            "UPDATE assistant_sessions SET status = 'archived' WHERE status = 'active'"
        ).rowcount
        conn.execute(
            "INSERT INTO assistant_sessions "
            "(session_id, agent_id, host_id, agent_bundle_hash, title, status, "
            "created_at, last_used_at) "
            "VALUES (?, ?, ?, ?, ?, 'active', ?, ?)",
            (session_id, agent_id, host_id, agent_bundle_hash, title, now, now),
        )
    if demoted:
        logger.debug(
            f"Archived previously active assistant session before "
            f"activating {session_id}"
        )


def archive_active(project_root: Path) -> int:
    """Demote the active session (if any) to ``archived``; return rows changed."""
    with closing(connect(project_root)) as conn, conn:
        return conn.execute(
            "UPDATE assistant_sessions SET status = 'archived' WHERE status = 'active'"
        ).rowcount


def mark_stale(project_root: Path, session_id: str) -> None:
    """Mark ``session_id`` as ``stale`` (agent-bundle drift or a dead host)."""
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE assistant_sessions SET status = 'stale' WHERE session_id = ?",
            (session_id,),
        )


def touch_session(project_root: Path, session_id: str) -> None:
    """Bump ``last_used_at`` to now for ``session_id``."""
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE assistant_sessions SET last_used_at = ? WHERE session_id = ?",
            (utc_now_iso(), session_id),
        )


def list_sessions(project_root: Path, limit: int = 50) -> list[dict[str, Any]]:
    """Return up to ``limit`` sessions, most recently used first."""
    with closing(connect(project_root)) as conn:
        rows = conn.execute(
            f"SELECT * FROM assistant_sessions {_MOST_RECENT_FIRST} LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]

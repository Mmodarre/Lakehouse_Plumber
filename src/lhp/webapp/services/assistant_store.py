"""Synchronous CRUD over the webapp assistant tables.

Thin data-access functions over :mod:`lhp.webapp.services.sqlite_store` for
the ``assistant_sessions`` / ``assistant_config`` tables. Everything here is
synchronous and opens one connection per call; async callers bridge via
``asyncio.to_thread`` or FastAPI's sync-handler threadpool.

Sessions track omnigent conversations (``session_id`` = ``conv_...``,
``agent_id`` = ``ag_...``) or Claude SDK sessions (``session_id`` =
``claude_...``, ``provider`` column discriminates) under a single-active
invariant: at most one row has ``status = 'active'`` across BOTH providers.
:func:`insert_session` / :func:`insert_claude_session` archive the previous
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


def insert_claude_session(
    project_root: Path,
    session_id: str,
    agent_bundle_hash: str,
    title: Optional[str] = None,
) -> None:
    """Insert a new ``active`` Claude SDK session, archiving any active one.

    Same single-transaction demote+insert invariant as :func:`insert_session`.
    ``agent_id`` / ``host_id`` are Omnigent concepts with no Claude equivalent;
    the columns are NOT NULL, so Claude rows write the sentinels ``''`` /
    ``'local'``.
    """
    now = utc_now_iso()
    with closing(connect(project_root)) as conn, conn:
        demoted = conn.execute(
            "UPDATE assistant_sessions SET status = 'archived' WHERE status = 'active'"
        ).rowcount
        conn.execute(
            "INSERT INTO assistant_sessions "
            "(session_id, agent_id, host_id, agent_bundle_hash, title, status, "
            "created_at, last_used_at, provider) "
            "VALUES (?, '', 'local', ?, ?, 'active', ?, ?, 'claude_sdk')",
            (session_id, agent_bundle_hash, title, now, now),
        )
    if demoted:
        logger.debug(
            f"Archived previously active assistant session before "
            f"activating {session_id}"
        )


def set_runtime_session_id(
    project_root: Path, session_id: str, runtime_session_id: Optional[str]
) -> None:
    """Store the SDK resume handle for ``session_id`` (``None`` clears it).

    Refreshed after every Claude turn — the SDK may re-mint its own session id
    on resume. Cleared (``None``) when a resume fails so the next turn starts
    a fresh SDK session under the same LHP ``session_id``.
    """
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE assistant_sessions SET runtime_session_id = ? WHERE session_id = ?",
            (runtime_session_id, session_id),
        )


def insert_item(project_root: Path, session_id: str, item: dict[str, Any]) -> int:
    """Append ``item`` to the session transcript; return its assigned ``seq``.

    ``seq`` is ``MAX(seq) + 1`` computed inside the insert's transaction; the
    turn engine is the single writer (under its turn lock), so this cannot
    race with itself.
    """
    with closing(connect(project_root)) as conn, conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM assistant_items "
            "WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        seq = int(row[0])
        conn.execute(
            "INSERT INTO assistant_items (session_id, seq, item_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (session_id, seq, json.dumps(item), utc_now_iso()),
        )
    return seq


def list_items(project_root: Path, session_id: str) -> list[dict[str, Any]]:
    """Return the session transcript's item envelopes in insertion order."""
    with closing(connect(project_root)) as conn:
        rows = conn.execute(
            "SELECT item_json FROM assistant_items WHERE session_id = ? ORDER BY seq",
            (session_id,),
        ).fetchall()
    return [json.loads(row["item_json"]) for row in rows]


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

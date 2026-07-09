"""Synchronous CRUD over the webapp assistant tables.

Thin data-access functions over :mod:`lhp.webapp.services.sqlite_store` for
the ``assistant_sessions`` / ``assistant_config`` / ``assistant_items`` /
``assistant_turn_usage`` tables. Everything here is synchronous and opens one
connection per call; async callers bridge via ``asyncio.to_thread`` or
FastAPI's sync-handler threadpool.

Sessions track omnigent conversations (``session_id`` = ``conv_...``,
``agent_id`` = ``ag_...``) or Claude SDK sessions (``session_id`` =
``claude_...``, ``provider`` column discriminates). Status vocabulary:
``active`` = open tab, ``archived`` = closed tab, ``stale`` = config drift.
The omnigent provider keeps a single-active invariant —
:func:`insert_session` archives every previously active session in the same
transaction that activates the new one. The Claude provider allows MANY
active sessions (multi-tab); :func:`insert_claude_session` demotes nothing,
and :func:`get_active_session` returns the most recently used active row.

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
    """Return the most recently used ``status = 'active'`` row, or ``None``.

    Multiple active rows can coexist (Claude multi-tab); callers that pass no
    explicit session id fall back to this MRU row.
    """
    with closing(connect(project_root)) as conn:
        row = conn.execute(
            "SELECT * FROM assistant_sessions WHERE status = 'active' "
            f"{_MOST_RECENT_FIRST} LIMIT 1"
        ).fetchone()
    return dict(row) if row is not None else None


def get_session(project_root: Path, session_id: str) -> Optional[dict[str, Any]]:
    """Return the session row for ``session_id`` (any status), or ``None``."""
    with closing(connect(project_root)) as conn:
        row = conn.execute(
            "SELECT * FROM assistant_sessions WHERE session_id = ?", (session_id,)
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
    """Insert a new ``active`` Claude SDK session (no demotion — multi-tab).

    Unlike :func:`insert_session`, previously active sessions stay active:
    every open tab is one active Claude row. ``agent_id`` / ``host_id`` are
    Omnigent concepts with no Claude equivalent; the columns are NOT NULL, so
    Claude rows write the sentinels ``''`` / ``'local'``. ``title`` is
    normally left ``NULL`` (the placeholder) so the first user message can
    claim it via :func:`set_title_if_default`.
    """
    now = utc_now_iso()
    with closing(connect(project_root)) as conn, conn:
        conn.execute(
            "INSERT INTO assistant_sessions "
            "(session_id, agent_id, host_id, agent_bundle_hash, title, status, "
            "created_at, last_used_at, provider) "
            "VALUES (?, '', 'local', ?, ?, 'active', ?, ?, 'claude_sdk')",
            (session_id, agent_bundle_hash, title, now, now),
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


#: Aggregation over ``assistant_turn_usage``: token sums plus cost sums.
#: SQLite ``SUM`` skips NULLs and yields NULL when every addend is NULL, so
#: an all-unpriced session reports ``configured_cost_usd = None`` (never 0).
_USAGE_TOTALS_SELECT = (
    "SELECT session_id, "
    "SUM(input_tokens) AS input_tokens, "
    "SUM(output_tokens) AS output_tokens, "
    "SUM(cache_read_input_tokens) AS cache_read_input_tokens, "
    "SUM(cache_creation_input_tokens) AS cache_creation_input_tokens, "
    "SUM(sdk_cost_usd) AS sdk_cost_usd, "
    "SUM(configured_cost_usd) AS configured_cost_usd "
    "FROM assistant_turn_usage"
)


def insert_turn_usage(
    project_root: Path,
    session_id: str,
    usage: dict[str, int],
    sdk_cost_usd: Optional[float],
    configured_cost_usd: Optional[float],
    model_usage: Optional[dict[str, Any]],
) -> int:
    """Append one turn's usage row; return its assigned ``turn_seq``.

    ``usage`` is the normalized canonical counter dict
    (:func:`lhp.webapp.services.assistant_usage.normalize_usage`); missing
    counters write as 0. ``turn_seq`` is ``MAX(turn_seq) + 1`` inside the
    insert's transaction — same single-writer posture as :func:`insert_item`.
    """
    with closing(connect(project_root)) as conn, conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(turn_seq), 0) + 1 FROM assistant_turn_usage "
            "WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        turn_seq = int(row[0])
        conn.execute(
            "INSERT INTO assistant_turn_usage "
            "(session_id, turn_seq, input_tokens, output_tokens, "
            "cache_read_input_tokens, cache_creation_input_tokens, "
            "sdk_cost_usd, configured_cost_usd, model_usage_json, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                session_id,
                turn_seq,
                int(usage.get("input_tokens", 0)),
                int(usage.get("output_tokens", 0)),
                int(usage.get("cache_read_input_tokens", 0)),
                int(usage.get("cache_creation_input_tokens", 0)),
                sdk_cost_usd,
                configured_cost_usd,
                json.dumps(model_usage) if model_usage else None,
                utc_now_iso(),
            ),
        )
    return turn_seq


def _totals_row(row: Any) -> dict[str, Any]:
    return {
        "input_tokens": int(row["input_tokens"]),
        "output_tokens": int(row["output_tokens"]),
        "cache_read_input_tokens": int(row["cache_read_input_tokens"]),
        "cache_creation_input_tokens": int(row["cache_creation_input_tokens"]),
        "sdk_cost_usd": row["sdk_cost_usd"],
        "configured_cost_usd": row["configured_cost_usd"],
    }


def usage_totals(project_root: Path, session_id: str) -> Optional[dict[str, Any]]:
    """Summed usage for one session, or ``None`` when it has no usage rows."""
    with closing(connect(project_root)) as conn:
        row = conn.execute(
            f"{_USAGE_TOTALS_SELECT} WHERE session_id = ?", (session_id,)
        ).fetchone()
    # An aggregate over zero rows still yields one all-NULL row.
    if row is None or row["input_tokens"] is None:
        return None
    return _totals_row(row)


def usage_totals_by_session(
    project_root: Path, session_ids: list[str]
) -> dict[str, dict[str, Any]]:
    """Summed usage per session in ONE grouped query (list views, no N+1).

    Sessions without usage rows are simply absent from the result.
    """
    if not session_ids:
        return {}
    placeholders = ", ".join("?" for _ in session_ids)
    with closing(connect(project_root)) as conn:
        rows = conn.execute(
            f"{_USAGE_TOTALS_SELECT} WHERE session_id IN ({placeholders}) "
            "GROUP BY session_id",
            tuple(session_ids),
        ).fetchall()
    return {str(row["session_id"]): _totals_row(row) for row in rows}


def archive_active(project_root: Path) -> int:
    """Demote the active session (if any) to ``archived``; return rows changed."""
    with closing(connect(project_root)) as conn, conn:
        return conn.execute(
            "UPDATE assistant_sessions SET status = 'archived' WHERE status = 'active'"
        ).rowcount


def archive_session(project_root: Path, session_id: str) -> int:
    """Demote ``session_id`` from ``active`` to ``archived`` (tab closed).

    Returns rows changed — 0 when the session is unknown or not active
    (a stale row is never resurrected into ``archived``).
    """
    with closing(connect(project_root)) as conn, conn:
        return conn.execute(
            "UPDATE assistant_sessions SET status = 'archived' "
            "WHERE session_id = ? AND status = 'active'",
            (session_id,),
        ).rowcount


def reopen_session(project_root: Path, session_id: str) -> int:
    """Promote ``session_id`` from ``archived`` back to ``active`` (tab reopened).

    Bumps ``last_used_at`` so the reopened session becomes the MRU active
    row. Returns rows changed — 0 when unknown or not archived.
    """
    with closing(connect(project_root)) as conn, conn:
        return conn.execute(
            "UPDATE assistant_sessions SET status = 'active', last_used_at = ? "
            "WHERE session_id = ? AND status = 'archived'",
            (utc_now_iso(), session_id),
        ).rowcount


def set_title_if_default(project_root: Path, session_id: str, title: str) -> int:
    """Set ``title`` only while the session still has the ``NULL`` placeholder.

    Claude sessions are minted with ``title = NULL``; the first user message
    of the session claims the title exactly once — later calls are no-ops.
    Returns rows changed.
    """
    with closing(connect(project_root)) as conn, conn:
        return conn.execute(
            "UPDATE assistant_sessions SET title = ? "
            "WHERE session_id = ? AND title IS NULL",
            (title, session_id),
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

"""Ordered DDL migration batches for the webapp SQLite store.

Each entry in :data:`MIGRATIONS` is one migration batch (a tuple of DDL
statements applied atomically). Batch N brings the database to
``PRAGMA user_version = N``; :func:`lhp.webapp.services.sqlite_store.run_migrations`
applies every batch whose version exceeds the current ``user_version``.

Batches are APPEND-ONLY: never edit or reorder a shipped batch — an existing
database records how far it has migrated solely via ``user_version``. Schema
changes go in a new batch at the end.

All timestamps stored by consumers of this schema are ISO-8601 UTC strings.
"""

from __future__ import annotations

#: v1 — run history: run summaries, the raw NDJSON frame log, and extracted
#: per-run issues. ``run_events`` is keyed by ``(run_id, seq)`` so replay
#: order is the insertion order of the stream.
_V1: tuple[str, ...] = (
    """
    CREATE TABLE runs (
        run_id TEXT PRIMARY KEY,
        kind TEXT NOT NULL,
        env TEXT NOT NULL,
        pipeline TEXT,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        finished_at TEXT,
        summary_json TEXT
    )
    """,
    """
    CREATE TABLE run_events (
        run_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        frame_json TEXT NOT NULL,
        PRIMARY KEY (run_id, seq)
    )
    """,
    """
    CREATE TABLE run_issues (
        run_id TEXT NOT NULL,
        severity TEXT NOT NULL,
        code TEXT,
        message TEXT NOT NULL,
        file TEXT,
        line INTEGER
    )
    """,
    "CREATE INDEX idx_runs_started_at ON runs(started_at)",
    "CREATE INDEX idx_run_issues_run_id ON run_issues(run_id)",
)

#: v2 — assistant: omnigent-backed chat sessions plus the assistant's
#: persisted configuration. ``session_id`` is the omnigent conversation id
#: (``conv_...``) and ``agent_id`` the session-scoped omnigent agent id
#: (``ag_...``); ``agent_bundle_hash`` supports drift detection. ``status``
#: is one of ``active`` / ``archived`` / ``stale`` — at most one row is
#: ``active`` (enforced by :mod:`lhp.webapp.services.assistant_store`).
#: ``assistant_config`` is a JSON key/value store (keys ``executor`` /
#: ``agent``).
_V2: tuple[str, ...] = (
    """
    CREATE TABLE assistant_sessions (
        session_id TEXT PRIMARY KEY,
        agent_id TEXT NOT NULL,
        host_id TEXT NOT NULL,
        agent_bundle_hash TEXT NOT NULL,
        title TEXT,
        status TEXT NOT NULL,
        created_at TEXT NOT NULL,
        last_used_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE assistant_config (
        key TEXT PRIMARY KEY,
        value_json TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    "CREATE INDEX idx_assistant_sessions_status ON assistant_sessions(status)",
)

#: v3 — assistant providers: ``provider`` discriminates ``assistant_sessions``
#: rows between the in-process Claude Agent SDK provider (``claude_sdk``) and
#: the Omnigent daemon (``omnigent``, the pre-v3 implicit value, hence the
#: DEFAULT). ``runtime_session_id`` is the Claude SDK resume handle, refreshed
#: after every turn (LHP's ``session_id`` stays the stable primary key).
#: ``assistant_items`` is the per-session transcript for ``GET /session``
#: rehydration — one JSON envelope per item, keyed ``(session_id, seq)`` so
#: replay order is insertion order; written only by the Claude turn engine.
_V3: tuple[str, ...] = (
    "ALTER TABLE assistant_sessions ADD COLUMN provider TEXT NOT NULL DEFAULT 'omnigent'",
    "ALTER TABLE assistant_sessions ADD COLUMN runtime_session_id TEXT",
    """
    CREATE TABLE assistant_items (
        session_id TEXT NOT NULL,
        seq INTEGER NOT NULL,
        item_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        PRIMARY KEY (session_id, seq)
    )
    """,
)

MIGRATIONS: tuple[tuple[str, ...], ...] = (_V1, _V2, _V3)

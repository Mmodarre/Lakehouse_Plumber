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

MIGRATIONS: tuple[tuple[str, ...], ...] = (_V1,)

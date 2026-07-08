"""SQLite connection and migration plumbing for the webapp persistence layer.

The web IDE persists run history in a single stdlib-``sqlite3`` database at
``<project_root>/.lhp/webapp.db`` (no ORM, no async driver). Every function
here is SYNCHRONOUS and opens a fresh connection per operation; async callers
bridge via ``asyncio.to_thread``.

Connections are configured for a single-user local server that may still see
concurrent access (a run recorder thread plus API reads): WAL journal mode,
a 5s busy timeout, and enforced foreign keys.

Schema lifecycle: :func:`run_migrations` reads ``PRAGMA user_version`` and
applies the ordered DDL batches from
:mod:`lhp.webapp.services.sqlite_migrations`, bumping ``user_version`` once
per batch — idempotent by construction. :func:`mark_orphaned_runs_failed`
is startup crash recovery: any run the previous process left ``running`` is
closed out as ``failed``.
"""

from __future__ import annotations

import logging
import sqlite3
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

from lhp.webapp.services.sqlite_migrations import MIGRATIONS

logger = logging.getLogger(__name__)


def utc_now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string (the DB timestamp format)."""
    return datetime.now(timezone.utc).isoformat()


def db_path(project_root: Path) -> Path:
    """Return the webapp database path for ``project_root`` (not created here)."""
    return project_root / ".lhp" / "webapp.db"


def connect(project_root: Path) -> sqlite3.Connection:
    """Open a new configured connection to the project's webapp database.

    Creates ``.lhp/`` (and the database file) on first use. The caller owns
    the connection and must close it — pair with ``contextlib.closing`` or a
    ``with`` transaction block plus ``close()``.
    """
    path = db_path(project_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def run_migrations(project_root: Path) -> None:
    """Apply every pending DDL batch, bumping ``user_version`` per batch.

    Idempotent: batches at or below the recorded ``user_version`` are skipped,
    so calling this on every startup is safe. Each batch and its version bump
    are applied inside one transaction, so a failed batch leaves the database
    at the previous version.
    """
    with closing(connect(project_root)) as conn:
        current = int(conn.execute("PRAGMA user_version").fetchone()[0])
        for version, batch in enumerate(MIGRATIONS, start=1):
            if version <= current:
                continue
            with conn:
                for statement in batch:
                    conn.execute(statement)
                # PRAGMA does not accept bound parameters; version is a
                # trusted int from enumerate().
                conn.execute(f"PRAGMA user_version = {version}")
            logger.info(f"webapp.db migrated to schema version {version}")


def mark_orphaned_runs_failed(project_root: Path) -> int:
    """Close out runs left ``running`` by a crashed/killed previous process.

    Returns the number of runs transitioned to ``failed``. Called at startup,
    after :func:`run_migrations`.
    """
    with closing(connect(project_root)) as conn:
        with conn:
            cursor = conn.execute(
                "UPDATE runs SET status = 'failed', finished_at = ? "
                "WHERE status = 'running'",
                (utc_now_iso(),),
            )
        orphaned = cursor.rowcount
    if orphaned:
        logger.warning(f"Marked {orphaned} orphaned run(s) as failed at startup")
    return orphaned

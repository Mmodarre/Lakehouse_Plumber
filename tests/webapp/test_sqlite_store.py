"""Unit tests for the webapp SQLite store: migrations, pragmas, crash recovery.

Everything runs against a throwaway ``tmp_path`` acting as the project root —
the store only needs a directory to plant ``.lhp/webapp.db`` in.
"""

from __future__ import annotations

from contextlib import closing
from pathlib import Path

import pytest

from lhp.webapp.services import run_history, sqlite_store
from lhp.webapp.services.sqlite_migrations import MIGRATIONS

pytestmark = pytest.mark.webapp


def _user_version(project_root: Path) -> int:
    with closing(sqlite_store.connect(project_root)) as conn:
        return int(conn.execute("PRAGMA user_version").fetchone()[0])


def test_db_path_is_under_dot_lhp(tmp_path: Path) -> None:
    assert sqlite_store.db_path(tmp_path) == tmp_path / ".lhp" / "webapp.db"


def test_run_migrations_creates_schema_and_bumps_user_version(
    tmp_path: Path,
) -> None:
    sqlite_store.run_migrations(tmp_path)

    assert sqlite_store.db_path(tmp_path).is_file()
    assert _user_version(tmp_path) == len(MIGRATIONS)

    with closing(sqlite_store.connect(tmp_path)) as conn:
        tables = {
            row["name"]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
    assert {"runs", "run_events", "run_issues"} <= tables


def test_run_migrations_is_idempotent(tmp_path: Path) -> None:
    sqlite_store.run_migrations(tmp_path)
    # A second run must skip every applied batch (no "table already exists").
    sqlite_store.run_migrations(tmp_path)
    assert _user_version(tmp_path) == len(MIGRATIONS)


def test_wal_journal_mode_is_active(tmp_path: Path) -> None:
    sqlite_store.run_migrations(tmp_path)
    with closing(sqlite_store.connect(tmp_path)) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert str(mode).lower() == "wal"


def test_mark_orphaned_runs_failed_closes_only_running_runs(
    tmp_path: Path,
) -> None:
    sqlite_store.run_migrations(tmp_path)
    run_history.create_run(tmp_path, "orphan", "validate", "dev")
    run_history.create_run(tmp_path, "done", "generate", "dev")
    run_history.complete_run(tmp_path, "done", "completed", None)

    assert sqlite_store.mark_orphaned_runs_failed(tmp_path) == 1

    orphan = run_history.get_run(tmp_path, "orphan")
    assert orphan is not None
    assert orphan["status"] == "failed"
    assert orphan["finished_at"] is not None

    done = run_history.get_run(tmp_path, "done")
    assert done is not None
    assert done["status"] == "completed"


def test_mark_orphaned_runs_failed_with_no_orphans(tmp_path: Path) -> None:
    sqlite_store.run_migrations(tmp_path)
    assert sqlite_store.mark_orphaned_runs_failed(tmp_path) == 0

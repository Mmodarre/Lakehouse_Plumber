"""Unit tests for the run-history CRUD layer over the webapp SQLite store."""

from __future__ import annotations

from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lhp.webapp.services import run_history, sqlite_store
from lhp.webapp.services.run_history import IssueRecord

pytestmark = pytest.mark.webapp


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """A migrated throwaway project root."""
    sqlite_store.run_migrations(tmp_path)
    return tmp_path


def _set_started_at(project_root: Path, run_id: str, started_at: str) -> None:
    """Backdate a run for deterministic ordering / age assertions."""
    with closing(sqlite_store.connect(project_root)) as conn, conn:
        conn.execute(
            "UPDATE runs SET started_at = ? WHERE run_id = ?", (started_at, run_id)
        )


def test_create_then_get_run(project: Path) -> None:
    run_history.create_run(project, "r1", "validate", "dev", pipeline="bronze")

    run = run_history.get_run(project, "r1")
    assert run is not None
    assert run["run_id"] == "r1"
    assert run["kind"] == "validate"
    assert run["env"] == "dev"
    assert run["pipeline"] == "bronze"
    assert run["status"] == "running"
    assert run["started_at"] is not None
    assert run["finished_at"] is None
    assert run["summary"] is None
    assert run["issues"] == []
    assert run["events"] is None


def test_get_unknown_run_returns_none(project: Path) -> None:
    assert run_history.get_run(project, "nope") is None


def test_append_events_preserves_order_across_batches(project: Path) -> None:
    run_history.create_run(project, "r1", "validate", "dev")
    run_history.append_events(project, "r1", [(1, '{"type":"a"}'), (2, '{"type":"b"}')])
    run_history.append_events(project, "r1", [(3, '{"type":"c"}')])

    run = run_history.get_run(project, "r1", include_events=True)
    assert run is not None
    assert [frame["type"] for frame in run["events"]] == ["a", "b", "c"]


def test_append_events_empty_batch_is_noop(project: Path) -> None:
    run_history.create_run(project, "r1", "validate", "dev")
    run_history.append_events(project, "r1", [])
    run = run_history.get_run(project, "r1", include_events=True)
    assert run is not None
    assert run["events"] == []


def test_complete_run_sets_terminal_fields(project: Path) -> None:
    run_history.create_run(project, "r1", "generate", "dev")
    run_history.complete_run(project, "r1", "completed", '{"success": true}')

    run = run_history.get_run(project, "r1")
    assert run is not None
    assert run["status"] == "completed"
    assert run["finished_at"] is not None
    assert run["summary"] == {"success": True}


def test_add_issues_and_readback(project: Path) -> None:
    run_history.create_run(project, "r1", "validate", "dev")
    run_history.add_issues(
        project,
        "r1",
        [
            IssueRecord("error", "LHP-VAL-001", "bad thing", "pipelines/x.yaml", 3),
            IssueRecord("warning", None, "iffy thing"),
        ],
    )

    run = run_history.get_run(project, "r1")
    assert run is not None
    assert run["issues"] == [
        {
            "severity": "error",
            "code": "LHP-VAL-001",
            "message": "bad thing",
            "file": "pipelines/x.yaml",
            "line": 3,
        },
        {
            "severity": "warning",
            "code": None,
            "message": "iffy thing",
            "file": None,
            "line": None,
        },
    ]


def test_list_runs_newest_first_and_limit(project: Path) -> None:
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    for i in range(3):
        run_id = f"r{i}"
        run_history.create_run(project, run_id, "validate", "dev")
        _set_started_at(project, run_id, (base + timedelta(minutes=i)).isoformat())

    runs = run_history.list_runs(project)
    assert [r["run_id"] for r in runs] == ["r2", "r1", "r0"]

    limited = run_history.list_runs(project, limit=2)
    assert [r["run_id"] for r in limited] == ["r2", "r1"]


def test_prune_keeps_newest_100_of_105(project: Path) -> None:
    base = datetime(2026, 7, 1, tzinfo=timezone.utc)
    for i in range(105):
        run_id = f"r{i:03d}"
        run_history.create_run(project, run_id, "validate", "dev")
        _set_started_at(project, run_id, (base + timedelta(seconds=i)).isoformat())

    deleted = run_history.prune(project, keep_last=100, max_age_days=36500)
    assert deleted == 5

    remaining = run_history.list_runs(project, limit=200)
    ids = {r["run_id"] for r in remaining}
    assert len(ids) == 100
    # The five OLDEST runs are the ones pruned.
    assert {f"r{i:03d}" for i in range(5)} & ids == set()
    assert "r104" in ids


def test_prune_by_age_cascades_events_and_issues(project: Path) -> None:
    run_history.create_run(project, "old", "validate", "dev")
    run_history.append_events(project, "old", [(1, '{"type":"a"}')])
    run_history.add_issues(project, "old", [IssueRecord("error", None, "boom")])
    stale = datetime.now(timezone.utc) - timedelta(days=40)
    _set_started_at(project, "old", stale.isoformat())

    run_history.create_run(project, "fresh", "validate", "dev")

    deleted = run_history.prune(project, keep_last=100, max_age_days=30)
    assert deleted == 1
    assert run_history.get_run(project, "old") is None
    assert run_history.get_run(project, "fresh") is not None

    with closing(sqlite_store.connect(project)) as conn:
        event_rows = conn.execute(
            "SELECT COUNT(*) FROM run_events WHERE run_id = 'old'"
        ).fetchone()[0]
        issue_rows = conn.execute(
            "SELECT COUNT(*) FROM run_issues WHERE run_id = 'old'"
        ).fetchone()[0]
    assert event_rows == 0
    assert issue_rows == 0

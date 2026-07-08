"""Tests for the stdlib mtime-polling file watcher (scan / diff / tick / loop).

``scan_tree`` and ``diff_snapshots`` are pure and tested synchronously
against real tmp trees. The async ``_tick`` integration tests drive ONE
deterministic watch iteration with a stub app (real :class:`EventBus`,
recorded ``invalidate_facade``) — no polling loops, no sleeps. The ``watch``
loop test only checks crash-immunity, again with bounded waits.
"""

from __future__ import annotations

import asyncio
import os
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from lhp.webapp.services import file_watcher
from lhp.webapp.services.event_bus import EventBus

pytestmark = pytest.mark.webapp


def _write(root: Path, rel: str, content: str = "x\n") -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# scan_tree
# ---------------------------------------------------------------------------


def test_scan_tree_records_relposix_mtime_and_size(tmp_path: Path) -> None:
    yaml_file = _write(tmp_path, "pipelines/raw/customer.yaml", "pipeline: p1\n")
    _write(tmp_path, "lhp.yaml", "name: t\n")

    snapshot = file_watcher.scan_tree(tmp_path)

    assert set(snapshot) == {"pipelines/raw/customer.yaml", "lhp.yaml"}
    stat = os.stat(yaml_file)
    assert snapshot["pipelines/raw/customer.yaml"] == (stat.st_mtime, stat.st_size)


def test_scan_tree_ignores_generated_lhp_and_git_prefixes(tmp_path: Path) -> None:
    _write(tmp_path, "lhp.yaml")
    _write(tmp_path, "generated/dev/pipeline.py")
    _write(tmp_path, ".lhp/webapp.db")
    _write(tmp_path, ".git/config")

    assert set(file_watcher.scan_tree(tmp_path)) == {"lhp.yaml"}


def test_scan_tree_rel_prefixes_are_root_level_only(tmp_path: Path) -> None:
    """A NESTED "generated" dir is a user directory and stays watched."""
    _write(tmp_path, "notes/generated/keep.md")

    assert set(file_watcher.scan_tree(tmp_path)) == {"notes/generated/keep.md"}


def test_scan_tree_prunes_excluded_dir_names_anywhere(tmp_path: Path) -> None:
    _write(tmp_path, "pipelines/a.yaml")
    _write(tmp_path, "pipelines/__pycache__/a.cpython-312.pyc")
    _write(tmp_path, "sub/node_modules/pkg/index.js")
    _write(tmp_path, ".venv/lib/site.py")

    assert set(file_watcher.scan_tree(tmp_path)) == {"pipelines/a.yaml"}


def test_scan_tree_skips_editor_temp_files(tmp_path: Path) -> None:
    _write(tmp_path, "pipelines/a.yaml")
    _write(tmp_path, "pipelines/.a.yaml.swp")
    _write(tmp_path, "pipelines/a.tmp")
    _write(tmp_path, "pipelines/a.yaml~")
    _write(tmp_path, "pipelines/.#a.yaml")
    _write(tmp_path, "4913")

    assert set(file_watcher.scan_tree(tmp_path)) == {"pipelines/a.yaml"}


# ---------------------------------------------------------------------------
# diff_snapshots
# ---------------------------------------------------------------------------


def test_diff_detects_created_modified_and_deleted_sorted() -> None:
    old = {
        "kept.yaml": (1.0, 10),
        "modified.yaml": (1.0, 10),
        "deleted.yaml": (1.0, 10),
    }
    new = {
        "kept.yaml": (1.0, 10),
        "modified.yaml": (2.0, 10),
        "created.yaml": (3.0, 5),
    }

    assert file_watcher.diff_snapshots(old, new) == [
        "created.yaml",
        "deleted.yaml",
        "modified.yaml",
    ]


def test_diff_detects_size_only_change() -> None:
    # Same mtime (sub-second edits on coarse filesystems) but different size.
    assert file_watcher.diff_snapshots(
        {"a.yaml": (1.0, 10)}, {"a.yaml": (1.0, 12)}
    ) == ["a.yaml"]


def test_diff_identical_snapshots_is_empty() -> None:
    snapshot = {"a.yaml": (1.0, 10)}
    assert file_watcher.diff_snapshots(snapshot, dict(snapshot)) == []


# ---------------------------------------------------------------------------
# _tick (one deterministic watch iteration)
# ---------------------------------------------------------------------------


def _stub_app(project_root: Path) -> FastAPI:
    """Real FastAPI carrier for state: real EventBus, settings stub."""
    app = FastAPI()
    app.state.settings = SimpleNamespace(project_root=project_root)
    app.state.event_bus = EventBus()
    return app


def test_tick_yaml_change_invalidates_and_publishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "proj"
    _write(project, "lhp.yaml", "name: t\n")
    yaml_file = _write(project, "pipelines/fg.yaml", "pipeline: p1\n")

    app = _stub_app(project)
    queue = app.state.event_bus.subscribe()
    invalidated: list[FastAPI] = []
    monkeypatch.setattr(file_watcher, "invalidate_facade", invalidated.append)

    async def scenario() -> None:
        baseline = await file_watcher._tick(app, None)
        # Baseline scan establishes state and publishes NOTHING.
        assert not invalidated
        assert queue.empty()
        assert "pipelines/fg.yaml" in baseline

        yaml_file.write_text("pipeline: p1\nflowgroup: fg\n", encoding="utf-8")
        snapshot = await file_watcher._tick(app, baseline)

        assert invalidated == [app]
        assert queue.get_nowait() == {
            "event": "file-changed",
            "data": {"paths": ["pipelines/fg.yaml"]},
        }
        assert snapshot["pipelines/fg.yaml"] != baseline["pipelines/fg.yaml"]

    asyncio.run(scenario())


def test_tick_generated_change_is_ignored(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "proj"
    _write(project, "lhp.yaml", "name: t\n")

    app = _stub_app(project)
    queue = app.state.event_bus.subscribe()
    invalidated: list[FastAPI] = []
    monkeypatch.setattr(file_watcher, "invalidate_facade", invalidated.append)

    async def scenario() -> None:
        baseline = await file_watcher._tick(app, None)

        _write(project, "generated/dev/pipeline.py", "print('generated')\n")
        snapshot = await file_watcher._tick(app, baseline)

        # No invalidation and no event for LHP's own codegen output.
        assert not invalidated
        assert queue.empty()
        assert snapshot == baseline

    asyncio.run(scenario())


def test_tick_delete_publishes_removed_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "proj"
    _write(project, "lhp.yaml", "name: t\n")
    doomed = _write(project, "pipelines/doomed.yaml", "pipeline: p1\n")

    app = _stub_app(project)
    queue = app.state.event_bus.subscribe()
    monkeypatch.setattr(file_watcher, "invalidate_facade", lambda _app: None)

    async def scenario() -> None:
        baseline = await file_watcher._tick(app, None)
        doomed.unlink()
        await file_watcher._tick(app, baseline)

        assert queue.get_nowait() == {
            "event": "file-changed",
            "data": {"paths": ["pipelines/doomed.yaml"]},
        }

    asyncio.run(scenario())


# ---------------------------------------------------------------------------
# watch (loop crash-immunity)
# ---------------------------------------------------------------------------


def test_watch_survives_failing_iterations(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raising tick is logged and swallowed; the loop keeps running."""
    failures: list[int] = []

    async def boom(app: FastAPI, previous: object) -> dict[str, tuple[float, int]]:
        failures.append(1)
        raise RuntimeError("scan blew up")

    monkeypatch.setattr(file_watcher, "_tick", boom)

    async def scenario() -> None:
        task = asyncio.create_task(file_watcher.watch(FastAPI(), interval=0.01))
        while len(failures) < 3:  # bounded by the suite's test timeout
            await asyncio.sleep(0.005)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())

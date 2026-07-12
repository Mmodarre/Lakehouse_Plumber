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
# _is_graph_relevant
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "rel, expected",
    [
        ("lhp.yaml", True),
        ("pipelines/raw/fg.yaml", True),
        ("presets/bronze.yaml", True),
        ("templates/t.yaml", True),
        ("substitutions/dev.yaml", True),
        ("blueprints/bp.yaml", True),
        ("py_functions/helper.py", True),
        ("pipelines/raw/transform.sql", True),
        ("docs/readme.md", False),
        ("data/seed.csv", False),
        ("generated/dev/pipeline.py", False),  # ignored prefix wins over .py
        (".lhp/cache/x.py", False),
        (".git/hooks/pre-commit.py", False),
    ],
)
def test_is_graph_relevant(rel: str, expected: bool) -> None:
    assert file_watcher._is_graph_relevant(rel) is expected


# ---------------------------------------------------------------------------
# _tick (one deterministic watch iteration)
# ---------------------------------------------------------------------------


def _stub_app(project_root: Path) -> FastAPI:
    """Real FastAPI carrier for state: real EventBus, settings stub.

    ``graph_stale`` is seeded False, mirroring ``create_app`` — the serve-stale
    flag the watcher sets on a graph-relevant edit.
    """
    app = FastAPI()
    app.state.settings = SimpleNamespace(project_root=project_root)
    app.state.event_bus = EventBus()
    app.state.graph_stale = False
    return app


def test_tick_graph_relevant_change_marks_stale_without_invalidating(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A source-YAML edit serves stale: flag + graph-stale + file-changed, NO
    facade drop (dropping it would nuke the in-process graph memo). The
    discovery memo IS cleared (via invalidate_discovery_caches) so inspection
    reads still reflect the edit."""
    project = tmp_path / "proj"
    _write(project, "lhp.yaml", "name: t\n")
    yaml_file = _write(project, "pipelines/fg.yaml", "pipeline: p1\n")

    app = _stub_app(project)
    queue = app.state.event_bus.subscribe()
    invalidated: list[FastAPI] = []
    discovery_cleared: list[FastAPI] = []
    monkeypatch.setattr(file_watcher, "invalidate_facade", invalidated.append)
    monkeypatch.setattr(
        file_watcher, "invalidate_discovery_caches", discovery_cleared.append
    )

    async def scenario() -> None:
        baseline = await file_watcher._tick(app, None)
        # Baseline scan establishes state and publishes NOTHING.
        assert not invalidated
        assert not discovery_cleared
        assert queue.empty()
        assert app.state.graph_stale is False
        assert "pipelines/fg.yaml" in baseline

        yaml_file.write_text("pipeline: p1\nflowgroup: fg\n", encoding="utf-8")
        snapshot = await file_watcher._tick(app, baseline)

        # Serve-stale: the facade is NOT dropped, but the discovery memo IS
        # cleared so inspection reads reflect the edit.
        assert invalidated == []
        assert discovery_cleared == [app]
        assert app.state.graph_stale is True
        # graph-stale is published first, then the generic file-changed.
        assert queue.get_nowait() == {
            "event": "graph-stale",
            "data": {"paths": ["pipelines/fg.yaml"]},
        }
        assert queue.get_nowait() == {
            "event": "file-changed",
            "data": {"paths": ["pipelines/fg.yaml"]},
        }
        assert snapshot["pipelines/fg.yaml"] != baseline["pipelines/fg.yaml"]

    asyncio.run(scenario())


def test_tick_non_graph_change_invalidates_without_marking_stale(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A non-graph change (docs / data) keeps today's behavior for the OTHER
    caches: invalidate the facade + publish file-changed, but do not mark the
    dependency graph stale or emit graph-stale."""
    project = tmp_path / "proj"
    _write(project, "lhp.yaml", "name: t\n")
    _write(project, "docs/readme.md", "hello\n")

    app = _stub_app(project)
    queue = app.state.event_bus.subscribe()
    invalidated: list[FastAPI] = []
    discovery_cleared: list[FastAPI] = []
    monkeypatch.setattr(file_watcher, "invalidate_facade", invalidated.append)
    monkeypatch.setattr(
        file_watcher, "invalidate_discovery_caches", discovery_cleared.append
    )

    async def scenario() -> None:
        baseline = await file_watcher._tick(app, None)
        (project / "docs" / "readme.md").write_text("hello world\n", encoding="utf-8")
        await file_watcher._tick(app, baseline)

        assert invalidated == [app]
        # A non-graph change drops the whole facade; the discovery-only path is
        # not taken.
        assert discovery_cleared == []
        assert app.state.graph_stale is False
        assert queue.get_nowait() == {
            "event": "file-changed",
            "data": {"paths": ["docs/readme.md"]},
        }
        assert queue.empty()  # no graph-stale event

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

        # A deleted source YAML is graph-relevant: graph-stale then file-changed.
        assert app.state.graph_stale is True
        assert queue.get_nowait() == {
            "event": "graph-stale",
            "data": {"paths": ["pipelines/doomed.yaml"]},
        }
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

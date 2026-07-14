"""Stdlib mtime-polling project watcher driving live updates in the web IDE.

Every :data:`DEFAULT_INTERVAL_SECONDS` the watcher snapshots the project tree
(rel-posix-path -> ``(mtime, size)``) via :func:`scan_tree`, diffs it against
the previous snapshot via :func:`diff_snapshots`, and on any change:

1. if any changed path is graph-relevant (source YAML / config / ``.py`` /
   ``.sql`` — see :func:`_is_graph_relevant`), it sets ``app.state.graph_stale``
   and publishes a ``graph-stale`` event WITHOUT dropping the facade: the
   dependency graph is served stale until an explicit Refresh rebuilds it
   (serve-stale). It DOES clear the facades' flowgroup-discovery memo
   (:func:`~lhp.webapp.dependencies.invalidate_discovery_caches`) so the
   inspection reads (browse / validate / stats) still reflect the edit while
   the graph memo survives. Otherwise (a purely non-graph change) it drops the
   cached facade (:func:`~lhp.webapp.dependencies.invalidate_facade`) so the
   next API request re-discovers the other caches from disk, then
2. publishes a ``file-changed`` bus event (``{"paths": [...]}`` with
   project-root-relative POSIX paths) that the SSE endpoint pushes to the SPA.

Ignored while scanning:

* directory *names* anywhere in the tree from
  :data:`~lhp.webapp.services.file_io._EXCLUDED_DIR_NAMES` (``.git``,
  ``__pycache__``, ``.venv``, ``node_modules``, ...),
* the project-root-relative prefixes ``generated/``, ``.lhp/`` and ``.git/``
  (LHP's own outputs must not invalidate the facade they came from), and
* editor temp files (``*.swp``, ``*.tmp``, ``*~``, ``.#*``, vim's ``4913``).

Engine-selection seam: :func:`watch` is the single entry point the app
lifespan starts. A faster native engine (e.g. ``watchfiles``, currently not
permitted under the §5.8 import contract) would slot in behind this same
coroutine without touching the wiring or the tick semantics.

The first scan only establishes the baseline — nothing is published on
startup. Per-iteration errors are logged and swallowed: the watcher must
outlive transient scan failures.
"""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING

from lhp.webapp.dependencies import invalidate_discovery_caches, invalidate_facade
from lhp.webapp.services.file_io import _EXCLUDED_DIR_NAMES

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)

DEFAULT_INTERVAL_SECONDS = 2.0

# Project-root-relative POSIX prefixes never watched (root-level only; a
# nested "generated" directory elsewhere is a user directory and IS watched).
_IGNORED_REL_PREFIXES: tuple[str, ...] = ("generated/", ".lhp/", ".git/")

# Editor temp-file patterns, matched (fnmatch) against the file NAME: vim
# swap files, generic temp files, backup files, emacs lock files, and vim's
# literal "4913" write-probe file.
_TEMP_FILE_PATTERNS: tuple[str, ...] = ("*.swp", "*.tmp", "*~", ".#*", "4913")

# Project-root-relative POSIX prefixes / files whose change can alter the
# dependency graph. Source directories (flowgroup / preset / template /
# substitution / blueprint YAML) plus the project config; the graph itself is
# rebuilt only on an explicit Refresh, so a change here just marks it stale.
_GRAPH_RELEVANT_PREFIXES: tuple[str, ...] = (
    "pipelines/",
    "presets/",
    "templates/",
    "substitutions/",
    "blueprints/",
)
_GRAPH_RELEVANT_FILES: frozenset[str] = frozenset({"lhp.yaml"})
# The dependency extractor reads external .py / .sql transform bodies too
# (Phase B's body manifest tracks both), so editing one drifts the graph.
_GRAPH_BODY_SUFFIXES: tuple[str, ...] = (".py", ".sql")


def _is_temp_file(name: str) -> bool:
    """Return True when ``name`` matches an editor temp-file pattern."""
    return any(fnmatch.fnmatch(name, pattern) for pattern in _TEMP_FILE_PATTERNS)


def _is_ignored_rel(rel: str) -> bool:
    """Return True when the rel-posix path falls under an ignored prefix."""
    return any(rel.startswith(prefix) for prefix in _IGNORED_REL_PREFIXES)


def _is_graph_relevant(rel: str) -> bool:
    """Return True when a changed rel-posix path can alter the dependency graph.

    Covers the source YAML (``pipelines/``, ``presets/``, ``templates/``,
    ``substitutions/``, ``blueprints/``), the project config (``lhp.yaml``),
    and any ``.py`` / ``.sql`` body the extractor reads — but never LHP's own
    outputs under the ignored prefixes (``generated/`` / ``.lhp/`` / ``.git/``).
    """
    if _is_ignored_rel(rel):
        return False
    if rel in _GRAPH_RELEVANT_FILES:
        return True
    if rel.endswith(_GRAPH_BODY_SUFFIXES):
        return True
    return rel.startswith(_GRAPH_RELEVANT_PREFIXES)


def scan_tree(
    root: Path, *, ignore_dirs: frozenset[str] = _EXCLUDED_DIR_NAMES
) -> dict[str, tuple[float, int]]:
    """Snapshot ``root``: rel-posix-path -> ``(mtime, size)`` per watched file.

    Pure and synchronous (no asyncio) so it is unit-testable in isolation and
    can be pushed onto a worker thread by the async tick. Directories whose
    *name* is in ``ignore_dirs`` are pruned anywhere in the tree; the
    root-relative prefixes in :data:`_IGNORED_REL_PREFIXES` are pruned at the
    top level; editor temp files are skipped. Files vanishing mid-scan are
    silently dropped from the snapshot.
    """
    snapshot: dict[str, tuple[float, int]] = {}
    for dirpath, dirnames, filenames in os.walk(root):
        rel_dir = Path(dirpath).relative_to(root).as_posix()
        prefix = "" if rel_dir == "." else f"{rel_dir}/"
        dirnames[:] = [
            name
            for name in dirnames
            if name not in ignore_dirs and not _is_ignored_rel(f"{prefix}{name}/")
        ]
        for filename in filenames:
            if _is_temp_file(filename):
                continue
            rel = f"{prefix}{filename}"
            if _is_ignored_rel(rel):
                continue
            try:
                stat = os.stat(os.path.join(dirpath, filename))
            except OSError:
                continue
            snapshot[rel] = (stat.st_mtime, stat.st_size)
    return snapshot


def diff_snapshots(
    old: dict[str, tuple[float, int]], new: dict[str, tuple[float, int]]
) -> list[str]:
    """Return sorted rel paths created, modified, or deleted between snapshots."""
    changed = set(old.keys() ^ new.keys())
    changed.update(path for path in old.keys() & new.keys() if old[path] != new[path])
    return sorted(changed)


async def _tick(
    app: FastAPI, previous: dict[str, tuple[float, int]] | None
) -> dict[str, tuple[float, int]]:
    """Run one watch iteration and return the new snapshot.

    ``previous is None`` is the baseline scan: it records current state and
    publishes nothing. Otherwise any diff is partitioned into graph-relevant
    and other changes:

    * A graph-relevant change (source YAML / config / ``.py`` / ``.sql``) does
      NOT drop the facade — that would nuke the in-process dependency-graph
      memo and force a rebuild on the next read. Serve-stale instead: flag
      ``app.state.graph_stale`` and publish ``graph-stale`` so the SPA offers a
      manual Refresh (which force-rebuilds via ``POST /api/dependencies/refresh``).
      The facade's flowgroup-discovery memo IS cleared
      (:func:`~lhp.webapp.dependencies.invalidate_discovery_caches`) so the
      inspection reads reflect the edit while the graph memo survives. The
      per-env dataset lineage index (``GET /api/lineage``) is likewise served
      stale — kept, not dropped — so it stays consistent with the surviving
      graph memo it was built against; only an explicit Refresh rebuilds both.
    * A purely non-graph change keeps today's behavior — ``invalidate_facade``
      so the other caches (files list, validate) re-read from disk.

    Either way a ``file-changed`` event carrying every changed rel path is
    published so the SPA re-fetches. Genuine mutations made THROUGH the API
    (file writes/deletes, project-config writes) still invalidate the facade at
    their own call sites, unaffected by this path. The scan runs on a worker
    thread — ``os.walk`` over a large project must not block the event loop.
    """
    root: Path = app.state.settings.project_root
    snapshot = await asyncio.to_thread(scan_tree, root)
    if previous is not None:
        changed = diff_snapshots(previous, snapshot)
        if changed:
            logger.info(f"File watcher: {len(changed)} change(s) detected")
            graph_relevant = [rel for rel in changed if _is_graph_relevant(rel)]
            if graph_relevant:
                app.state.graph_stale = True
                # Serve-stale for the graph, but the inspection caches (browse /
                # validate / stats) must still reflect the edit: clear their
                # discovery memo WITHOUT dropping the facade, so the graph memo
                # survives and keeps serving last-good until an explicit refresh.
                invalidate_discovery_caches(app)
                # The per-env dataset lineage index (GET /api/lineage) is NOT
                # dropped here: it shares the graph memo's serve-stale lifecycle.
                # Keeping the last-good index leaves it consistent with the
                # served-stale graph it was built against; dropping it while the
                # graph memo survives would rebuild it against that stale graph
                # and false-404 a produced dataset whose write node is missing
                # from the memo (e.g. a just-added/renamed flowgroup). Only an
                # explicit POST /api/dependencies/refresh rebuilds both; the
                # endpoint's stale=True flag is the honest skew signal until then.
                app.state.event_bus.publish(
                    {"event": "graph-stale", "data": {"paths": graph_relevant}}
                )
            else:
                invalidate_facade(app)
            app.state.event_bus.publish(
                {"event": "file-changed", "data": {"paths": changed}}
            )
    return snapshot


async def watch(app: FastAPI, *, interval: float = DEFAULT_INTERVAL_SECONDS) -> None:
    """Poll the project tree forever, pushing invalidations and bus events.

    Started by the app lifespan (project state ``"ok"`` only) as a background
    task and stopped by cancelling that task. A failing iteration is logged
    with its traceback and retried after the next sleep — the watcher never
    dies on transient errors. Cancellation propagates (``CancelledError`` is
    not an ``Exception``), so shutdown stays prompt.
    """
    snapshot: dict[str, tuple[float, int]] | None = None
    while True:
        try:
            snapshot = await _tick(app, snapshot)
        except Exception:
            logger.exception("File watcher iteration failed; continuing")
        await asyncio.sleep(interval)

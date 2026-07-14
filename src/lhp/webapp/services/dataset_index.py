"""Per-env cache of the project dataset lineage index for the ``lhp web`` backend.

The lineage endpoint (:mod:`lhp.webapp.routers.lineage`) answers a table→table
lineage lookup out of :meth:`lhp.api.DependencyFacade.build_dataset_index`,
whose first call per environment pays the same per-flowgroup env-resolution
cost that ``GET /api/tables`` pays on EVERY request. This module caches that
result per environment and pre-indexes it by FQN, so a lookup after the first
build is an O(1) dict hit.

Freshness follows the dependency graph's serve-stale model: this cache shares
the in-process graph memo's lifecycle, so a graph-relevant edit does NOT drop
it — the file watcher only flips ``app.state.graph_stale`` and serves the
last-good index (see :mod:`lhp.webapp.services.file_watcher`). Only
``POST /api/dependencies/refresh`` drops this cache, rebuilding it against the
freshly-rebuilt graph. The endpoint reports the project-wide
``app.state.graph_stale`` flag as its ``stale`` field so the SPA knows the
underlying graph — and hence this index — may be behind until an explicit
refresh. Dropping the index on every edit while keeping the graph memo would
rebuild it against the stale graph and silently 404 a produced dataset whose
write node is not yet in that memo.

Multi-writer collision policy: :class:`~lhp.api.DatasetIndexResult` ``datasets``
is a flat tuple and two write actions may target one FQN. The FQN index keeps
EVERY writer's position in deterministic result order; :meth:`CachedDatasetIndex.lookup`
serves the first and returns a warning naming the others — a second writer is
never silently dropped.

Per the ``webapp-uses-public-api`` import contract, this module imports only
:mod:`lhp.api` from the ``lhp`` package (plus the standard library).
"""

from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from typing import TYPE_CHECKING

from lhp.api import DatasetIndexResult, DatasetView, DependencyFacade

if TYPE_CHECKING:
    from fastapi import FastAPI

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CachedDatasetIndex:
    """A built :class:`~lhp.api.DatasetIndexResult` with an FQN→positions index.

    ``positions`` maps each dataset FQN to the tuple of indices into
    ``result.datasets`` that produce it, in the result's deterministic order —
    the O(1) lookup key and the backing for the multi-writer collision policy.
    """

    result: DatasetIndexResult
    positions: dict[str, tuple[int, ...]]

    @classmethod
    def from_result(cls, result: DatasetIndexResult) -> "CachedDatasetIndex":
        """Index ``result.datasets`` by FQN, preserving per-FQN result order."""
        positions: dict[str, list[int]] = {}
        for index, dataset in enumerate(result.datasets):
            positions.setdefault(dataset.fqn, []).append(index)
        return cls(
            result=result,
            positions={fqn: tuple(idx) for fqn, idx in positions.items()},
        )

    def lookup(self, fqn: str) -> tuple[DatasetView, tuple[str, ...]] | None:
        """Return the primary dataset for ``fqn`` plus any collision warnings.

        Serves the FIRST writer in deterministic result order. When more than
        one write action targets ``fqn``, a single warning naming the other
        writer(s) is returned alongside it. Returns ``None`` when no produced
        dataset matches ``fqn``.
        """
        found = self.positions.get(fqn)
        if not found:
            return None
        primary = self.result.datasets[found[0]]
        if len(found) == 1:
            return primary, ()
        others = ", ".join(
            f"{ds.pipeline}/{ds.flowgroup}/{ds.action_name}"
            for ds in (self.result.datasets[i] for i in found[1:])
        )
        return primary, (f"multiple writers for {fqn}: also written by {others}",)


class DatasetIndexCache:
    """Process-lifetime per-env cache of the dataset lineage index.

    One instance lives on ``app.state.dataset_index_cache`` (seeded by
    :func:`lhp.webapp.app.create_app`). Builds are serialised by a lock with the
    double-checked-locking discipline of
    :func:`lhp.webapp.dependencies.get_facade_for`, so a single build happens
    per environment even under concurrent first requests.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, CachedDatasetIndex] = {}

    def get(self, dependency: DependencyFacade, *, env: str) -> CachedDatasetIndex:
        """Return the cached index for ``env``, building it once on a miss.

        Blocking: on a cache miss it calls
        :meth:`~lhp.api.DependencyFacade.build_dataset_index`, so callers on the
        event loop must dispatch this via ``asyncio.to_thread``.
        """
        cached = self._entries.get(env)
        if cached is not None:
            return cached
        with self._lock:
            cached = self._entries.get(env)
            if cached is None:
                result = dependency.build_dataset_index(env=env)
                cached = CachedDatasetIndex.from_result(result)
                self._entries[env] = cached
        return cached

    def clear(self) -> None:
        """Drop every cached environment (the next request rebuilds from disk)."""
        with self._lock:
            self._entries = {}


def invalidate(app: "FastAPI") -> None:
    """Drop the app's dataset-index cache if one has been seeded.

    Called only by ``POST /api/dependencies/refresh``, which rebuilds the
    dependency graph and this index together. The file watcher deliberately does
    NOT call this on a graph-relevant edit — the index shares the graph memo's
    serve-stale lifecycle and is kept last-good until an explicit refresh, so it
    stays consistent with the served-stale graph. Tolerant of a missing cache
    (some unit tests drive a bare stub app), so it is always safe to call.
    """
    cache: DatasetIndexCache | None = getattr(app.state, "dataset_index_cache", None)
    if cache is not None:
        cache.clear()

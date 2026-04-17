"""Per-run staleness analysis cache for LakehousePlumber.

Caches the env-wide result of ``StateAnalyzer.get_all_files_needing_generation``
so that the display phase and the filter phase share a single scan instead of
running O(P+1) overlapping passes for P pipelines.
"""

import logging
import threading
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class StalenessCache:
    """Per-run cache of env-wide staleness analysis.

    The cached value is the same shape as
    ``StateAnalyzer.get_all_files_needing_generation``:
    ``pipeline -> {"new": [...], "stale": [...], "up_to_date": [...]}``.

    Thread-safe via ``threading.Lock`` for use during parallel flowgroup
    generation.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, Dict[str, Dict[str, List]]] = {}
        self._lock = threading.Lock()

    def get(
        self,
        env: str,
        loader: Callable[[str], Dict[str, Dict[str, List]]],
    ) -> Dict[str, Dict[str, List]]:
        """Return the cached env-wide staleness result, populating via loader on miss.

        The loader runs *inside* the lock. The whole point of this cache is
        "compute once, reuse everywhere" — serializing a single env's first
        scan is fine, and it guarantees exactly one loader invocation per
        env even under the parallel flowgroup generation path.

        Args:
            env: Environment name (cache key).
            loader: Callable that takes the env name and returns the full
                per-pipeline staleness dict. Invoked exactly once per env
                per cache lifetime.

        Returns:
            Dict mapping pipeline name -> {"new": [...], "stale": [...], "up_to_date": [...]}.
        """
        with self._lock:
            if env not in self._cache:
                self._cache[env] = loader(env)
            return self._cache[env]

    def invalidate(self, env: Optional[str] = None) -> None:
        """Drop cached staleness results.

        Args:
            env: If provided, drop only that env's entry. Otherwise clear all.
        """
        with self._lock:
            if env is None:
                self._cache.clear()
            else:
                self._cache.pop(env, None)

    @property
    def size(self) -> int:
        """Number of cached env entries."""
        with self._lock:
            return len(self._cache)

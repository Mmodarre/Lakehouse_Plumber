"""Unit tests for StalenessCache.

The cache sits between the display phase and the filter phase of ``lhp
generate``. Its contract: *exactly one* env-wide staleness scan per run,
thread-safe under parallel flowgroup generation, and drop the cached value
when the state file is saved.
"""

import threading
from typing import Any, Dict

import pytest

from lhp.core.state.staleness_cache import StalenessCache


def _fake_result(pipeline: str = "p1") -> Dict[str, Dict[str, Any]]:
    return {pipeline: {"new": [], "stale": [], "up_to_date": []}}


class TestStalenessCacheGet:
    def test_loader_invoked_once_per_env(self):
        cache = StalenessCache()
        calls = {"count": 0}

        def loader(env: str):
            calls["count"] += 1
            return _fake_result("p")

        assert cache.get("dev", loader) == _fake_result("p")
        assert cache.get("dev", loader) == _fake_result("p")
        assert cache.get("dev", loader) == _fake_result("p")
        assert calls["count"] == 1

    def test_env_scoping_independent(self):
        cache = StalenessCache()
        calls = {"count": 0}

        def loader(env: str):
            calls["count"] += 1
            return {env: {"new": [], "stale": [], "up_to_date": []}}

        cache.get("dev", loader)
        cache.get("prod", loader)
        cache.get("dev", loader)
        assert calls["count"] == 2


class TestStalenessCacheInvalidate:
    def test_invalidate_all_forces_reload(self):
        cache = StalenessCache()
        calls = {"count": 0}

        def loader(env: str):
            calls["count"] += 1
            return _fake_result("p")

        cache.get("dev", loader)
        cache.invalidate()
        cache.get("dev", loader)
        assert calls["count"] == 2

    def test_invalidate_specific_env_preserves_others(self):
        cache = StalenessCache()
        calls = {"count": 0}

        def loader(env: str):
            calls["count"] += 1
            return {env: {"new": [], "stale": [], "up_to_date": []}}

        cache.get("dev", loader)
        cache.get("prod", loader)
        assert calls["count"] == 2
        cache.invalidate("dev")
        cache.get("dev", loader)  # miss, reloads
        cache.get("prod", loader)  # hit, no reload
        assert calls["count"] == 3


class TestStalenessCacheThreadSafety:
    def test_concurrent_reads_load_only_once(self):
        """Under contention the loader must run at most once per env."""
        cache = StalenessCache()
        call_count = {"n": 0}
        # Barrier ensures all threads race into get() simultaneously.
        n_threads = 8
        barrier = threading.Barrier(n_threads)
        results = []
        results_lock = threading.Lock()

        def loader(env: str):
            # Simulate the ~tens-of-ms staleness scan.
            call_count["n"] += 1
            # Small sleep inside the lock window would detect serialization bugs.
            import time

            time.sleep(0.01)
            return _fake_result(env)

        def worker():
            barrier.wait()
            r = cache.get("dev", loader)
            with results_lock:
                results.append(r)

        threads = [threading.Thread(target=worker) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert call_count["n"] == 1
        assert len(results) == n_threads
        # All callers observe the same dict (identity), proving they're
        # sharing the cached entry rather than each computing their own.
        assert all(r is results[0] for r in results)


class TestStalenessCacheSizeProperty:
    def test_size_reflects_populated_envs(self):
        cache = StalenessCache()
        assert cache.size == 0
        cache.get("dev", lambda env: _fake_result("p"))
        assert cache.size == 1
        cache.get("prod", lambda env: _fake_result("p"))
        assert cache.size == 2
        cache.invalidate()
        assert cache.size == 0

"""Unit tests for :class:`DependencyTracker`'s factory classmethods.

Two construction factories make the tracker's scope explicit:

- :meth:`DependencyTracker.for_pipeline` — worker-side scope. Owns a
  per-instance :class:`ChecksumCache` (never crosses the spawn boundary)
  so a multi-doc source YAML is hashed once per pipeline.
- :meth:`DependencyTracker.for_project` — main-thread scope. Accepts an
  optional :class:`ChecksumCache` for source-side checksum deduplication.
"""

import tempfile
from pathlib import Path

import pytest

from lhp.core.state.checksum_cache import ChecksumCache
from lhp.core.state.dependency_tracker import DependencyTracker
from lhp.core.state.pipeline_state_manager import PipelineState
from lhp.core.state_models import ProjectState


# ----------------------------------------------------------------------
# for_pipeline factory
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestForPipelineFactory:
    """Worker-scope construction. Worker-local cache by contract."""

    def test_returns_tracker_with_state_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = PipelineState(pipeline="raw_pipeline", environments={})
            tracker = DependencyTracker.for_pipeline(state, Path(tmp))
            assert isinstance(tracker, DependencyTracker)
            assert tracker._state is state

    def test_attaches_worker_local_checksum_cache(self):
        """Workers get their own :class:`ChecksumCache` so multi-doc
        source YAMLs are hashed once per pipeline."""
        with tempfile.TemporaryDirectory() as tmp:
            state = PipelineState(pipeline="raw_pipeline", environments={})
            tracker = DependencyTracker.for_pipeline(state, Path(tmp))
            assert isinstance(tracker._checksum_cache, ChecksumCache)
            # Distinct trackers must NOT share their cache (one cache per worker).
            other = DependencyTracker.for_pipeline(state, Path(tmp))
            assert tracker._checksum_cache is not other._checksum_cache


# ----------------------------------------------------------------------
# for_project factory
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestForProjectFactory:
    """Main-thread-scope construction. Cache is optional but allowed."""

    def test_returns_tracker_with_state_stored(self):
        with tempfile.TemporaryDirectory() as tmp:
            state = ProjectState()
            tracker = DependencyTracker.for_project(state, Path(tmp))
            assert isinstance(tracker, DependencyTracker)
            assert tracker._state is state
            # Without a cache argument, no cache is attached.
            assert tracker._checksum_cache is None

    def test_passes_checksum_cache_through(self):
        """The cache supplied at factory time replaces the dropped
        ``set_checksum_cache`` injection point."""
        with tempfile.TemporaryDirectory() as tmp:
            state = ProjectState()
            cache = ChecksumCache()
            tracker = DependencyTracker.for_project(state, Path(tmp), checksum_cache=cache)
            assert tracker._checksum_cache is cache

    def test_checksum_cache_propagates_to_dependency_resolver(self):
        """The dependency resolver inside the tracker also receives the cache,
        so its source-side checksum lookups dedupe through the same cache."""
        with tempfile.TemporaryDirectory() as tmp:
            state = ProjectState()
            cache = ChecksumCache()
            tracker = DependencyTracker.for_project(state, Path(tmp), checksum_cache=cache)
            # The resolver's cache attribute is consistent with the tracker's.
            # Probing via the public set_checksum_cache pathway is brittle;
            # we just confirm the resolver got the same instance.
            assert getattr(tracker.dependency_resolver, "_checksum_cache", None) is cache


# ----------------------------------------------------------------------
# Constructor still works without state/cache (back-compat)
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestConstructorBackCompat:
    """Existing callers ``DependencyTracker(project_root)`` continue to work."""

    def test_no_state_no_cache_is_acceptable(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = DependencyTracker(Path(tmp))
            assert tracker._state is None
            assert tracker._checksum_cache is None

    def test_calculate_checksum_uses_cache_when_present(self):
        """When constructed via ``for_project(..., cache=)``, the tracker's
        ``calculate_checksum`` looks up via the cache rather than reading the
        file each call. Hits the same path the legacy ``set_checksum_cache``
        used to wire up."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            target = tmp_path / "f.txt"
            target.write_text("payload")

            cache = ChecksumCache()
            state = ProjectState()
            tracker = DependencyTracker.for_project(state, tmp_path, checksum_cache=cache)

            digest1 = tracker.calculate_checksum(target)
            digest2 = tracker.calculate_checksum(target)
            assert digest1 == digest2
            assert digest1 != ""  # actually computed
            assert cache.size == 1  # one cache entry created


# ----------------------------------------------------------------------
# _StateLike protocol structural typing
# ----------------------------------------------------------------------


@pytest.mark.unit
class TestStateLikeProtocol:
    """Both :class:`ProjectState` and :class:`PipelineState` satisfy the
    structural ``_StateLike`` type used by the tracker's internals.

    A protocol-level test catches refactors that accidentally drop the
    ``environments`` attribute from either dataclass.
    """

    def test_project_state_has_environments(self):
        state = ProjectState()
        assert hasattr(state, "environments")
        assert isinstance(state.environments, dict)

    def test_pipeline_state_has_environments(self):
        state = PipelineState()
        assert hasattr(state, "environments")
        assert isinstance(state.environments, dict)

    def test_for_pipeline_accepts_pipeline_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = DependencyTracker.for_pipeline(PipelineState(), Path(tmp))
            assert isinstance(tracker, DependencyTracker)

    def test_for_project_accepts_project_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            tracker = DependencyTracker.for_project(ProjectState(), Path(tmp))
            assert isinstance(tracker, DependencyTracker)

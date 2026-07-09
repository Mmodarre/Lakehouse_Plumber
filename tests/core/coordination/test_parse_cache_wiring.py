"""Engine-level tests for persistent parse-cache wiring.

These exercise the orchestrator wiring DIRECTLY (no CLI), mirroring
``test_max_workers_wiring.py``: the CLI surface only parses ``--no-cache``
into a kwarg. Everything load-bearing — default-on store construction under
``<project>/.lhp/cache/parse``, the ``no_cache`` param and ``LHP_NO_CACHE``
env disable switches, and the single store instance shared between the
orchestrator's parser and the ``DependencyAnalysisService``'s — is engine
behavior and lives here (constitution §8.4 / §9.11).
"""

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.parsers import PersistentParseCache


@pytest.fixture
def _bare_project(tmp_path):
    """Minimal lhp.yaml so build_facade_orchestrator can construct."""
    (tmp_path / "lhp.yaml").write_text(
        "name: test_project\nversion: 1.0\nauthor: test\n"
    )
    return tmp_path


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure LHP_NO_CACHE does not leak between tests."""
    monkeypatch.delenv("LHP_NO_CACHE", raising=False)


class TestParseCacheWiring:
    def test_default_constructs_store_under_project_lhp_cache(self, _bare_project):
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        store = orch._cached_yaml_parser._persistent_cache
        assert isinstance(store, PersistentParseCache)
        expected_dir = _bare_project / ".lhp" / "cache" / "parse"
        assert store.cache_dir == expected_dir
        assert expected_dir.is_dir()

    def test_no_cache_true_disables_store_and_creates_nothing(self, _bare_project):
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, no_cache=True
        )
        assert orch._cached_yaml_parser._persistent_cache is None
        assert not (_bare_project / ".lhp" / "cache").exists()

    @pytest.mark.parametrize("env_value", ["1", "true", "yes", "TRUE"])
    def test_env_var_disables_store(self, _bare_project, monkeypatch, env_value):
        monkeypatch.setenv("LHP_NO_CACHE", env_value)
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch._cached_yaml_parser._persistent_cache is None
        assert not (_bare_project / ".lhp" / "cache").exists()

    def test_falsy_env_var_keeps_cache_enabled(self, _bare_project, monkeypatch):
        monkeypatch.setenv("LHP_NO_CACHE", "0")
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert isinstance(
            orch._cached_yaml_parser._persistent_cache, PersistentParseCache
        )

    def test_orchestrator_and_dependency_service_share_one_store(self, _bare_project):
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        store = orch._cached_yaml_parser._persistent_cache
        assert store is not None
        assert orch.dependencies._cached_yaml_parser._persistent_cache is store

    def test_no_cache_propagates_to_dependency_service(self, _bare_project):
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, no_cache=True
        )
        assert orch.dependencies._cached_yaml_parser._persistent_cache is None

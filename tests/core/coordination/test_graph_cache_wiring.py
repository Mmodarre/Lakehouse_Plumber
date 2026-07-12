"""Engine-level tests for persistent graph-cache wiring.

Mirrors ``test_parse_cache_wiring.py``: the orchestrator builds a
:class:`PersistentGraphCache` under ``<project>/.lhp/cache/graph`` (default
on), gated by the SAME ``no_cache`` param / ``LHP_NO_CACHE`` env as the parse
cache, and injects it into the ``DependencyAnalysisService``.
"""

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.core.dependencies import PersistentGraphCache


@pytest.fixture
def _bare_project(tmp_path):
    (tmp_path / "lhp.yaml").write_text(
        "name: test_project\nversion: 1.0\nauthor: test\n"
    )
    return tmp_path


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    monkeypatch.delenv("LHP_NO_CACHE", raising=False)


class TestGraphCacheWiring:
    def test_default_constructs_store_under_project_lhp_cache_graph(
        self, _bare_project
    ):
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        store = orch._persistent_graph_cache
        assert isinstance(store, PersistentGraphCache)
        expected_dir = _bare_project / ".lhp" / "cache" / "graph"
        assert store.cache_dir == expected_dir
        assert expected_dir.is_dir()

    def test_injected_into_dependency_service(self, _bare_project):
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch.dependencies._graph_cache is orch._persistent_graph_cache
        assert orch._persistent_graph_cache is not None

    def test_no_cache_true_disables_graph_cache(self, _bare_project):
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, no_cache=True
        )
        assert orch._persistent_graph_cache is None
        assert orch.dependencies._graph_cache is None
        assert not (_bare_project / ".lhp" / "cache" / "graph").exists()

    @pytest.mark.parametrize("env_value", ["1", "true", "yes", "TRUE"])
    def test_env_var_disables_graph_cache(self, _bare_project, monkeypatch, env_value):
        monkeypatch.setenv("LHP_NO_CACHE", env_value)
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch._persistent_graph_cache is None
        assert not (_bare_project / ".lhp" / "cache" / "graph").exists()

    def test_falsy_env_var_keeps_graph_cache_enabled(self, _bare_project, monkeypatch):
        monkeypatch.setenv("LHP_NO_CACHE", "0")
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert isinstance(orch._persistent_graph_cache, PersistentGraphCache)

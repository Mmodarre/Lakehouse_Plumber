"""Tests for :class:`PersistentGraphCache` — the on-disk dependency-graph
build cache under ``<project>/.lhp/cache/graph/``.

Covers the pickle round-trip (HIT), every invalidation axis (YAML
added/removed/edited, ``.py`` body edit, version-tag bump), corruption /
mkdir-failure degradation, the version-prefixed sweep, and a service-level
integration check that ``analyze_project`` serves an EQUAL result from a
disk HIT (never rebuilding) and REBUILDS after a contributing ``.py`` edit.

The ``.py``-body-edit -> MISS test is the load-bearing correctness proof: a
HIT must never serve a graph stale w.r.t. an external transform body.
"""

import pickle
import time
from pathlib import Path

import networkx as nx
import pytest

from lhp.core.coordination.validation_service import ValidationService
from lhp.core.dependencies import graph_cache as graph_cache_module
from lhp.core.dependencies.graph_cache import PersistentGraphCache
from lhp.core.dependencies.service import DependencyAnalysisService
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig
from lhp.models.dependencies import DependencyAnalysisResult, DependencyGraphs

TRIPLE = (None, None, False)


def _result(edges=(("a", "b"),), external=("src.t",)) -> DependencyAnalysisResult:
    graphs = DependencyGraphs(
        action_graph=nx.DiGraph(),
        flowgroup_graph=nx.DiGraph(),
        pipeline_graph=nx.DiGraph(),
        metadata={"n": 1},
    )
    graphs.action_graph.add_edges_from(edges)
    return DependencyAnalysisResult(
        graphs=graphs,
        pipeline_dependencies={},
        execution_stages=[["p1"]],
        circular_dependencies=[],
        external_sources=list(external),
    )


def _bodies(*paths: Path) -> dict[Path, tuple[int, int]]:
    """Body manifest input: the read-time ``(mtime_ns, size)`` per path, as the
    builder's ParseCache now hands to ``save`` (captured at read, not save)."""
    return {p: (p.stat().st_mtime_ns, p.stat().st_size) for p in paths}


@pytest.fixture
def project(tmp_path):
    """Minimal project tree: one flowgroup YAML plus one external .py body."""
    (tmp_path / "pipelines").mkdir()
    yaml_file = tmp_path / "pipelines" / "fg.yaml"
    yaml_file.write_text("flowgroup: fg\npipeline: p\n")
    body = tmp_path / "pipelines" / "transforms" / "t.py"
    body.parent.mkdir(parents=True)
    body.write_text("def do_it(df, spark, parameters):\n    return df\n")
    return tmp_path, yaml_file, body


@pytest.fixture
def cache(project):
    root = project[0]
    return PersistentGraphCache(root / ".lhp" / "cache" / "graph", root)


@pytest.mark.unit
class TestPersistentGraphCacheStore:
    def test_cold_miss_then_hit_preserves_graph(self, cache, project):
        _, _, body = project
        assert cache.load(TRIPLE) is None  # cold
        cache.save(TRIPLE, _result(), _bodies(body))
        hit = cache.load(TRIPLE)
        assert hit is not None
        assert sorted(hit.graphs.action_graph.edges()) == [("a", "b")]
        assert hit.external_sources == ["src.t"]
        assert hit.execution_stages == [["p1"]]

    def test_yaml_edit_misses(self, cache, project):
        _, yaml_file, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        assert cache.load(TRIPLE) is not None
        time.sleep(0.01)
        yaml_file.write_text("flowgroup: fg\npipeline: p\n# edited\n")
        assert cache.load(TRIPLE) is None

    def test_added_yaml_misses(self, cache, project):
        root, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        assert cache.load(TRIPLE) is not None
        (root / "pipelines" / "fg2.yaml").write_text("flowgroup: fg2\npipeline: p\n")
        assert cache.load(TRIPLE) is None

    def test_removed_yaml_misses(self, cache, project):
        _, yaml_file, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        assert cache.load(TRIPLE) is not None
        yaml_file.unlink()
        assert cache.load(TRIPLE) is None

    def test_py_body_edit_misses(self, cache, project):
        """CORRECTNESS: editing an external transform body invalidates the
        cached graph even though NO YAML changed."""
        _, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        assert cache.load(TRIPLE) is not None
        time.sleep(0.01)
        body.write_text("def do_it(df, spark, parameters):\n    return df.limit(1)\n")
        assert cache.load(TRIPLE) is None

    def test_missing_body_misses(self, cache, project):
        _, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        assert cache.load(TRIPLE) is not None
        body.unlink()
        assert cache.load(TRIPLE) is None

    def test_corrupt_shard_returns_none(self, cache, project):
        _, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        shard = next((cache.cache_dir).glob("*.pkl"))
        shard.write_bytes(b"not a pickle at all")
        assert cache.load(TRIPLE) is None

    def test_wrong_payload_shape_returns_none(self, cache):
        cache.cache_dir.mkdir(parents=True, exist_ok=True)
        shard = cache.cache_dir / cache._shard_name(TRIPLE)
        shard.write_bytes(pickle.dumps({"key": ("x", TRIPLE)}))
        assert cache.load(TRIPLE) is None

    def test_version_bump_misses(self, project, monkeypatch):
        root, _, body = project
        cache_dir = root / ".lhp" / "cache" / "graph"
        old = PersistentGraphCache(cache_dir, root)
        old.save(TRIPLE, _result(), _bodies(body))
        assert old.load(TRIPLE) is not None
        # A manual CACHE_SCHEMA_VERSION bump changes the version tag (and thus
        # the shard filename prefix) so a new process ignores the old shard.
        monkeypatch.setattr(graph_cache_module, "CACHE_SCHEMA_VERSION", 999)
        bumped = PersistentGraphCache(cache_dir, root)
        assert bumped.load(TRIPLE) is None

    def test_distinct_triples_are_isolated(self, cache, project):
        _, _, body = project
        cache.save(TRIPLE, _result(edges=(("a", "b"),)), _bodies(body))
        other = ("pipeline_x", None, False)
        assert cache.load(other) is None
        cache.save(other, _result(edges=(("c", "d"),)), _bodies(body))
        assert sorted(cache.load(TRIPLE).graphs.action_graph.edges()) == [("a", "b")]
        assert sorted(cache.load(other).graphs.action_graph.edges()) == [("c", "d")]

    def test_sweep_removes_foreign_version_and_tmp(self, cache, project):
        _, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        live = next(cache.cache_dir.glob("*.pkl")).name
        # A shard from another LHP version (different filename prefix) + a
        # leftover temp file both get swept; the live shard survives.
        (cache.cache_dir / "deadbeefdeadbeef_stale.pkl").write_bytes(b"x")
        (cache.cache_dir / "orphan.tmp").write_bytes(b"x")
        cache.sweep()
        names = {p.name for p in cache.cache_dir.iterdir()}
        assert live in names
        assert "deadbeefdeadbeef_stale.pkl" not in names
        assert "orphan.tmp" not in names

    def test_unwritable_cache_dir_degrades(self, project):
        root, _, body = project
        disabled = PersistentGraphCache(Path("/dev/null/cannot/graph"), root)
        assert disabled._enabled is False
        disabled.save(TRIPLE, _result(), _bodies(body))  # no raise
        assert disabled.load(TRIPLE) is None


@pytest.mark.unit
class TestDescribe:
    """``describe`` surfaces cheap ``(built_at, fingerprint)`` metadata for the
    staleness poll — one ``os.stat``, never an unpickle."""

    def test_no_shard_reports_no_build_but_a_fingerprint(self, cache):
        built_at, fingerprint = cache.describe(TRIPLE)
        assert built_at is None
        assert fingerprint == cache._version_tag
        assert fingerprint  # non-empty

    def test_after_save_reports_iso_builtat_and_version_fingerprint(
        self, cache, project
    ):
        _, _, body = project
        cache.save(TRIPLE, _result(), _bodies(body))
        built_at, fingerprint = cache.describe(TRIPLE)
        assert built_at is not None and "T" in built_at  # ISO-8601
        assert fingerprint == cache._version_tag

    def test_disabled_cache_still_reports_version_tag(self, project):
        root = project[0]
        disabled = PersistentGraphCache(Path("/dev/null/cannot/graph"), root)
        built_at, fingerprint = disabled.describe(TRIPLE)
        assert built_at is None
        assert fingerprint == disabled._version_tag


def _seeded_service(root: Path, yaml_file: Path, body_rel: str, cache):
    """A service with discovery pre-seeded so ``analyze_project`` builds a
    real graph (reading the external ``.py`` body) without a full project."""
    project_config = ProjectConfig(name="t", version="1.0")
    validation_service = ValidationService(root, project_config)
    svc = DependencyAnalysisService(
        root,
        project_config,
        validation_service,
        persistent_graph_cache=cache,
    )
    fg = FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            Action(
                name="t_act",
                type=ActionType.TRANSFORM,
                transform_type="python",
                module_path=body_rel,
                function_name="do_it",
                source="v_in",
                target="v_out",
            )
        ],
    )
    svc._flowgroups = [fg]
    svc._flowgroup_file_paths = {"fg": yaml_file}
    return svc


@pytest.mark.integration
class TestAnalyzeProjectDiskCache:
    def test_hit_equals_fresh_build_without_rebuilding(self, project):
        root, yaml_file, _ = project
        cache_dir = root / ".lhp" / "cache" / "graph"
        body_rel = "transforms/t.py"

        svc1 = _seeded_service(
            root, yaml_file, body_rel, PersistentGraphCache(cache_dir, root)
        )
        fresh = svc1.analyze_project()

        svc2 = _seeded_service(
            root, yaml_file, body_rel, PersistentGraphCache(cache_dir, root)
        )

        def _no_build(*a, **k):
            raise AssertionError("disk HIT must not rebuild")

        svc2._builder.build_from_flowgroups = _no_build  # type: ignore[method-assign]
        hit = svc2.analyze_project()

        assert sorted(hit.graphs.action_graph.edges()) == sorted(
            fresh.graphs.action_graph.edges()
        )
        assert sorted(hit.graphs.action_graph.nodes()) == sorted(
            fresh.graphs.action_graph.nodes()
        )
        assert hit.external_sources == fresh.external_sources
        assert hit.execution_stages == fresh.execution_stages

    def test_rebuilds_after_body_edit(self, project):
        root, yaml_file, body = project
        cache_dir = root / ".lhp" / "cache" / "graph"
        body_rel = "transforms/t.py"

        _seeded_service(
            root, yaml_file, body_rel, PersistentGraphCache(cache_dir, root)
        ).analyze_project()

        time.sleep(0.01)
        body.write_text(
            "def do_it(df, spark, parameters):\n"
            "    return spark.read.table('cat.sch.new_dep')\n"
        )

        svc = _seeded_service(
            root, yaml_file, body_rel, PersistentGraphCache(cache_dir, root)
        )
        builds = {"n": 0}
        original = svc._builder.build_from_flowgroups

        def _count(*a, **k):
            builds["n"] += 1
            return original(*a, **k)

        svc._builder.build_from_flowgroups = _count  # type: ignore[method-assign]
        svc.analyze_project()
        assert builds["n"] == 1  # body edit -> disk MISS -> rebuild

    def test_force_rebuild_bypasses_disk_hit_and_repersists(self, project):
        root, yaml_file, _ = project
        cache_dir = root / ".lhp" / "cache" / "graph"
        body_rel = "transforms/t.py"

        # Warm the disk cache with a first build.
        _seeded_service(
            root, yaml_file, body_rel, PersistentGraphCache(cache_dir, root)
        ).analyze_project()

        # Fresh service over the warm cache: a normal analyze would be a disk
        # HIT (no build). force_rebuild must skip the load, rebuild, and save.
        cache = PersistentGraphCache(cache_dir, root)
        svc = _seeded_service(root, yaml_file, body_rel, cache)
        seeded = list(svc._flowgroups)

        # force_rebuild resets discovery; return the seeded flowgroups so the
        # forced rebuild still has a graph to build (this project has no real
        # on-disk actions to re-discover).
        def _fake_discover():
            svc._flowgroup_file_paths = {"fg": yaml_file}
            return seeded

        svc._discover_and_process_all_flowgroups = _fake_discover  # type: ignore[method-assign]

        loads: list = []
        original_load = cache.load

        def _count_load(key):
            loads.append(key)
            return original_load(key)

        cache.load = _count_load  # type: ignore[method-assign]

        saves: list = []
        original_save = cache.save

        def _count_save(*a, **k):
            saves.append(1)
            return original_save(*a, **k)

        cache.save = _count_save  # type: ignore[method-assign]

        builds = {"n": 0}
        original_build = svc._builder.build_from_flowgroups

        def _count_build(*a, **k):
            builds["n"] += 1
            return original_build(*a, **k)

        svc._builder.build_from_flowgroups = _count_build  # type: ignore[method-assign]

        svc.analyze_project(force_rebuild=True)

        assert loads == []  # disk load bypassed
        assert builds["n"] == 1  # forced fresh build
        assert saves == [1]  # result re-persisted

    def test_reset_discovery_state_clears_caches(self, project):
        root, yaml_file, _ = project
        svc = _seeded_service(
            root,
            yaml_file,
            "transforms/t.py",
            PersistentGraphCache(root / ".lhp" / "cache" / "graph", root),
        )
        svc._blueprint_provenance = {("p", "fg"): "x"}  # type: ignore[dict-item]
        svc._reset_discovery_state()
        assert svc._flowgroups is None
        assert svc._flowgroup_file_paths == {}
        assert svc._blueprint_provenance == {}

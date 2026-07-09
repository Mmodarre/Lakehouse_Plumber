"""Tests for :class:`PersistentParseCache` and its CachingYAMLParser seam.

Covers the on-disk shard round-trip, every invalidation axis (mtime_ns, size,
version tag), corruption / mkdir-failure degradation, the doc-only payload
upgrade path, sweep garbage collection, the error-files-never-get-shards
invariant, and a concurrent-write smoke (atomic replace = always a complete
payload).
"""

import pickle
import threading
from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.models import FlowGroup
from lhp.parsers import PersistentParseCache
from lhp.parsers.yaml_parser import CachingYAMLParser

_FLOWGROUP_YAML = """flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta_table
      path: test_table
"""


@pytest.fixture
def store(tmp_path):
    return PersistentParseCache(tmp_path / "cache" / "parse")


@pytest.fixture
def yaml_file(tmp_path):
    path = tmp_path / "pipelines" / "test_flowgroup.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(_FLOWGROUP_YAML)
    return path


def _stat_triplet(path: Path):
    resolved = path.resolve()
    st = resolved.stat()
    return resolved, st.st_mtime_ns, st.st_size


def _sample_payload():
    documents = [{"flowgroup": "fg", "pipeline": "pl", "actions": []}]
    flowgroups = [FlowGroup(pipeline="pl", flowgroup="fg", actions=[])]
    return documents, flowgroups


class TestPersistentParseCacheStore:
    def test_round_trip_hit(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents, flowgroups = _sample_payload()

        store.save(resolved, mtime_ns, size, documents, flowgroups)
        payload = store.load(resolved, mtime_ns, size)

        assert payload is not None
        assert payload["documents"] == documents
        assert len(payload["flowgroups"]) == 1
        assert payload["flowgroups"][0].flowgroup == "fg"

    def test_miss_on_changed_mtime_ns(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents, flowgroups = _sample_payload()

        store.save(resolved, mtime_ns, size, documents, flowgroups)
        assert store.load(resolved, mtime_ns + 1, size) is None

    def test_miss_on_changed_size(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents, flowgroups = _sample_payload()

        store.save(resolved, mtime_ns, size, documents, flowgroups)
        assert store.load(resolved, mtime_ns, size + 1) is None

    def test_miss_on_version_tag_change(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents, flowgroups = _sample_payload()

        store.save(resolved, mtime_ns, size, documents, flowgroups)
        assert store.load(resolved, mtime_ns, size) is not None

        # Simulate an LHP upgrade / CACHE_SCHEMA_VERSION bump / FlowGroup
        # model-shape change: the embedded key no longer matches.
        store._version_tag = "other-version|999|deadbeefdeadbeef"
        assert store.load(resolved, mtime_ns, size) is None

    def test_corrupt_shard_returns_none_then_rewrite_recovers(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents, flowgroups = _sample_payload()

        store.save(resolved, mtime_ns, size, documents, flowgroups)
        shard = store.cache_dir / store._shard_name(resolved)
        shard.write_bytes(b"this is not a pickle")

        assert store.load(resolved, mtime_ns, size) is None

        # A later successful parse rewrites the shard and recovers the cache.
        store.save(resolved, mtime_ns, size, documents, flowgroups)
        payload = store.load(resolved, mtime_ns, size)
        assert payload is not None
        assert payload["documents"] == documents

    def test_non_dict_pickle_payload_is_a_miss(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        shard = store.cache_dir / store._shard_name(resolved)
        shard.write_bytes(pickle.dumps(["not", "a", "payload"]))

        assert store.load(resolved, mtime_ns, size) is None

    def test_sweep_removes_orphans_and_tmp_files_keeps_live(self, store, tmp_path):
        live = tmp_path / "live.yaml"
        dead = tmp_path / "dead.yaml"
        for path in (live, dead):
            path.write_text(_FLOWGROUP_YAML)
        documents, flowgroups = _sample_payload()
        for path in (live, dead):
            resolved, mtime_ns, size = _stat_triplet(path)
            store.save(resolved, mtime_ns, size, documents, flowgroups)
        stray_tmp = store.cache_dir / "tmpabc123.tmp"
        stray_tmp.write_bytes(b"partial write leftover")
        unrelated = store.cache_dir / "README"
        unrelated.write_text("not ours to delete")

        store.sweep([live.resolve()])

        live_shard = store.cache_dir / store._shard_name(live.resolve())
        dead_shard = store.cache_dir / store._shard_name(dead.resolve())
        assert live_shard.exists()
        assert not dead_shard.exists()
        assert not stray_tmp.exists()
        assert unrelated.exists()

    def test_mkdir_failure_disables_store_without_raising(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("a file where a directory should be")

        store = PersistentParseCache(blocker / "cache" / "parse")

        documents, flowgroups = _sample_payload()
        # Every operation must be a silent no-op on the disabled store.
        store.save(Path("/nonexistent.yaml"), 1, 2, documents, flowgroups)
        assert store.load(Path("/nonexistent.yaml"), 1, 2) is None
        store.sweep([])
        assert not store._enabled

    def test_concurrent_writes_always_leave_a_complete_payload(self, store, yaml_file):
        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        errors = []

        def writer(worker_id: int):
            try:
                for i in range(50):
                    documents = [{"worker": worker_id, "iteration": i}]
                    store.save(resolved, mtime_ns, size, documents, None)
            except Exception as e:  # smoke test collects everything
                errors.append(e)

        def reader():
            try:
                for _ in range(100):
                    payload = store.load(resolved, mtime_ns, size)
                    # None (not yet written / lost race) or a complete payload.
                    if payload is not None:
                        assert set(payload) == {"key", "documents", "flowgroups"}
            except Exception as e:  # smoke test collects everything
                errors.append(e)

        threads = [threading.Thread(target=writer, args=(n,)) for n in range(2)]
        threads.append(threading.Thread(target=reader))
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        final = store.load(resolved, mtime_ns, size)
        assert final is not None
        assert final["documents"][0]["iteration"] == 49


class TestCachingParserPersistentSeam:
    def test_error_yaml_gets_no_shard_and_reraises_every_run(self, store, tmp_path):
        bad_file = tmp_path / "pipelines" / "empty.yaml"
        bad_file.parent.mkdir(parents=True)
        bad_file.write_text("")  # empty flowgroup file -> LHP-CFG-005

        parser = CachingYAMLParser(persistent_cache=store)
        with pytest.raises(LHPError):
            parser.parse_flowgroups_from_file(bad_file)
        assert list(store.cache_dir.glob("*.pkl")) == []

        # A brand-new parser sharing the store must fail identically: the
        # error was never cached, so the fresh parse re-raises.
        fresh_parser = CachingYAMLParser(persistent_cache=store)
        with pytest.raises(LHPError):
            fresh_parser.parse_flowgroups_from_file(bad_file)
        assert list(store.cache_dir.glob("*.pkl")) == []

    def test_doc_only_payload_upgrades_to_full_shard(self, store, yaml_file):
        from unittest.mock import patch

        import lhp.parsers.yaml_parser as yaml_parser_module

        resolved, mtime_ns, size = _stat_triplet(yaml_file)
        documents = yaml_parser_module.load_yaml_documents_all(yaml_file)
        store.save(resolved, mtime_ns, size, documents, None)

        parser = CachingYAMLParser(persistent_cache=store)
        reads = []

        def counting_load(file_path, *args, **kwargs):
            reads.append(str(file_path))
            return yaml_parser_module.load_yaml_documents_all(file_path)

        with patch.object(
            yaml_parser_module, "load_yaml_documents_all", side_effect=counting_load
        ):
            flowgroups = parser.parse_flowgroups_from_file(yaml_file)

        # Served from the cached documents: no physical read.
        assert reads == []
        assert len(flowgroups) == 1
        assert flowgroups[0].flowgroup == "test_flowgroup"

        # The shard was rewritten as a full payload (flowgroups populated).
        payload = store.load(resolved, mtime_ns, size)
        assert payload is not None
        assert payload["flowgroups"] is not None
        assert payload["flowgroups"][0].flowgroup == "test_flowgroup"

    def test_fresh_parse_writes_shard_and_warm_hits_across_parsers(
        self, store, yaml_file
    ):
        cold_parser = CachingYAMLParser(persistent_cache=store)
        cold_result = cold_parser.parse_flowgroups_from_file(yaml_file)
        assert len(list(store.cache_dir.glob("*.pkl"))) == 1

        warm_parser = CachingYAMLParser(persistent_cache=store)
        assert warm_parser.warm(yaml_file) is True
        warm_result = warm_parser.parse_flowgroups_from_file(yaml_file)
        assert [fg.flowgroup for fg in warm_result] == [
            fg.flowgroup for fg in cold_result
        ]

    def test_warm_misses_after_file_modification(self, store, yaml_file):
        parser = CachingYAMLParser(persistent_cache=store)
        parser.parse_flowgroups_from_file(yaml_file)

        yaml_file.write_text(_FLOWGROUP_YAML + "\n# modified\n")

        fresh_parser = CachingYAMLParser(persistent_cache=store)
        assert fresh_parser.warm(yaml_file) is False

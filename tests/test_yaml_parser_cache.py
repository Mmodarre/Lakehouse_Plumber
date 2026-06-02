"""Tests for CachingYAMLParser functionality."""

import tempfile
import time
from pathlib import Path

import pytest

from lhp.models import FlowGroup
from lhp.parsers.yaml_parser import CachingYAMLParser, YAMLParser


@pytest.fixture
def temp_yaml_file():
    """Create a temporary YAML file for testing."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_load
    type: load
    source:
      type: delta_table
      path: test_table
""")
        temp_path = Path(f.name)

    yield temp_path

    # Cleanup
    if temp_path.exists():
        temp_path.unlink()


class TestCachingYAMLParser:
    """Tests for CachingYAMLParser class."""

    def test_cache_initialization(self):
        """Test that CachingYAMLParser initializes correctly."""
        parser = CachingYAMLParser()
        assert parser._max_cache_size == 500
        assert parser._hits == 0
        assert len(parser._cache) == 0

    def test_cache_hit_on_second_read(self, temp_yaml_file):
        """Test that second read of same file hits cache."""
        parser = CachingYAMLParser()

        # First read - cold parse populates the flowgroup sub-cache (1 entry)
        # and the now-shared documents sub-cache; no hit yet.
        flowgroups1 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(flowgroups1) == 1
        assert len(parser._cache) == 1
        assert parser._hits == 0

        # Second read - should be cache hit (no descent into documents cache,
        # no new flowgroup-cache entry).
        flowgroups2 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(flowgroups2) == 1
        assert len(parser._cache) == 1
        assert parser._hits == 1

        # Verify same objects returned (from cache)
        assert flowgroups1[0].flowgroup == flowgroups2[0].flowgroup

    def test_cache_invalidation_on_file_modification(self, temp_yaml_file):
        """Test that cache is invalidated when file is modified."""
        parser = CachingYAMLParser()

        # First read - cold parse, one flowgroup-cache entry, no hit
        flowgroups1 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(parser._cache) == 1
        assert parser._hits == 0

        # Modify file (change mtime)
        time.sleep(0.01)  # Ensure different mtime
        with open(temp_yaml_file, "a") as f:
            f.write("\n# Modified\n")

        # Second read after modification - the new mtime key misses the cache
        # and re-loads under a fresh key (no hit, a second resident entry).
        flowgroups2 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(parser._cache) == 2
        assert parser._hits == 0

    def test_cache_eviction_on_size_limit(self):
        """Test that cache evicts old entries when size limit is reached."""
        parser = CachingYAMLParser(max_cache_size=10)

        # Create and cache 12 files (exceeds limit of 10)
        temp_files = []
        try:
            for i in range(12):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yaml", delete=False
                ) as f:
                    f.write(f"""flowgroup: test_flowgroup_{i}
pipeline: test_pipeline
actions:
  - name: test_action_{i}
    type: load
    source:
      type: delta_table
      path: test_table
""")
                    temp_path = Path(f.name)
                    temp_files.append(temp_path)
                # Parse after file is closed
                parser.parse_flowgroups_from_file(temp_path)

            # Cache should have evicted oldest entries
            assert len(parser._cache) <= 10

        finally:
            # Cleanup
            for temp_file in temp_files:
                if temp_file.exists():
                    temp_file.unlink()

    def test_reserve_capacity_grows_only(self):
        """reserve_capacity raises the cap monotonically and is idempotent.

        The parser is a single shared instance and both discovery passes call
        reserve_capacity with their own glob size; a monotonic max guarantees a
        later, smaller reserve can never shrink the ceiling mid-workload.
        """
        parser = CachingYAMLParser(max_cache_size=10)
        assert parser._max_cache_size == 10

        # Grow: reserving above the current cap raises it.
        parser.reserve_capacity(15)
        assert parser._max_cache_size == 15

        # Grow-only: a smaller reserve is a no-op, not a shrink.
        parser.reserve_capacity(5)
        assert parser._max_cache_size == 15

        # Idempotent: reserving the same value again leaves it unchanged.
        parser.reserve_capacity(15)
        assert parser._max_cache_size == 15

    def test_reserve_capacity_prevents_eviction(self):
        """With the working set reserved, no entry is evicted and no file is
        read twice — the production fix reproduced at the unit level.

        Starts below the working set (cap 10 < 15 files) so that WITHOUT the
        reserve the documents sub-cache would evict on the 11th distinct file.
        After reserving 15, all 15 distinct files stay resident
        (documents_cache_size == 15) and each file is physically read exactly
        once even on a repeat load pass.
        """
        from unittest.mock import patch

        import lhp.parsers.yaml_parser as yaml_parser_module

        parser = CachingYAMLParser(max_cache_size=10)
        parser.reserve_capacity(15)
        assert parser._max_cache_size == 15

        # Spy on the physical read (the binding the active read path uses;
        # see tests/core/discovery/test_single_parse_pass.py). wraps= keeps
        # real parsing so behavior is unchanged.
        real_load = yaml_parser_module.load_yaml_documents_all
        read_counter: dict = {}

        def counting_load(file_path, *args, **kwargs):
            key = str(Path(file_path).resolve())
            read_counter[key] = read_counter.get(key, 0) + 1
            return real_load(file_path, *args, **kwargs)

        temp_files = []
        try:
            for i in range(15):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yaml", delete=False
                ) as f:
                    f.write(f"flowgroup: fg_{i}\npipeline: pl\nactions: []\n")
                    temp_path = Path(f.name)
                    temp_files.append(temp_path)

            with patch.object(
                yaml_parser_module,
                "load_yaml_documents_all",
                side_effect=counting_load,
            ):
                # First pass over all 15 files populates the documents cache.
                for temp_file in temp_files:
                    parser.load_documents_all(temp_file)
                # Second pass: with no eviction, every file is a cache hit and
                # is NOT read from disk again.
                for temp_file in temp_files:
                    parser.load_documents_all(temp_file)

            # No eviction: the documents sub-cache holds the full working set.
            assert len(parser._documents_cache) == 15

            # Each distinct file was physically read exactly once across both
            # passes (the cap-10 default would have evicted and re-read).
            assert len(read_counter) == 15
            assert all(count == 1 for count in read_counter.values()), read_counter
        finally:
            for temp_file in temp_files:
                if temp_file.exists():
                    temp_file.unlink()

    def test_delegation_to_base_parser(self, temp_yaml_file):
        """Test that other methods are delegated to base parser."""
        parser = CachingYAMLParser()

        # Test that parse_file is delegated
        content = parser.parse_file(temp_yaml_file)
        assert isinstance(content, dict)
        assert "flowgroup" in content

    def test_thread_safety(self, temp_yaml_file):
        """Test that cache is thread-safe."""
        import threading

        parser = CachingYAMLParser()
        results = []
        errors = []

        def read_file():
            try:
                flowgroups = parser.parse_flowgroups_from_file(temp_yaml_file)
                results.append(flowgroups)
            except Exception as e:
                errors.append(e)

        # Create multiple threads reading same file
        threads = [threading.Thread(target=read_file) for _ in range(10)]

        # Start all threads
        for thread in threads:
            thread.start()

        # Wait for all threads
        for thread in threads:
            thread.join()

        # Verify no errors and all reads succeeded
        assert len(errors) == 0
        assert len(results) == 10

        # Verify cache stats show hits (thread-safe operations).
        # 10 threads over one file = 1 cold parse (one flowgroup-cache entry)
        # + 9 flowgroup-cache hits.
        assert len(parser._cache) == 1
        assert parser._hits == 9

    # ------------------------------------------------------------------
    # load_documents_all (raw-document sub-cache)
    # ------------------------------------------------------------------

    def test_load_documents_all_caches_by_mtime(self, temp_yaml_file):
        """Two calls on an unchanged file: first miss, second hit."""
        parser = CachingYAMLParser()

        documents1 = parser.load_documents_all(temp_yaml_file)
        assert len(documents1) == 1
        assert documents1[0]["flowgroup"] == "test_flowgroup"
        assert parser._hits == 0
        assert len(parser._documents_cache) == 1

        documents2 = parser.load_documents_all(temp_yaml_file)
        assert documents2 is documents1  # same cached list object
        assert len(parser._documents_cache) == 1
        assert parser._hits == 1

    def test_load_documents_all_invalidates_on_mtime_change(self, temp_yaml_file):
        """Editing the file must invalidate the cache entry and re-load."""
        parser = CachingYAMLParser()

        parser.load_documents_all(temp_yaml_file)
        assert len(parser._documents_cache) == 1

        time.sleep(0.01)
        with open(temp_yaml_file, "a") as f:
            f.write("\n# Modified\n")

        # New mtime key misses and re-loads under a fresh key (second entry).
        parser.load_documents_all(temp_yaml_file)
        assert len(parser._documents_cache) == 2
        assert parser._hits == 0

    def test_load_documents_all_eviction(self):
        """Cache evicts ~10% of oldest entries when max size is exceeded."""
        parser = CachingYAMLParser(max_cache_size=10)

        temp_files = []
        try:
            for i in range(12):
                with tempfile.NamedTemporaryFile(
                    mode="w", suffix=".yaml", delete=False
                ) as f:
                    f.write(f"flowgroup: fg_{i}\npipeline: pl\nactions: []\n")
                    temp_path = Path(f.name)
                    temp_files.append(temp_path)
                parser.load_documents_all(temp_path)

            assert len(parser._documents_cache) <= 10
        finally:
            for temp_file in temp_files:
                if temp_file.exists():
                    temp_file.unlink()

    def test_load_documents_all_falls_through_on_oserror(
        self, temp_yaml_file, monkeypatch
    ):
        """If stat() raises OSError, fall through to the uncached loader
        rather than crashing. Result must still match the direct loader.
        """
        parser = CachingYAMLParser()

        # Force every Path.stat() to raise OSError. The cache key cannot be
        # computed; the parser must fall back to calling the loader directly.
        real_stat = Path.stat

        def fail_stat(self, *args, **kwargs):
            raise OSError("simulated stat failure")

        monkeypatch.setattr(Path, "stat", fail_stat)

        documents = parser.load_documents_all(temp_yaml_file)
        assert len(documents) == 1
        assert documents[0]["flowgroup"] == "test_flowgroup"
        # Nothing should have been cached (stat failed before we could key it).
        assert len(parser._documents_cache) == 0
        assert parser._hits == 0

        # Restore for cleanup of the temp_yaml_file fixture
        monkeypatch.setattr(Path, "stat", real_stat)

    def test_load_documents_all_thread_safety(self, temp_yaml_file):
        """Concurrent load_documents_all calls must not corrupt counters."""
        import threading

        parser = CachingYAMLParser()
        results = []
        errors = []

        def read_file():
            try:
                documents = parser.load_documents_all(temp_yaml_file)
                results.append(documents)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=read_file) for _ in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert len(errors) == 0
        assert len(results) == 10

        # All 10 calls hit the same key; cache must hold exactly one entry.
        # 1 cold load (the single entry) + 9 hits.
        assert len(parser._documents_cache) == 1
        assert parser._hits == 9

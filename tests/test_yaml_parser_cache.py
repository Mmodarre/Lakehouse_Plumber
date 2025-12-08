"""Tests for CachingYAMLParser functionality."""

import tempfile
import time
from pathlib import Path
import pytest

from lhp.parsers.yaml_parser import YAMLParser, CachingYAMLParser
from lhp.models.config import FlowGroup


@pytest.fixture
def temp_yaml_file():
    """Create a temporary YAML file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
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
        assert parser._misses == 0
        assert len(parser._cache) == 0
    
    def test_cache_hit_on_second_read(self, temp_yaml_file):
        """Test that second read of same file hits cache."""
        parser = CachingYAMLParser()
        
        # First read - should be cache miss
        flowgroups1 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(flowgroups1) == 1
        assert parser._misses == 1
        assert parser._hits == 0
        
        # Second read - should be cache hit
        flowgroups2 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(flowgroups2) == 1
        assert parser._misses == 1
        assert parser._hits == 1
        
        # Verify same objects returned (from cache)
        assert flowgroups1[0].flowgroup == flowgroups2[0].flowgroup
    
    def test_cache_invalidation_on_file_modification(self, temp_yaml_file):
        """Test that cache is invalidated when file is modified."""
        parser = CachingYAMLParser()
        
        # First read
        flowgroups1 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert parser._misses == 1
        assert parser._hits == 0
        
        # Modify file (change mtime)
        time.sleep(0.01)  # Ensure different mtime
        with open(temp_yaml_file, 'a') as f:
            f.write("\n# Modified\n")
        
        # Second read after modification - should be cache miss
        flowgroups2 = parser.parse_flowgroups_from_file(temp_yaml_file)
        assert parser._misses == 2
        assert parser._hits == 0
    
    def test_cache_stats(self, temp_yaml_file):
        """Test cache statistics reporting."""
        parser = CachingYAMLParser()
        
        # Read once
        parser.parse_flowgroups_from_file(temp_yaml_file)
        stats = parser.get_cache_stats()
        assert stats['hits'] == 0
        assert stats['misses'] == 1
        assert stats['total'] == 1
        assert stats['hit_rate_percent'] == 0.0
        assert stats['cache_size'] == 1
        
        # Read again (cache hit)
        parser.parse_flowgroups_from_file(temp_yaml_file)
        stats = parser.get_cache_stats()
        assert stats['hits'] == 1
        assert stats['misses'] == 1
        assert stats['total'] == 2
        assert stats['hit_rate_percent'] == 50.0
        assert stats['cache_size'] == 1
    
    def test_cache_clear(self, temp_yaml_file):
        """Test that cache can be cleared."""
        parser = CachingYAMLParser()
        
        # Read and cache
        parser.parse_flowgroups_from_file(temp_yaml_file)
        assert len(parser._cache) == 1
        
        # Clear cache
        parser.clear_cache()
        assert len(parser._cache) == 0
        assert parser._hits == 0
        assert parser._misses == 0
    
    def test_cache_eviction_on_size_limit(self):
        """Test that cache evicts old entries when size limit is reached."""
        parser = CachingYAMLParser(max_cache_size=10)
        
        # Create and cache 12 files (exceeds limit of 10)
        temp_files = []
        try:
            for i in range(12):
                with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
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
    
    def test_delegation_to_base_parser(self, temp_yaml_file):
        """Test that other methods are delegated to base parser."""
        parser = CachingYAMLParser()
        
        # Test that parse_file is delegated
        content = parser.parse_file(temp_yaml_file)
        assert isinstance(content, dict)
        assert 'flowgroup' in content
    
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
        
        # Verify cache stats show hits (thread-safe operations)
        stats = parser.get_cache_stats()
        assert stats['total'] == 10
        assert stats['hits'] + stats['misses'] == 10


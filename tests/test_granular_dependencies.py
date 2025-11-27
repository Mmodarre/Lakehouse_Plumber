"""Tests for granular global dependency tracking."""

from pathlib import Path
import pytest

from lhp.utils.substitution import EnhancedSubstitutionManager
from lhp.core.state_models import FileState


class TestGranularDependencyTracking:
    """Tests for substitution key access tracking."""
    
    def test_tracking_context_initialization(self):
        """Test that tracking context can be set."""
        mgr = EnhancedSubstitutionManager()
        
        mgr.set_tracking_context("test_flowgroup")
        # Thread-local storage - access via thread_local
        assert getattr(mgr._thread_local, 'current_flowgroup', None) == "test_flowgroup"
        assert "test_flowgroup" in mgr._accessed_keys
    
    def test_clear_tracking_context(self):
        """Test that tracking context can be cleared."""
        mgr = EnhancedSubstitutionManager()
        
        mgr.set_tracking_context("test_flowgroup")
        mgr.clear_tracking_context()
        # Thread-local storage - access via thread_local
        assert getattr(mgr._thread_local, 'current_flowgroup', None) is None
    
    def test_key_access_tracking(self):
        """Test that accessed keys are tracked."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {
            "catalog": "main",
            "schema": "bronze",
            "table": "customers"
        }
        
        mgr.set_tracking_context("test_flowgroup")
        
        # Process string with substitutions
        text = "Use {catalog}.{schema}.{table} table"
        result = mgr._replace_tokens_in_string(text)
        
        # Verify keys were tracked
        accessed_keys = mgr.get_accessed_keys("test_flowgroup")
        assert "catalog" in accessed_keys
        assert "schema" in accessed_keys
        assert "table" in accessed_keys
    
    def test_dollar_token_tracking(self):
        """Test that ${TOKEN} patterns are tracked."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {
            "env": "dev",
            "bucket": "my-bucket"
        }
        
        mgr.set_tracking_context("test_flowgroup")
        
        # Process string with ${} syntax
        text = "s3://${bucket}/data/${env}/"
        result = mgr._replace_tokens_in_string(text)
        
        # Verify keys were tracked
        accessed_keys = mgr.get_accessed_keys("test_flowgroup")
        assert "env" in accessed_keys
        assert "bucket" in accessed_keys
    
    def test_no_tracking_without_context(self):
        """Test that keys are not tracked without context."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {"catalog": "main"}
        
        # Don't set tracking context
        text = "Use {catalog}"
        result = mgr._replace_tokens_in_string(text)
        
        # No keys should be tracked
        accessed_keys = mgr.get_accessed_keys("test_flowgroup")
        assert len(accessed_keys) == 0
    
    def test_multiple_flowgroups_tracked_separately(self):
        """Test that different flowgroups track keys separately."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {
            "catalog": "main",
            "schema": "bronze",
            "table": "customers"
        }
        
        # Track flowgroup 1
        mgr.set_tracking_context("flowgroup_1")
        text1 = "{catalog}.{schema}"
        mgr._replace_tokens_in_string(text1)
        
        # Track flowgroup 2
        mgr.set_tracking_context("flowgroup_2")
        text2 = "{table}"
        mgr._replace_tokens_in_string(text2)
        
        # Verify separate tracking
        keys1 = mgr.get_accessed_keys("flowgroup_1")
        keys2 = mgr.get_accessed_keys("flowgroup_2")
        
        assert "catalog" in keys1
        assert "schema" in keys1
        assert "table" not in keys1
        
        assert "table" in keys2
        assert "catalog" not in keys2
    
    def test_file_state_stores_used_keys(self):
        """Test that FileState can store used substitution keys."""
        file_state = FileState(
            source_yaml="test.yaml",
            generated_path="test.py",
            checksum="abc123",
            source_yaml_checksum="def456",
            timestamp="2024-01-01T12:00:00",
            environment="dev",
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            used_substitution_keys=["catalog", "schema", "table"]
        )
        
        assert file_state.used_substitution_keys == ["catalog", "schema", "table"]
    
    def test_file_state_without_used_keys(self):
        """Test that FileState works without used_substitution_keys (backward compat)."""
        file_state = FileState(
            source_yaml="test.yaml",
            generated_path="test.py",
            checksum="abc123",
            source_yaml_checksum="def456",
            timestamp="2024-01-01T12:00:00",
            environment="dev",
            pipeline="test_pipeline",
            flowgroup="test_flowgroup"
        )
        
        assert file_state.used_substitution_keys is None
    
    def test_recursive_token_expansion_tracking(self):
        """Test that recursive token expansion tracks all accessed keys."""
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {
            "base_path": "s3://my-bucket",
            "data_path": "{base_path}/data",
            "table_path": "{data_path}/tables"
        }
        
        # Expand recursive tokens
        mgr._expand_recursive_tokens()
        
        mgr.set_tracking_context("test_flowgroup")
        
        # Access top-level token
        text = "{table_path}"
        result = mgr._replace_tokens_in_string(text)
        
        # Should track the accessed token
        accessed_keys = mgr.get_accessed_keys("test_flowgroup")
        assert "table_path" in accessed_keys
    
    def test_thread_safety_concurrent_tracking(self):
        """Test that concurrent tracking from multiple threads works correctly."""
        import threading
        
        mgr = EnhancedSubstitutionManager()
        mgr.mappings = {
            "catalog": "main",
            "schema": "bronze", 
            "table": "customers"
        }
        
        results = {}
        errors = []
        
        def track_flowgroup(flowgroup_name, tokens_to_use):
            try:
                mgr.set_tracking_context(flowgroup_name)
                for token in tokens_to_use:
                    text = f"{{{token}}}"
                    mgr._replace_tokens_in_string(text)
                mgr.clear_tracking_context()
                
                # Store what this thread tracked
                results[flowgroup_name] = mgr.get_accessed_keys(flowgroup_name)
            except Exception as e:
                errors.append((flowgroup_name, e))
        
        # Create threads tracking different flowgroups
        threads = [
            threading.Thread(target=track_flowgroup, args=("fg1", ["catalog", "schema"])),
            threading.Thread(target=track_flowgroup, args=("fg2", ["table"])),
            threading.Thread(target=track_flowgroup, args=("fg3", ["catalog"])),
            threading.Thread(target=track_flowgroup, args=("fg4", ["schema", "table"])),
        ]
        
        # Start all threads
        for t in threads:
            t.start()
        
        # Wait for all threads
        for t in threads:
            t.join()
        
        # Verify no errors
        assert len(errors) == 0, f"Errors occurred: {errors}"
        
        # Verify correct tracking per flowgroup
        assert "catalog" in results["fg1"]
        assert "schema" in results["fg1"]
        assert "table" not in results["fg1"]
        
        assert "table" in results["fg2"]
        assert "catalog" not in results["fg2"]
        
        assert "catalog" in results["fg3"]
        assert "schema" not in results["fg3"]
        
        assert "schema" in results["fg4"]
        assert "table" in results["fg4"]


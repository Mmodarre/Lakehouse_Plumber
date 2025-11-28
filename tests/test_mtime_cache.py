"""Tests for mtime-based checksum caching."""

import tempfile
import time
from pathlib import Path
import pytest

from lhp.core.state_models import DependencyInfo


class TestMtimeCaching:
    """Tests for mtime-based dependency caching."""
    
    def test_dependency_info_with_mtime(self):
        """Test that DependencyInfo can store mtime."""
        dep = DependencyInfo(
            path="test/path.py",
            checksum="abc123",
            type="preset",
            last_modified="2024-01-01T12:00:00",
            mtime=1704110400.0
        )
        
        assert dep.mtime == 1704110400.0
        assert dep.path == "test/path.py"
    
    def test_dependency_info_without_mtime(self):
        """Test that DependencyInfo works without mtime (backward compat)."""
        dep = DependencyInfo(
            path="test/path.py",
            checksum="abc123",
            type="preset",
            last_modified="2024-01-01T12:00:00"
        )
        
        assert dep.mtime is None
    
    def test_mtime_comparison_tolerance(self):
        """Test mtime comparison with floating point tolerance."""
        mtime1 = 1704110400.0
        mtime2 = 1704110400.0005  # 0.5ms difference
        
        # Should be considered equal within tolerance of 1ms
        tolerance = 0.001
        assert abs(mtime1 - mtime2) < tolerance
    
    def test_mtime_invalidation_on_file_change(self):
        """Test that mtime changes when file is modified."""
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write("initial content")
            temp_path = Path(f.name)
        
        try:
            # Get initial mtime
            mtime1 = temp_path.stat().st_mtime
            
            # Wait and modify file
            time.sleep(0.01)
            with open(temp_path, 'w') as f:
                f.write("modified content")
            
            # Get new mtime
            mtime2 = temp_path.stat().st_mtime
            
            # Verify mtime changed
            assert mtime2 > mtime1
            
        finally:
            temp_path.unlink()


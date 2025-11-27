"""Tests for SmartFileWriter checksum optimization."""

import tempfile
from pathlib import Path
import pytest

from lhp.utils.smart_file_writer import SmartFileWriter


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


class TestSmartFileWriterChecksumOptimization:
    """Tests for SmartFileWriter checksum-based optimization."""
    
    def test_checksum_fast_path_skip(self, temp_dir):
        """Test that matching checksum skips file without reading."""
        writer = SmartFileWriter()
        file_path = temp_dir / "test.py"
        content = "def test():\n    pass\n"
        
        # Write file initially
        result1 = writer.write_if_changed(file_path, content)
        assert result1 is True
        assert writer.files_written == 1
        
        # Calculate checksum of content
        checksum = writer._calculate_checksum(writer._normalize_content(content))
        
        # Try to write same content with checksum - should skip
        result2 = writer.write_if_changed(file_path, content, stored_checksum=checksum)
        assert result2 is False
        assert writer.files_skipped == 1
        assert writer.files_written == 1  # Unchanged
    
    def test_checksum_mismatch_writes(self, temp_dir):
        """Test that mismatched checksum causes write."""
        writer = SmartFileWriter()
        file_path = temp_dir / "test.py"
        
        # Write initial content
        content1 = "def test():\n    pass\n"
        writer.write_if_changed(file_path, content1)
        
        # Try to write different content with old checksum
        content2 = "def test():\n    return 42\n"
        old_checksum = writer._calculate_checksum(writer._normalize_content(content1))
        
        result = writer.write_if_changed(file_path, content2, stored_checksum=old_checksum)
        assert result is True
        assert writer.files_written == 2
        
        # Verify new content was written
        assert file_path.read_text() == "def test():\n    return 42\n"
    
    def test_checksum_without_stored_uses_content_comparison(self, temp_dir):
        """Test that without stored checksum, falls back to content comparison."""
        writer = SmartFileWriter()
        file_path = temp_dir / "test.py"
        content = "def test():\n    pass\n"
        
        # Write file initially
        writer.write_if_changed(file_path, content)
        
        # Try to write same content without checksum
        result = writer.write_if_changed(file_path, content, stored_checksum=None)
        assert result is False  # Should skip via content comparison
        assert writer.files_skipped == 1
    
    def test_checksum_with_nonexistent_file(self, temp_dir):
        """Test that checksum with nonexistent file writes anyway."""
        writer = SmartFileWriter()
        file_path = temp_dir / "test.py"
        content = "def test():\n    pass\n"
        
        # Try to write with checksum when file doesn't exist
        fake_checksum = "abc123"
        result = writer.write_if_changed(file_path, content, stored_checksum=fake_checksum)
        assert result is True
        assert writer.files_written == 1
    
    def test_calculate_checksum(self):
        """Test checksum calculation."""
        writer = SmartFileWriter()
        
        content1 = "def test():\n    pass\n"
        content2 = "def test():\n    pass\n"
        content3 = "def test():\n    return 42\n"
        
        checksum1 = writer._calculate_checksum(content1)
        checksum2 = writer._calculate_checksum(content2)
        checksum3 = writer._calculate_checksum(content3)
        
        # Same content should have same checksum
        assert checksum1 == checksum2
        
        # Different content should have different checksum
        assert checksum1 != checksum3
        
        # Checksums should be hex strings
        assert isinstance(checksum1, str)
        assert len(checksum1) == 64  # SHA256 produces 64 hex chars
    
    def test_thread_safety_of_counters(self, temp_dir):
        """Test that counter increments are thread-safe."""
        import threading
        
        writer = SmartFileWriter()
        
        def write_file(i):
            file_path = temp_dir / f"test{i}.py"
            content = f"def test{i}():\n    pass\n"
            writer.write_if_changed(file_path, content)
        
        # Create multiple threads writing files
        threads = [threading.Thread(target=write_file, args=(i,)) for i in range(10)]
        
        # Start all threads
        for thread in threads:
            thread.start()
        
        # Wait for all threads
        for thread in threads:
            thread.join()
        
        # Verify counter is correct
        assert writer.files_written == 10
        assert writer.files_skipped == 0


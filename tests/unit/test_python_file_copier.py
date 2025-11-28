"""Unit tests for thread-safe Python file copier."""

import pytest
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
import shutil

from lhp.generators.transform.python_file_copier import (
    PythonFileCopier,
    PythonFunctionConflictError
)


class TestPythonFileCopier:
    """Test suite for PythonFileCopier class."""
    
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield Path(temp)
        shutil.rmtree(temp, ignore_errors=True)
    
    @pytest.fixture
    def copier(self):
        """Create a PythonFileCopier instance."""
        return PythonFileCopier()
    
    def test_single_file_copy(self, copier, temp_dir):
        """Test copying a single file."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"
        
        result = copier.copy_python_file(source_path, dest_path, content)
        
        assert result is True
        assert dest_path.exists()
        assert dest_path.read_text() == content
    
    def test_duplicate_same_source(self, copier, temp_dir):
        """Test that copying the same source twice skips the second copy."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"
        
        # First copy
        result1 = copier.copy_python_file(source_path, dest_path, content)
        assert result1 is True
        
        # Second copy (should skip)
        result2 = copier.copy_python_file(source_path, dest_path, content)
        assert result2 is False
    
    def test_conflict_different_sources(self, copier, temp_dir):
        """Test that different sources targeting same destination raises error."""
        dest_path = temp_dir / "test_module.py"
        source_path1 = "py_functions/test_module.py"
        source_path2 = "other_functions/test_module.py"
        content = "# Test content"
        
        # First copy
        copier.copy_python_file(source_path1, dest_path, content)
        
        # Second copy from different source should raise error
        with pytest.raises(PythonFunctionConflictError) as exc_info:
            copier.copy_python_file(source_path2, dest_path, content)
        
        assert source_path1 in str(exc_info.value)
        assert source_path2 in str(exc_info.value)
    
    def test_concurrent_same_source(self, copier, temp_dir):
        """Test concurrent access from multiple threads with same source."""
        dest_path = temp_dir / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content\ndef test_func():\n    pass"
        
        results = []
        
        def copy_file():
            return copier.copy_python_file(source_path, dest_path, content)
        
        # Run 10 concurrent copies
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(copy_file) for _ in range(10)]
            for future in as_completed(futures):
                results.append(future.result())
        
        # Exactly one should return True (actual copy), rest should return False (skipped)
        assert sum(results) == 1
        assert len(results) == 10
        assert dest_path.exists()
        assert dest_path.read_text() == content
    
    def test_concurrent_different_files(self, copier, temp_dir):
        """Test concurrent copying of different files."""
        files = [
            ("py_functions/module1.py", temp_dir / "module1.py", "# Module 1"),
            ("py_functions/module2.py", temp_dir / "module2.py", "# Module 2"),
            ("py_functions/module3.py", temp_dir / "module3.py", "# Module 3"),
        ]
        
        def copy_file(source, dest, content):
            return copier.copy_python_file(source, dest, content)
        
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(copy_file, source, dest, content)
                for source, dest, content in files
            ]
            results = [future.result() for future in as_completed(futures)]
        
        # All should succeed
        assert all(results)
        assert all(dest.exists() for _, dest, _ in files)
    
    def test_ensure_init_file(self, copier, temp_dir):
        """Test creating __init__.py file."""
        custom_dir = temp_dir / "custom_python_functions"
        
        copier.ensure_init_file(custom_dir)
        
        assert custom_dir.exists()
        assert (custom_dir / "__init__.py").exists()
        assert "Generated package" in (custom_dir / "__init__.py").read_text()
    
    def test_ensure_init_file_concurrent(self, copier, temp_dir):
        """Test concurrent __init__.py creation."""
        custom_dir = temp_dir / "custom_python_functions"
        
        def create_init():
            copier.ensure_init_file(custom_dir)
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(create_init) for _ in range(5)]
            for future in as_completed(futures):
                future.result()  # Wait for completion
        
        # Should only create once
        assert custom_dir.exists()
        assert (custom_dir / "__init__.py").exists()
    
    def test_get_copied_files(self, copier, temp_dir):
        """Test retrieving copied files registry."""
        dest1 = temp_dir / "module1.py"
        dest2 = temp_dir / "module2.py"
        source1 = "py_functions/module1.py"
        source2 = "py_functions/module2.py"
        
        copier.copy_python_file(source1, dest1, "# Module 1")
        copier.copy_python_file(source2, dest2, "# Module 2")
        
        copied = copier.get_copied_files()
        
        assert len(copied) == 2
        assert str(dest1) in copied
        assert str(dest2) in copied
        assert copied[str(dest1)] == source1
        assert copied[str(dest2)] == source2
    
    def test_error_attributes(self, copier, temp_dir):
        """Test PythonFunctionConflictError has correct attributes."""
        dest_path = temp_dir / "test_module.py"
        source1 = "py_functions/test_module.py"
        source2 = "other_functions/test_module.py"
        content = "# Test"
        
        copier.copy_python_file(source1, dest_path, content)
        
        try:
            copier.copy_python_file(source2, dest_path, content)
            assert False, "Should have raised error"
        except PythonFunctionConflictError as e:
            assert e.destination == str(dest_path)
            assert e.existing_source == source1
            assert e.new_source == source2
    
    def test_thread_safety_stress(self, copier, temp_dir):
        """Stress test with many concurrent operations."""
        # Create 20 files, each copied by 5 threads
        num_files = 20
        copies_per_file = 5
        
        results = []
        errors = []
        
        def copy_file(file_id, thread_id):
            try:
                dest = temp_dir / f"module_{file_id}.py"
                source = f"py_functions/module_{file_id}.py"
                content = f"# Module {file_id} from thread {thread_id}"
                return copier.copy_python_file(source, dest, content)
            except Exception as e:
                errors.append(e)
                return False
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = []
            for file_id in range(num_files):
                for thread_id in range(copies_per_file):
                    futures.append(executor.submit(copy_file, file_id, thread_id))
            
            for future in as_completed(futures):
                results.append(future.result())
        
        # No errors should occur
        assert len(errors) == 0
        
        # Each file should be copied exactly once
        copied = copier.get_copied_files()
        assert len(copied) == num_files
        
        # Exactly one True per file (actual copy), rest False (skipped)
        true_count = sum(results)
        assert true_count == num_files
    
    def test_parent_directory_creation(self, copier, temp_dir):
        """Test that parent directories are created if needed."""
        dest_path = temp_dir / "nested" / "deep" / "test_module.py"
        source_path = "py_functions/test_module.py"
        content = "# Test content"
        
        copier.copy_python_file(source_path, dest_path, content)
        
        assert dest_path.exists()
        assert dest_path.parent.exists()
        assert dest_path.read_text() == content


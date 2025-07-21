"""
TEMPORARY: Tests for Databricks notebook headers functionality.

These tests should be REMOVED when the temporary functionality is removed.
Search for "temporary_databricks_headers" to find all references for removal.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, Mock

from lhp.bundle.temporary_databricks_headers import (
    add_databricks_notebook_headers,
    _find_python_files,
    _has_databricks_header,
    _add_databricks_header,
    DATABRICKS_HEADER
)


class TestTemporaryDatabricksHeaders:
    """TEMPORARY: Test suite for Databricks headers functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.generated_dir = self.temp_dir / "generated"
        self.generated_dir.mkdir()

    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)

    def test_find_python_files(self):
        """Should find all Python files in directory tree."""
        # Create test structure
        (self.generated_dir / "pipeline1").mkdir()
        (self.generated_dir / "pipeline1" / "flow1.py").write_text("# Python file 1")
        (self.generated_dir / "pipeline1" / "flow2.py").write_text("# Python file 2")
        (self.generated_dir / "pipeline2").mkdir()
        (self.generated_dir / "pipeline2" / "flow3.py").write_text("# Python file 3")
        (self.generated_dir / "README.md").write_text("# Not Python")

        python_files = _find_python_files(self.generated_dir)
        
        assert len(python_files) == 3
        assert all(f.suffix == ".py" for f in python_files)
        assert all(f.is_file() for f in python_files)

    def test_has_databricks_header_true(self):
        """Should detect existing Databricks header."""
        test_file = self.generated_dir / "test.py"
        test_file.write_text(f"{DATABRICKS_HEADER}\nprint('hello')")
        
        assert _has_databricks_header(test_file) is True

    def test_has_databricks_header_false(self):
        """Should detect missing Databricks header."""
        test_file = self.generated_dir / "test.py"
        test_file.write_text("print('hello')")
        
        assert _has_databricks_header(test_file) is False

    def test_has_databricks_header_empty_file(self):
        """Should handle empty files gracefully."""
        test_file = self.generated_dir / "empty.py"
        test_file.write_text("")
        
        assert _has_databricks_header(test_file) is False

    def test_add_databricks_header_success(self):
        """Should add header to file without one."""
        test_file = self.generated_dir / "test.py"
        original_content = "import dlt\nprint('hello')"
        test_file.write_text(original_content)
        
        result = _add_databricks_header(test_file)
        
        assert result is True
        new_content = test_file.read_text()
        assert new_content == f"{DATABRICKS_HEADER}\n{original_content}"

    def test_add_databricks_header_to_empty_file(self):
        """Should add header to empty file."""
        test_file = self.generated_dir / "empty.py"
        test_file.write_text("")
        
        result = _add_databricks_header(test_file)
        
        assert result is True
        new_content = test_file.read_text()
        assert new_content == f"{DATABRICKS_HEADER}\n"

    def test_add_databricks_notebook_headers_no_directory(self):
        """Should handle missing directory gracefully."""
        non_existent = self.temp_dir / "nonexistent"
        
        result = add_databricks_notebook_headers(non_existent, "dev")
        
        assert result == 0

    def test_add_databricks_notebook_headers_no_python_files(self):
        """Should handle directory with no Python files."""
        (self.generated_dir / "README.md").write_text("# No Python files")
        
        result = add_databricks_notebook_headers(self.generated_dir, "dev")
        
        assert result == 0

    def test_add_databricks_notebook_headers_success(self):
        """Should add headers to Python files that need them."""
        # Create files with and without headers
        file1 = self.generated_dir / "flow1.py"
        file1.write_text("import dlt\nprint('flow1')")
        
        file2 = self.generated_dir / "flow2.py"
        file2.write_text(f"{DATABRICKS_HEADER}\nimport dlt\nprint('flow2')")
        
        file3 = self.generated_dir / "flow3.py"
        file3.write_text("# Some other comment\nprint('flow3')")
        
        result = add_databricks_notebook_headers(self.generated_dir, "dev")
        
        # Should modify 2 files (file1 and file3, but not file2)
        assert result == 2
        
        # Verify headers were added
        assert file1.read_text().startswith(DATABRICKS_HEADER)
        assert file2.read_text().startswith(DATABRICKS_HEADER)
        assert file3.read_text().startswith(DATABRICKS_HEADER)

    def test_add_databricks_notebook_headers_all_have_headers(self):
        """Should not modify files that already have headers."""
        file1 = self.generated_dir / "flow1.py"
        file1.write_text(f"{DATABRICKS_HEADER}\nimport dlt\nprint('flow1')")
        
        file2 = self.generated_dir / "flow2.py"
        file2.write_text(f"{DATABRICKS_HEADER}\nimport dlt\nprint('flow2')")
        
        result = add_databricks_notebook_headers(self.generated_dir, "dev")
        
        assert result == 0

    @patch('lhp.bundle.temporary_databricks_headers.logger')
    def test_add_databricks_notebook_headers_with_error(self, mock_logger):
        """Should handle file processing errors gracefully."""
        # Create a file
        test_file = self.generated_dir / "test.py"
        test_file.write_text("print('test')")
        
        # Mock _add_databricks_header to raise exception
        with patch('lhp.bundle.temporary_databricks_headers._add_databricks_header') as mock_add:
            mock_add.side_effect = Exception("File error")
            
            result = add_databricks_notebook_headers(self.generated_dir, "dev")
            
            # Should continue processing despite error
            assert result == 0
            mock_logger.error.assert_called()

    def test_duplicate_prevention(self):
        """Should prevent duplicate headers when run multiple times."""
        test_file = self.generated_dir / "test.py"
        test_file.write_text("import dlt\nprint('test')")
        
        # Run twice
        result1 = add_databricks_notebook_headers(self.generated_dir, "dev")
        result2 = add_databricks_notebook_headers(self.generated_dir, "dev")
        
        assert result1 == 1  # First run adds header
        assert result2 == 0  # Second run does nothing
        
        # Verify only one header
        content = test_file.read_text()
        header_count = content.count(DATABRICKS_HEADER)
        assert header_count == 1 
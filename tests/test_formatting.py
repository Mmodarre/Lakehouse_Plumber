"""Tests for code formatting utilities of LakehousePlumber."""

import pytest
from lhp.utils.formatter import format_code, organize_imports, format_sql


class TestCodeFormatter:
    """Test code formatting utilities."""
    
    def test_format_code(self):
        """Test Python code formatting."""
        unformatted = """def hello(  name   ):
    return    f"Hello {name}"
"""
        formatted = format_code(unformatted)
        assert 'def hello(name):' in formatted
        assert '    return f"Hello {name}"' in formatted
    
    def test_organize_imports(self):
        """Test import organization."""
        code = """import os
from pathlib import Path
import dlt
from pyspark.sql import DataFrame
from mymodule import helper
"""
        
        organized = organize_imports(code)
        
        # Check order: stdlib, third-party, local
        lines = organized.strip().split('\n')
        
        # Find positions of imports
        os_pos = next(i for i, line in enumerate(lines) if 'import os' in line)
        dlt_pos = next(i for i, line in enumerate(lines) if 'import dlt' in line)
        
        # Standard library should come before third-party
        assert os_pos < dlt_pos
    
    def test_format_sql(self):
        """Test SQL formatting."""
        sql = "SELECT id, name FROM users WHERE age > 18 ORDER BY name"
        formatted = format_sql(sql)
        
        assert 'SELECT' in formatted
        assert 'FROM' in formatted
        assert 'WHERE' in formatted
        assert 'ORDER BY' in formatted
        assert formatted.count('\n') >= 3  # Should be multi-line


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
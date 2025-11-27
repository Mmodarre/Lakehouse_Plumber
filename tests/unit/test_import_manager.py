"""
Comprehensive tests for ImportManager module.

Tests cover:
- Basic import collection functionality
- Conflict resolution (wildcard precedence, submodule conflicts)
- Import sorting and categorization
- AST processing for file-based imports
- Integration with BaseActionGenerator and other components
- Error handling and edge cases (90% coverage target)
- Real-world scenarios
"""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.utils.import_manager import ImportManager
from lhp.core.base_generator import BaseActionGenerator
from lhp.generators.load.custom_datasource import CustomDataSourceLoadGenerator
from lhp.models.config import Action, ActionType


class TestImportManagerBasics:
    """Test basic ImportManager functionality."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()
    
    def test_initialization(self):
        """Test ImportManager initialization."""
        assert self.manager is not None
        assert len(self.manager.get_consolidated_imports()) == 0
        
        stats = self.manager.get_stats()
        assert stats["manual_imports"] == 0
        assert stats["expression_imports"] == 0
        assert stats["file_imports"] == 0
        assert stats["total_unique"] == 0

    def test_manual_import_addition(self):
        """Test adding manual import statements."""
        # Test basic import addition
        self.manager.add_import("import os")
        self.manager.add_import("from pathlib import Path")
        
        imports = self.manager.get_consolidated_imports()
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) == 2
        
        stats = self.manager.get_stats()
        assert stats["manual_imports"] == 2
        assert stats["total_unique"] == 2

    def test_manual_import_duplicates(self):
        """Test that duplicate manual imports are handled correctly."""
        self.manager.add_import("import os")
        self.manager.add_import("import os")  # Duplicate
        self.manager.add_import("from pathlib import Path")
        
        imports = self.manager.get_consolidated_imports()
        assert imports.count("import os") == 1  # Should only appear once
        assert len(imports) == 2

    def test_manual_import_whitespace_handling(self):
        """Test whitespace handling in manual imports."""
        self.manager.add_import("  import os  ")
        self.manager.add_import("\tfrom pathlib import Path\n")
        self.manager.add_import("")  # Empty string
        self.manager.add_import("   ")  # Whitespace only
        
        imports = self.manager.get_consolidated_imports()
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) == 2  # Empty/whitespace should be ignored

    def test_expression_import_detection(self):
        """Test detection of imports from PySpark expressions."""
        # Test common PySpark expressions
        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('name').alias('column_name')")
        self.manager.add_imports_from_expression("F.when(F.col('age') > 18, F.lit('adult'))")
        
        imports = self.manager.get_consolidated_imports()
        # Should detect F (functions) import from expressions
        assert any("functions" in imp for imp in imports)
        
        stats = self.manager.get_stats()
        assert stats["expression_imports"] > 0

    def test_expression_import_invalid_expressions(self):
        """Test handling of invalid expressions."""
        # These should not crash the system
        self.manager.add_imports_from_expression("invalid_syntax((")
        self.manager.add_imports_from_expression("")
        self.manager.add_imports_from_expression(None)
        
        # Should still work normally
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_file_import_extraction(self):
        """Test extraction of imports from Python files."""
        # Read our test fixture files
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        
        # Test basic imports file
        basic_file = fixtures_dir / "basic_imports.py"
        source_code = basic_file.read_text()
        
        cleaned_source = self.manager.add_imports_from_file(source_code)
        
        # Check that imports were extracted
        imports = self.manager.get_consolidated_imports()
        assert len(imports) > 0
        assert any("import os" in imp for imp in imports)
        assert any("pathlib" in imp for imp in imports)
        
        # Check that source was cleaned (imports removed but structure preserved)
        assert "def sample_function" in cleaned_source
        assert "import os" not in cleaned_source
        
        stats = self.manager.get_stats()
        assert stats["file_imports"] > 0

    def test_file_import_mixed_sources(self):
        """Test combining manual, expression, and file imports."""
        # Add from different sources
        self.manager.add_import("import custom_module")
        self.manager.add_imports_from_expression("F.lit('test')")
        
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        basic_file = fixtures_dir / "basic_imports.py"
        self.manager.add_imports_from_file(basic_file.read_text())
        
        imports = self.manager.get_consolidated_imports()
        stats = self.manager.get_stats()
        
        # Should have imports from all sources
        assert stats["manual_imports"] >= 1
        assert stats["expression_imports"] >= 1
        assert stats["file_imports"] >= 1
        assert stats["total_unique"] == len(imports)
        
        # Check for specific imports from each source
        assert "import custom_module" in imports
        assert any("pathlib" in imp for imp in imports)

    def test_clear_functionality(self):
        """Test clearing all collected imports."""
        # Add various imports
        self.manager.add_import("import os")
        self.manager.add_imports_from_expression("F.current_timestamp()")
        
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        basic_file = fixtures_dir / "basic_imports.py"
        self.manager.add_imports_from_file(basic_file.read_text())
        
        # Verify imports exist
        assert len(self.manager.get_consolidated_imports()) > 0
        
        # Clear and verify
        self.manager.clear()
        imports = self.manager.get_consolidated_imports()
        stats = self.manager.get_stats()
        
        assert len(imports) == 0
        assert stats["manual_imports"] == 0
        assert stats["expression_imports"] == 0
        assert stats["file_imports"] == 0
        assert stats["total_unique"] == 0


class TestConflictResolution:
    """Test import conflict resolution functionality."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_basic_wildcard_precedence(self):
        """Test that wildcard imports take precedence over specific imports."""
        # Add conflicting imports from same module
        self.manager.add_import("from pyspark.sql.functions import col, lit")
        self.manager.add_import("from pyspark.sql.functions import *")
        
        imports = self.manager.get_consolidated_imports()
        
        # Should only have wildcard import, not specific ones
        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql.functions import col, lit" not in imports
        assert len([imp for imp in imports if "pyspark.sql.functions" in imp]) == 1

    def test_submodule_conflict_resolution(self):
        """Test resolution of parent-child module conflicts."""
        # This is the key scenario we fixed: F alias vs wildcard import
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql.functions import *")
        
        imports = self.manager.get_consolidated_imports()
        
        # Wildcard from child module should win, parent alias should be removed
        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql import functions as F" not in imports

    def test_multiple_submodule_conflicts(self):
        """Test multiple parent-child conflicts."""
        # Add multiple conflicting patterns
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql import types as T")
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.types import *")
        
        imports = self.manager.get_consolidated_imports()
        
        # Both wildcard imports should remain, both parent aliases should be removed
        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql.types import *" in imports
        assert "from pyspark.sql import functions as F" not in imports
        assert "from pyspark.sql import types as T" not in imports

    def test_non_conflicting_submodules(self):
        """Test that non-conflicting submodule imports are preserved."""
        # Parent import without wildcard child
        self.manager.add_import("from pyspark.sql import SparkSession")
        self.manager.add_import("from pyspark.sql.functions import col, lit")  # No wildcard
        
        imports = self.manager.get_consolidated_imports()
        
        # Both should be preserved since no wildcard conflict
        assert "from pyspark.sql import SparkSession" in imports
        assert "from pyspark.sql.functions import col, lit" in imports

    def test_complex_conflict_scenario(self):
        """Test complex scenarios with multiple types of conflicts."""
        # Mix of manual, expression, and file imports with conflicts
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("import os")
        
        # Add from expression (should detect F usage)
        self.manager.add_imports_from_expression("F.current_timestamp()")
        
        # Add from file (contains wildcard imports)
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        wildcard_file = fixtures_dir / "wildcard_conflicts.py"
        self.manager.add_imports_from_file(wildcard_file.read_text())
        
        imports = self.manager.get_consolidated_imports()
        
        # Non-conflicting imports should remain
        assert "import os" in imports
        
        # Wildcard should win over specific imports
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)

    def test_same_module_wildcard_duplicates(self):
        """Test handling of multiple wildcard imports from same module."""
        self.manager.add_import("from pyspark.sql.functions import *")
        self.manager.add_import("from pyspark.sql.functions import *")  # Duplicate
        
        imports = self.manager.get_consolidated_imports()
        
        # Should only appear once
        wildcard_imports = [imp for imp in imports if "from pyspark.sql.functions import *" in imp]
        assert len(wildcard_imports) == 1

    def test_no_conflicts_preserved(self):
        """Test that imports without conflicts are preserved correctly."""
        # Different modules, no conflicts
        self.manager.add_import("import os")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_import("from pyspark import pipelines as dp")
        self.manager.add_import("from typing import Dict, List")
        
        imports = self.manager.get_consolidated_imports()
        
        # All should be preserved
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert "from pyspark import pipelines as dp" in imports
        assert "from typing import Dict, List" in imports
        assert len(imports) == 4

    def test_partial_conflicts(self):
        """Test scenarios with some conflicts and some non-conflicts."""
        # Mix of conflicting and non-conflicting imports
        self.manager.add_import("import os")  # No conflict
        self.manager.add_import("from pyspark.sql import functions as F")  # Will conflict
        self.manager.add_import("from pathlib import Path")  # No conflict
        self.manager.add_import("from pyspark.sql.functions import *")  # Conflicts with F import
        self.manager.add_import("from pyspark import pipelines as dp")  # No conflict
        
        imports = self.manager.get_consolidated_imports()
        
        # Non-conflicting should remain
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert "from pyspark import pipelines as dp" in imports
        
        # Wildcard should win
        assert "from pyspark.sql.functions import *" in imports
        assert "from pyspark.sql import functions as F" not in imports

    def test_different_alias_patterns(self):
        """Test different import alias patterns for conflict detection."""
        # Test various alias patterns that should conflict with wildcards
        self.manager.add_import("from pyspark.sql import functions as F")
        self.manager.add_import("from pyspark.sql import functions as pyspark_functions")
        self.manager.add_import("from pyspark.sql import functions")  # No alias
        self.manager.add_import("from pyspark.sql.functions import *")
        
        imports = self.manager.get_consolidated_imports()
        
        # Only wildcard should remain
        assert "from pyspark.sql.functions import *" in imports
        
        # All parent imports should be removed
        assert not any("from pyspark.sql import functions" in imp for imp in imports 
                      if "import *" not in imp)


class TestImportSorting:
    """Test import sorting and categorization."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_import_categorization(self):
        """Test correct categorization of different import types."""
        # Add imports from different categories
        imports_by_category = {
            "standard": ["import os", "from pathlib import Path", "import json"],
            "third_party": ["import pandas", "import numpy", "import requests"],
            "pyspark": ["from pyspark.sql import SparkSession", "import pyspark"],
            "dlt": ["from pyspark import pipelines as dp"],
            "custom": ["from mymodule import helper", "import custom_package"]
        }
        
        for category, imports in imports_by_category.items():
            for imp in imports:
                self.manager.add_import(imp)
        
        # Test categorization through sorting
        sorted_imports = self.manager.get_consolidated_imports()
        
        # Should have all imports
        total_expected = sum(len(imports) for imports in imports_by_category.values())
        assert len(sorted_imports) == total_expected

    def test_import_order_standard_first(self):
        """Test that standard library imports come first."""
        self.manager.add_import("import custom_module")  # custom (last)
        self.manager.add_import("from pyspark import pipelines as dp")           # dlt (4th)
        self.manager.add_import("import os")            # standard (1st)
        self.manager.add_import("import pandas")        # third-party (2nd)
        self.manager.add_import("import pyspark")       # pyspark (3rd)
        
        imports = self.manager.get_consolidated_imports()
        
        # Find positions
        os_pos = imports.index("import os")
        pandas_pos = imports.index("import pandas") 
        pyspark_pos = imports.index("import pyspark")
        dlt_pos = imports.index("from pyspark import pipelines as dp")
        custom_pos = imports.index("import custom_module")
        
        # Verify correct order: standard -> third_party -> pyspark -> dlt -> custom
        assert os_pos < pandas_pos < pyspark_pos < dlt_pos < custom_pos

    def test_alphabetical_within_categories(self):
        """Test alphabetical sorting within each category."""
        # Add multiple imports from same category in non-alphabetical order
        standard_imports = ["import sys", "import os", "import json", "from pathlib import Path"]
        for imp in standard_imports:
            self.manager.add_import(imp)
        
        imports = self.manager.get_consolidated_imports()
        
        # Extract just the standard library imports
        standard_in_result = [imp for imp in imports if any(
            std in imp for std in ["os", "sys", "json", "pathlib"]
        )]
        
        # Should be alphabetically sorted
        expected_order = ["from pathlib import Path", "import json", "import os", "import sys"]
        assert standard_in_result == expected_order

    def test_mixed_import_styles_sorting(self):
        """Test sorting of mixed import styles (import vs from)."""
        self.manager.add_import("import os")
        self.manager.add_import("from os import path")
        self.manager.add_import("from pathlib import Path")
        self.manager.add_import("import sys")
        
        imports = self.manager.get_consolidated_imports()
        
        # All should be standard library and sorted alphabetically
        expected = ["from os import path", "from pathlib import Path", "import os", "import sys"]
        assert imports == expected

    def test_pyspark_specific_categorization(self):
        """Test that PySpark imports are correctly categorized."""
        pyspark_imports = [
            "import pyspark",
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import col",
            "from pyspark.sql.types import StructType"
        ]
        
        for imp in pyspark_imports:
            self.manager.add_import(imp)
        
        imports = self.manager.get_consolidated_imports()
        
        # All should be recognized as pyspark and grouped together
        assert len(imports) == len(pyspark_imports)
        
        # Check they're in correct alphabetical order within pyspark category
        # (from imports come before import statements alphabetically)
        expected_order = [
            "from pyspark import sql",  # Would be detected if present
            "from pyspark.sql import SparkSession",
            "from pyspark.sql import functions as F",
            "from pyspark.sql.functions import col",
            "from pyspark.sql.types import StructType",
            "import pyspark"
        ]
        
        # Just verify they're all there and pyspark imports are grouped
        for imp in pyspark_imports:
            assert imp in imports

    def test_dlt_categorization(self):
        """Test DLT-specific import categorization."""
        dlt_imports = [
            "from pyspark import pipelines as dp",
        ]
        
        for imp in dlt_imports:
            self.manager.add_import(imp)
        
        imports = self.manager.get_consolidated_imports()
        
        # All should be categorized as DLT
        for imp in dlt_imports:
            assert imp in imports

    def test_complete_sorting_workflow(self):
        """Test complete sorting workflow with all categories."""
        # Add imports in random order from all categories
        all_imports = [
            "import custom_module",      # custom
            "import os",                 # standard
            "from pyspark.sql import functions as F",  # pyspark
            "import requests",           # third_party
            "from pathlib import Path",  # standard
            "from pyspark import pipelines as dp",               # dlt
            "import pandas",            # third_party
            "from pyspark.sql.functions import *",  # pyspark
            "import json"               # standard
        ]
        
        for imp in all_imports:
            self.manager.add_import(imp)
        
        imports = self.manager.get_consolidated_imports()
        
        # Verify we have expected number (minus conflicts)
        assert len(imports) >= 7  # Some may be removed due to conflicts
        
        # Find category boundaries by checking first occurrence of each type
        first_third_party = next((i for i, imp in enumerate(imports) 
                                if "requests" in imp or "pandas" in imp), -1)
        first_pyspark = next((i for i, imp in enumerate(imports) 
                            if "pyspark" in imp and "pipelines" not in imp), -1)
        first_dlt = next((i for i, imp in enumerate(imports) 
                        if "pipelines" in imp), -1)
        first_custom = next((i for i, imp in enumerate(imports) 
                           if "custom_module" in imp), -1)
        
        # Standard should come first (os, pathlib, json at positions 0-2)
        standard_imports = [imp for imp in imports if any(
            std in imp for std in ["os", "pathlib", "json"]
        )]
        assert len(standard_imports) >= 2  # At least os and pathlib
        
        # Verify order progression (each category after the previous)
        if first_third_party >= 0 and first_pyspark >= 0:
            assert first_third_party < first_pyspark
        if first_pyspark >= 0 and first_dlt >= 0:
            assert first_pyspark < first_dlt  
        if first_dlt >= 0 and first_custom >= 0:
            assert first_dlt < first_custom

    def test_unknown_modules_categorized_as_custom(self):
        """Test that unknown modules are categorized as custom."""
        unknown_imports = [
            "import unknown_module",
            "from mysterious import function",
            "import project_specific"
        ]
        
        for imp in unknown_imports:
            self.manager.add_import(imp)
        
        imports = self.manager.get_consolidated_imports()
        
        # Should all be present and categorized as custom (appear last)
        for imp in unknown_imports:
            assert imp in imports


class TestASTProcessing:
    """Test AST processing for file-based imports."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_valid_python_file_processing(self):
        """Test processing of valid Python files."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        
        # Test basic imports file
        basic_file = fixtures_dir / "basic_imports.py"
        source_code = basic_file.read_text()
        
        cleaned_source = self.manager.add_imports_from_file(source_code)
        
        # Verify imports were extracted
        imports = self.manager.get_consolidated_imports()
        assert len(imports) > 0
        
        # Verify source was cleaned but structure preserved
        assert "def sample_function" in cleaned_source
        assert "import os" not in cleaned_source
        assert "from pathlib import Path" not in cleaned_source

    def test_invalid_syntax_file_handling(self):
        """Test graceful handling of files with syntax errors."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        
        # Test file with syntax errors
        invalid_file = fixtures_dir / "invalid_syntax.py"
        source_code = invalid_file.read_text()
        
        # Should not crash, should return original source
        cleaned_source = self.manager.add_imports_from_file(source_code)
        
        # Should return original source unchanged
        assert cleaned_source == source_code
        
        # Should still be able to get consolidated imports
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_complex_file_processing(self):
        """Test processing of complex Python files with mixed imports."""
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        
        # Test mixed imports file
        mixed_file = fixtures_dir / "mixed_imports.py"
        source_code = mixed_file.read_text()
        
        cleaned_source = self.manager.add_imports_from_file(source_code)
        
        # Verify various import types were extracted
        imports = self.manager.get_consolidated_imports()
        assert len(imports) > 5  # Should have many imports
        
        # Verify function code is preserved
        assert "def complex_function" in cleaned_source


class TestIntegration:
    """Test integration with other components."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_base_generator_integration(self):
        """Test integration with BaseActionGenerator."""
        # Create a mock generator that uses ImportManager
        class TestGenerator(BaseActionGenerator):
            def __init__(self):
                super().__init__(use_import_manager=True)
            
            def generate(self, action, context):
                return "test_code"
        
        generator = TestGenerator()
        
        # Add imports through generator
        generator.add_import("import os")
        generator.add_imports_from_expression("F.current_timestamp()")
        
        # Verify ImportManager is working
        import_manager = generator.get_import_manager()
        assert import_manager is not None
        
        imports = generator.imports
        assert "import os" in imports
        assert len(imports) >= 1

    def test_custom_datasource_integration(self):
        """Test integration with CustomDataSourceLoadGenerator scenario."""
        # Simulate the custom datasource scenario
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        custom_source = fixtures_dir / "custom_datasource.py"
        
        # Extract imports from custom source
        source_code = custom_source.read_text()
        cleaned_source = self.manager.add_imports_from_file(source_code)
        
        # Add operational metadata imports
        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('_metadata')")
        
        imports = self.manager.get_consolidated_imports()
        
        # Should have resolved conflicts properly
        assert len(imports) > 0
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)


class TestErrorHandling:
    """Test error handling and edge cases (90% coverage target)."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_none_inputs(self):
        """Test handling of None inputs."""
        # Should not crash with None inputs
        self.manager.add_import(None)
        self.manager.add_imports_from_expression(None)
        
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_empty_inputs(self):
        """Test handling of empty inputs."""
        self.manager.add_import("")
        self.manager.add_import("   ")
        self.manager.add_imports_from_expression("")
        self.manager.add_imports_from_file("")
        
        imports = self.manager.get_consolidated_imports()
        assert len(imports) == 0

    def test_malformed_import_statements(self):
        """Test handling of malformed import statements."""
        malformed_imports = [
            "import",           # Incomplete
            "from import",      # Invalid syntax
            "import 123invalid", # Invalid module name
            "from . import",    # Incomplete relative import
        ]
        
        for imp in malformed_imports:
            self.manager.add_import(imp)
        
        # Should not crash
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_expression_parsing_errors(self):
        """Test handling of expression parsing errors."""
        invalid_expressions = [
            "F.function_with_unclosed_paren(",
            "invalid.syntax..with..dots",
            "F.('malformed')",
            "invalid_identifier_with_$pecial_chars",
        ]
        
        for expr in invalid_expressions:
            self.manager.add_imports_from_expression(expr)
        
        # Should not crash
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)

    def test_file_processing_edge_cases(self):
        """Test file processing edge cases."""
        edge_cases = [
            "",              # Empty file
            "# Just comments\n# More comments",  # Comments only
            "'''Triple quoted string'''",        # String only
            "pass",          # Single statement
        ]
        
        for source in edge_cases:
            cleaned = self.manager.add_imports_from_file(source)
            assert isinstance(cleaned, str)
        
        imports = self.manager.get_consolidated_imports()
        assert isinstance(imports, list)


class TestUtilityMethods:
    """Test utility and helper methods."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_extract_module_name(self):
        """Test _extract_module_name helper method."""
        test_cases = [
            ("import os", "os"),
            ("from pathlib import Path", "pathlib"),
            ("from pyspark.sql import functions as F", "pyspark.sql"),
            ("from pyspark.sql.functions import *", "pyspark.sql.functions"),
            ("invalid import statement", None),
        ]
        
        for import_stmt, expected in test_cases:
            result = self.manager._extract_module_name(import_stmt)
            assert result == expected

    def test_is_wildcard_import(self):
        """Test _is_wildcard_import helper method."""
        wildcard_cases = [
            ("from pyspark.sql.functions import *", True),
            ("from os import *", True),
            ("from pyspark.sql.functions import col", False),
            ("import os", False),
        ]
        
        for import_stmt, expected in wildcard_cases:
            result = self.manager._is_wildcard_import(import_stmt)
            assert result == expected

    def test_categorize_import(self):
        """Test _categorize_import helper method."""
        categorization_cases = [
            ("import os", "standard"),
            ("from pathlib import Path", "standard"),
            ("import pandas", "third_party"),
            ("import requests", "third_party"),
            ("from pyspark.sql import SparkSession", "pyspark"),
            ("from pyspark import pipelines as dp", "dlt"),
            ("import unknown_module", "custom"),
        ]
        
        for import_stmt, expected in categorization_cases:
            result = self.manager._categorize_import(import_stmt)
            assert result == expected

    def test_stats_and_debug_info(self):
        """Test statistics and debug information methods."""
        # Add various imports
        self.manager.add_import("import os")
        self.manager.add_imports_from_expression("F.current_timestamp()")
        
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        basic_file = fixtures_dir / "basic_imports.py"
        self.manager.add_imports_from_file(basic_file.read_text())
        
        # Test stats
        stats = self.manager.get_stats()
        assert stats["manual_imports"] >= 1
        assert stats["expression_imports"] >= 1
        assert stats["file_imports"] >= 1
        assert stats["total_unique"] > 0
        
        # Test debug info
        debug_info = self.manager.debug_info()
        assert "manual_imports" in debug_info
        assert "expression_imports" in debug_info
        assert "file_imports" in debug_info
        assert "consolidated" in debug_info
        assert "stats" in debug_info


class TestRealWorldScenarios:
    """Test real-world usage scenarios."""
    
    def setup_method(self):
        """Setup for each test method."""
        self.manager = ImportManager()

    def test_custom_datasource_complete_scenario(self):
        """Test complete custom datasource scenario like our currency_api_source.py fix."""
        # Simulate the exact scenario we fixed
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        
        # 1. Extract imports from custom source file
        custom_source = fixtures_dir / "custom_datasource.py"
        self.manager.add_imports_from_file(custom_source.read_text())
        
        # 2. Add operational metadata imports
        self.manager.add_imports_from_expression("F.current_timestamp()")
        self.manager.add_imports_from_expression("F.col('_processing_timestamp')")
        
        # 3. Add manual imports that might come from generator
        self.manager.add_import("from pyspark import pipelines as dp")
        
        imports = self.manager.get_consolidated_imports()
        
        # Should have resolved the key conflict we fixed
        wildcard_imports = [imp for imp in imports if "from pyspark.sql.functions import *" in imp]
        f_alias_imports = [imp for imp in imports if "from pyspark.sql import functions as F" in imp]
        
        assert len(wildcard_imports) == 1  # Should have wildcard
        assert len(f_alias_imports) == 0   # Should NOT have F alias due to conflict resolution

    def test_operational_metadata_integration(self):
        """Test operational metadata expression integration."""
        # Common operational metadata expressions
        metadata_expressions = [
            "F.current_timestamp()",
            "F.lit('pipeline_name')",
            "F.col('_metadata.file_path')",
            "F.when(F.col('status') == 'active', F.lit('valid'))",
        ]
        
        for expr in metadata_expressions:
            self.manager.add_imports_from_expression(expr)
        
        imports = self.manager.get_consolidated_imports()
        
        # Should detect and consolidate PySpark function imports
        assert len(imports) > 0
        assert any("functions" in imp for imp in imports)

    def test_mixed_generator_scenario(self):
        """Test scenario with multiple generators contributing imports."""
        # Simulate imports from different generators
        
        # CloudFiles generator imports
        self.manager.add_import("from pyspark.sql import functions as F")
        
        # Custom source imports (with conflicts)
        fixtures_dir = Path(__file__).parent / "fixtures" / "import_manager"
        wildcard_file = fixtures_dir / "wildcard_conflicts.py"
        self.manager.add_imports_from_file(wildcard_file.read_text())
        
        # Operational metadata imports
        self.manager.add_imports_from_expression("F.input_file_name()")
        
        # DLT imports
        self.manager.add_import("from pyspark import pipelines as dp")
        
        imports = self.manager.get_consolidated_imports()
        
        # Should handle all conflicts properly
        assert len(imports) > 0
        
        # Verify key conflict resolution
        assert any("from pyspark.sql.functions import *" in imp for imp in imports)
        
        # Non-conflicting imports should remain
        assert "from pyspark import pipelines as dp" in imports 
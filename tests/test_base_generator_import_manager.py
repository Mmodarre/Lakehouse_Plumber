"""
Comprehensive tests for BaseActionGenerator ImportManager integration.

Tests cover the changes made to base_generator.py for ImportManager support:
- Backward compatibility with existing generators
- ImportManager integration and routing
- New helper methods functionality
- Error handling and edge cases
"""

import pytest
from unittest.mock import Mock, patch
from pathlib import Path

from lhp.core.base_generator import BaseActionGenerator
from lhp.models.config import Action, ActionType


class ConcreteBaseActionGenerator(BaseActionGenerator):
    """Concrete implementation for testing BaseActionGenerator."""
    
    def generate(self, action, context):
        """Test implementation."""
        return "test_generated_code"


class TestBaseActionGeneratorBackwardCompatibility:
    """Test backward compatibility - existing behavior unchanged."""
    
    def test_default_initialization_legacy_mode(self):
        """Test default initialization maintains legacy behavior."""
        generator = ConcreteBaseActionGenerator()
        
        # Should not use ImportManager by default
        assert not generator._use_import_manager
        assert generator._import_manager is None
        assert hasattr(generator, '_imports')
        assert len(generator._imports) == 0

    def test_legacy_add_import_functionality(self):
        """Test add_import works as before when ImportManager disabled."""
        generator = ConcreteBaseActionGenerator()
        
        # Add imports using legacy method
        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        generator.add_import("import os")  # Duplicate
        
        # Should store in legacy _imports set
        assert "import os" in generator._imports
        assert "from pathlib import Path" in generator._imports
        assert len(generator._imports) == 2  # No duplicates
        
        # ImportManager should not be involved
        assert generator._import_manager is None

    def test_legacy_imports_property(self):
        """Test imports property returns sorted legacy imports."""
        generator = ConcreteBaseActionGenerator()
        
        # Add imports in non-alphabetical order
        generator.add_import("import sys")
        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        
        imports = generator.imports
        
        # Should return sorted list
        expected = ["from pathlib import Path", "import os", "import sys"]
        assert imports == expected
        assert isinstance(imports, list)

    def test_legacy_new_methods_graceful_fallback(self):
        """Test new methods do nothing gracefully when ImportManager disabled."""
        generator = ConcreteBaseActionGenerator()
        
        # These should not crash but should do nothing
        generator.add_imports_from_expression("F.current_timestamp()")
        source_code = "import test\nprint('hello')"
        cleaned_source = generator.add_imports_from_file(source_code)
        
        # Should return source unchanged
        assert cleaned_source == source_code
        
        # Should not affect legacy imports
        assert len(generator._imports) == 0
        
        # get_import_manager should return None
        assert generator.get_import_manager() is None


class TestBaseActionGeneratorImportManagerIntegration:
    """Test ImportManager integration when enabled."""
    
    def test_import_manager_initialization(self):
        """Test ImportManager is properly initialized when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Should use ImportManager
        assert generator._use_import_manager
        assert generator._import_manager is not None
        assert hasattr(generator, '_imports')  # Legacy still available

    def test_import_manager_add_import_routing(self):
        """Test add_import routes to ImportManager when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Mock the ImportManager to verify calls
        mock_import_manager = Mock()
        generator._import_manager = mock_import_manager
        
        # Add imports
        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        
        # Should route to ImportManager, not legacy set
        assert mock_import_manager.add_import.call_count == 2
        mock_import_manager.add_import.assert_any_call("import os")
        mock_import_manager.add_import.assert_any_call("from pathlib import Path")
        
        # Legacy set should remain empty
        assert len(generator._imports) == 0

    def test_import_manager_imports_property_routing(self):
        """Test imports property routes to ImportManager when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Mock the ImportManager
        mock_import_manager = Mock()
        mock_import_manager.get_consolidated_imports.return_value = [
            "import os", "from pathlib import Path"
        ]
        generator._import_manager = mock_import_manager
        
        imports = generator.imports
        
        # Should call ImportManager method
        mock_import_manager.get_consolidated_imports.assert_called_once()
        assert imports == ["import os", "from pathlib import Path"]

    def test_import_manager_add_imports_from_expression(self):
        """Test add_imports_from_expression when ImportManager enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Mock the ImportManager
        mock_import_manager = Mock()
        generator._import_manager = mock_import_manager
        
        # Add imports from expressions
        generator.add_imports_from_expression("F.current_timestamp()")
        generator.add_imports_from_expression("F.col('name').alias('column')")
        
        # Should route to ImportManager
        assert mock_import_manager.add_imports_from_expression.call_count == 2
        mock_import_manager.add_imports_from_expression.assert_any_call("F.current_timestamp()")
        mock_import_manager.add_imports_from_expression.assert_any_call("F.col('name').alias('column')")

    def test_import_manager_add_imports_from_file(self):
        """Test add_imports_from_file when ImportManager enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Mock the ImportManager
        mock_import_manager = Mock()
        mock_import_manager.add_imports_from_file.return_value = "cleaned_source"
        generator._import_manager = mock_import_manager
        
        source_code = "import os\nfrom pathlib import Path\nprint('hello')"
        cleaned_source = generator.add_imports_from_file(source_code)
        
        # Should route to ImportManager and return result
        mock_import_manager.add_imports_from_file.assert_called_once_with(source_code)
        assert cleaned_source == "cleaned_source"

    def test_import_manager_get_import_manager_access(self):
        """Test get_import_manager returns ImportManager instance when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        import_manager = generator.get_import_manager()
        
        # Should return the ImportManager instance
        assert import_manager is not None
        assert import_manager is generator._import_manager


class TestBaseActionGeneratorRealWorldIntegration:
    """Test with real ImportManager (not mocked) for integration testing."""
    
    def test_real_import_manager_basic_functionality(self):
        """Test basic functionality with real ImportManager."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Add various types of imports
        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        generator.add_imports_from_expression("F.current_timestamp()")
        
        imports = generator.imports
        
        # Should have imports from all sources
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) >= 2  # May have additional from expression

    def test_real_import_manager_file_processing(self):
        """Test file processing with real ImportManager."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        source_code = """import os
from pathlib import Path
import json

def test_function():
    return {"path": Path.cwd()}
"""
        
        cleaned_source = generator.add_imports_from_file(source_code)
        imports = generator.imports
        
        # Should extract imports and clean source
        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert "import json" in imports
        
        # Source should be cleaned (no import statements)
        assert "import os" not in cleaned_source
        assert "def test_function" in cleaned_source

    def test_real_import_manager_conflict_resolution(self):
        """Test import conflict resolution with real ImportManager."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Add conflicting imports
        generator.add_import("from pyspark.sql import functions as F")
        generator.add_import("from pyspark.sql.functions import *")
        
        imports = generator.imports
        
        # Should resolve conflicts (wildcard takes precedence)
        assert "from pyspark.sql.functions import *" in imports
        # F alias should be removed due to conflict
        assert not any("from pyspark.sql import functions as F" in imp for imp in imports)


class TestBaseActionGeneratorEdgeCases:
    """Test edge cases and error handling."""
    
    def test_import_manager_disabled_after_initialization(self):
        """Test behavior when ImportManager is manually disabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Manually disable (edge case)
        generator._use_import_manager = False
        
        # Should fall back to legacy behavior
        generator.add_import("import os")
        
        # Should add to legacy set, not ImportManager
        assert "import os" in generator._imports
        assert generator.get_import_manager() is None

    def test_import_manager_none_after_initialization(self):
        """Test behavior when ImportManager instance is None."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Manually set to None (edge case)
        generator._import_manager = None
        
        # Should fall back to legacy behavior gracefully
        generator.add_import("import os")
        
        assert "import os" in generator._imports

    def test_empty_inputs_handling(self):
        """Test handling of empty or None inputs."""
        # Test legacy mode - Note: BaseActionGenerator doesn't handle None gracefully
        # This is expected behavior, so we test empty strings only
        generator_legacy = ConcreteBaseActionGenerator()
        generator_legacy.add_import("")  # Empty string should be handled
        
        # Should handle gracefully
        imports = generator_legacy.imports
        assert isinstance(imports, list)
        
        # Test ImportManager mode (should handle None gracefully)
        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)
        generator_im.add_import("")
        generator_im.add_imports_from_expression("")
        generator_im.add_imports_from_expression(None)
        cleaned = generator_im.add_imports_from_file("")
        
        # Should handle gracefully
        imports = generator_im.imports
        assert isinstance(imports, list)
        assert isinstance(cleaned, str)

    def test_none_input_handling_difference(self):
        """Test that None inputs behave differently in legacy vs ImportManager mode."""
        # Legacy mode: None causes sorting error (expected limitation)
        generator_legacy = ConcreteBaseActionGenerator()
        generator_legacy.add_import("import os")
        generator_legacy._imports.add(None)  # Directly add None to demonstrate issue
        
        with pytest.raises(TypeError, match="not supported between instances"):
            # This is expected behavior - legacy mode can't handle None
            _ = generator_legacy.imports
        
        # ImportManager mode: Should handle None gracefully
        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)
        generator_im.add_import("import os")
        generator_im.add_import(None)  # Should be handled gracefully
        
        imports = generator_im.imports
        assert "import os" in imports
        assert isinstance(imports, list)

    def test_mixed_usage_patterns(self):
        """Test mixing legacy and new methods."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)
        
        # Mix different types of import additions
        generator.add_import("import os")  # Should route to ImportManager
        generator.add_imports_from_expression("F.lit('test')")  # ImportManager method
        
        # Legacy _imports should remain empty
        assert len(generator._imports) == 0
        
        # All imports should be managed by ImportManager
        imports = generator.imports
        assert "import os" in imports


class TestBaseActionGeneratorTemplateIntegration:
    """Test that template functionality remains unchanged."""
    
    def test_template_functionality_preserved(self):
        """Test that Jinja2 template functionality is preserved."""
        # Test both modes
        for use_im in [False, True]:
            generator = ConcreteBaseActionGenerator(use_import_manager=use_im)
            
            # Should have template environment
            assert hasattr(generator, 'env')
            assert generator.env is not None
            
            # Should have filters
            assert 'tojson' in generator.env.filters
            assert 'toyaml' in generator.env.filters
            
            # Should be able to render templates (if template exists)
            assert hasattr(generator, 'render_template')

    def test_generate_method_still_abstract(self):
        """Test that generate method remains abstract."""
        # Cannot instantiate base class directly
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseActionGenerator()
        
        # Our test class should work
        generator = ConcreteBaseActionGenerator()
        assert generator.generate(None, {}) == "test_generated_code"


class TestBaseActionGeneratorPerformance:
    """Test performance characteristics."""
    
    def test_initialization_performance(self):
        """Test that initialization is not significantly slower."""
        import time
        
        # Test legacy initialization
        start = time.time()
        for _ in range(100):
            ConcreteBaseActionGenerator()
        legacy_time = time.time() - start
        
        # Test ImportManager initialization
        start = time.time()
        for _ in range(100):
            ConcreteBaseActionGenerator(use_import_manager=True)
        im_time = time.time() - start
        
        # ImportManager initialization should not be dramatically slower
        # Allow up to 10x slower (very generous threshold)
        assert im_time < legacy_time * 10

    def test_import_collection_performance(self):
        """Test that import collection performance is reasonable."""
        import time
        
        # Test with many imports
        generator_legacy = ConcreteBaseActionGenerator()
        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)
        
        imports_to_add = [f"import module_{i}" for i in range(100)]
        
        # Legacy mode
        start = time.time()
        for imp in imports_to_add:
            generator_legacy.add_import(imp)
        _ = generator_legacy.imports
        legacy_time = time.time() - start
        
        # ImportManager mode
        start = time.time()
        for imp in imports_to_add:
            generator_im.add_import(imp)
        _ = generator_im.imports
        im_time = time.time() - start
        
        # Should complete in reasonable time (less than 1 second each)
        assert legacy_time < 1.0
        assert im_time < 1.0 
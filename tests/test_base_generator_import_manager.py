"""Tests for BaseActionGenerator ImportManager integration."""

from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lhp.core.registry import BaseActionGenerator
from lhp.models import Action, ActionType


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

        assert not generator._use_import_manager
        assert generator._import_manager is None
        assert hasattr(generator, "_imports")
        assert len(generator._imports) == 0

    def test_legacy_add_import_functionality(self):
        """Test add_import works as before when ImportManager disabled."""
        generator = ConcreteBaseActionGenerator()

        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        generator.add_import("import os")  # duplicate

        assert "import os" in generator._imports
        assert "from pathlib import Path" in generator._imports
        assert len(generator._imports) == 2

        assert generator._import_manager is None

    def test_legacy_imports_property(self):
        """Test imports property returns sorted legacy imports."""
        generator = ConcreteBaseActionGenerator()

        generator.add_import("import sys")
        generator.add_import("import os")
        generator.add_import("from pathlib import Path")

        imports = generator.imports

        expected = ["from pathlib import Path", "import os", "import sys"]
        assert imports == expected
        assert isinstance(imports, list)

    def test_legacy_new_methods_graceful_fallback(self):
        """Test new methods do nothing gracefully when ImportManager disabled."""
        generator = ConcreteBaseActionGenerator()

        generator.add_imports_from_expression("F.current_timestamp()")

        assert len(generator._imports) == 0
        assert generator.get_import_manager() is None


class TestBaseActionGeneratorImportManagerIntegration:
    """Test ImportManager integration when enabled."""

    def test_import_manager_initialization(self):
        """Test ImportManager is properly initialized when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        assert generator._use_import_manager
        assert generator._import_manager is not None
        assert hasattr(generator, "_imports")

    def test_import_manager_add_import_routing(self):
        """Test add_import routes to ImportManager when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        mock_import_manager = Mock()
        generator._import_manager = mock_import_manager

        generator.add_import("import os")
        generator.add_import("from pathlib import Path")

        assert mock_import_manager.add_import.call_count == 2
        mock_import_manager.add_import.assert_any_call("import os")
        mock_import_manager.add_import.assert_any_call("from pathlib import Path")
        assert len(generator._imports) == 0

    def test_import_manager_imports_property_routing(self):
        """Test imports property routes to ImportManager when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        mock_import_manager = Mock()
        mock_import_manager.get_consolidated_imports.return_value = [
            "import os",
            "from pathlib import Path",
        ]
        generator._import_manager = mock_import_manager

        imports = generator.imports

        mock_import_manager.get_consolidated_imports.assert_called_once()
        assert imports == ["import os", "from pathlib import Path"]

    def test_import_manager_add_imports_from_expression(self):
        """Test add_imports_from_expression when ImportManager enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        mock_import_manager = Mock()
        generator._import_manager = mock_import_manager

        generator.add_imports_from_expression("F.current_timestamp()")
        generator.add_imports_from_expression("F.col('name').alias('column')")

        assert mock_import_manager.add_imports_from_expression.call_count == 2
        mock_import_manager.add_imports_from_expression.assert_any_call(
            "F.current_timestamp()"
        )
        mock_import_manager.add_imports_from_expression.assert_any_call(
            "F.col('name').alias('column')"
        )

    def test_import_manager_get_import_manager_access(self):
        """Test get_import_manager returns ImportManager instance when enabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        import_manager = generator.get_import_manager()

        assert import_manager is not None
        assert import_manager is generator._import_manager


class TestBaseActionGeneratorRealWorldIntegration:
    """Test with real ImportManager (not mocked) for integration testing."""

    def test_real_import_manager_basic_functionality(self):
        """Test basic functionality with real ImportManager."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        generator.add_import("import os")
        generator.add_import("from pathlib import Path")
        generator.add_imports_from_expression("F.current_timestamp()")

        imports = generator.imports

        assert "import os" in imports
        assert "from pathlib import Path" in imports
        assert len(imports) >= 2

    def test_real_import_manager_conflict_resolution(self):
        """Test import conflict resolution with real ImportManager."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        generator.add_import("from pyspark.sql import functions as F")
        generator.add_import("from pyspark.sql.functions import *")

        imports = generator.imports

        # Wildcard takes precedence; F alias removed due to conflict.
        assert "from pyspark.sql.functions import *" in imports
        assert not any(
            "from pyspark.sql import functions as F" in imp for imp in imports
        )


class TestBaseActionGeneratorEdgeCases:
    """Test edge cases and error handling."""

    def test_import_manager_disabled_after_initialization(self):
        """Test behavior when ImportManager is manually disabled."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        generator._use_import_manager = False

        generator.add_import("import os")

        assert "import os" in generator._imports
        assert generator.get_import_manager() is None

    def test_import_manager_none_after_initialization(self):
        """Test behavior when ImportManager instance is None."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        generator._import_manager = None

        generator.add_import("import os")

        assert "import os" in generator._imports

    def test_empty_inputs_handling(self):
        """Test handling of empty or None inputs."""
        # Legacy mode does not handle None gracefully; only empty strings are safe.
        generator_legacy = ConcreteBaseActionGenerator()
        generator_legacy.add_import("")

        imports = generator_legacy.imports
        assert isinstance(imports, list)

        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)
        generator_im.add_import("")
        generator_im.add_imports_from_expression("")
        generator_im.add_imports_from_expression(None)

        imports = generator_im.imports
        assert isinstance(imports, list)

    def test_none_input_handling_difference(self):
        """Test that None inputs behave differently in legacy vs ImportManager mode."""
        # Legacy mode raises TypeError on sort when None is present.
        generator_legacy = ConcreteBaseActionGenerator()
        generator_legacy.add_import("import os")
        generator_legacy._imports.add(None)

        with pytest.raises(TypeError, match="not supported between instances"):
            _ = generator_legacy.imports

        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)
        generator_im.add_import("import os")
        generator_im.add_import(None)

        imports = generator_im.imports
        assert "import os" in imports
        assert isinstance(imports, list)

    def test_mixed_usage_patterns(self):
        """Test mixing legacy and new methods."""
        generator = ConcreteBaseActionGenerator(use_import_manager=True)

        generator.add_import("import os")
        generator.add_imports_from_expression("F.lit('test')")

        assert len(generator._imports) == 0

        imports = generator.imports
        assert "import os" in imports


class TestBaseActionGeneratorTemplateIntegration:
    """Test that template functionality remains unchanged."""

    def test_template_functionality_preserved(self):
        """Test that Jinja2 template functionality is preserved."""
        for use_im in [False, True]:
            generator = ConcreteBaseActionGenerator(use_import_manager=use_im)

            assert hasattr(generator, "env")
            assert generator.env is not None
            assert "tojson" in generator.env.filters
            assert "toyaml" in generator.env.filters
            assert hasattr(generator, "render_template")

    def test_generate_method_still_abstract(self):
        """Test that generate method remains abstract."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseActionGenerator()

        generator = ConcreteBaseActionGenerator()
        assert generator.generate(None, {}) == "test_generated_code"


class TestBaseActionGeneratorPerformance:
    """Test performance characteristics."""

    def test_initialization_performance(self):
        """Test that initialization is not significantly slower."""
        import time

        start = time.time()
        for _ in range(100):
            ConcreteBaseActionGenerator()
        legacy_time = time.time() - start

        start = time.time()
        for _ in range(100):
            ConcreteBaseActionGenerator(use_import_manager=True)
        im_time = time.time() - start

        # Allow up to 10x slower (generous threshold).
        assert im_time < legacy_time * 10

    def test_import_collection_performance(self):
        """Test that import collection performance is reasonable."""
        import time

        generator_legacy = ConcreteBaseActionGenerator()
        generator_im = ConcreteBaseActionGenerator(use_import_manager=True)

        imports_to_add = [f"import module_{i}" for i in range(100)]

        start = time.time()
        for imp in imports_to_add:
            generator_legacy.add_import(imp)
        _ = generator_legacy.imports
        legacy_time = time.time() - start

        start = time.time()
        for imp in imports_to_add:
            generator_im.add_import(imp)
        _ = generator_im.imports
        im_time = time.time() - start

        assert legacy_time < 1.0
        assert im_time < 1.0

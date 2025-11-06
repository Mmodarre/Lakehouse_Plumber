"""Tests for the base generator framework of LakehousePlumber."""

import pytest
from lhp.core.base_generator import BaseActionGenerator


class TestBaseGenerator:
    """Test the base generator framework."""
    
    def test_base_generator_initialization(self):
        """Test that we can't instantiate the abstract base class."""
        with pytest.raises(TypeError):
            BaseActionGenerator()
    
    def test_concrete_generator(self):
        """Test a concrete implementation of BaseActionGenerator."""
        class TestGenerator(BaseActionGenerator):
            def generate(self, action, context):
                return f"Generated code for {action.name}"
        
        generator = TestGenerator()
        generator.add_import("from pyspark import pipelines as dp")
        generator.add_import("import pyspark")
        
        assert "from pyspark import pipelines as dp" in generator.imports
        assert "import pyspark" in generator.imports
        assert generator.imports == ["from pyspark import pipelines as dp", "import pyspark"]  # Should be sorted


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
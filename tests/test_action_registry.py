"""Tests for Action Registry - Step 4.1.5."""

import pytest
from lhp.core.action_registry import ActionRegistry
from lhp.models.config import ActionType, LoadSourceType, TransformType, WriteTargetType
from lhp.core.base_generator import BaseActionGenerator
from lhp.generators.load import CloudFilesLoadGenerator, DeltaLoadGenerator
from lhp.generators.transform import SQLTransformGenerator
from lhp.generators.write import StreamingTableWriteGenerator
from lhp.utils.error_formatter import LHPError


class TestActionRegistry:
    """Test action registry functionality."""
    
    def test_registry_initialization(self):
        """Test that registry initializes with all generators."""
        registry = ActionRegistry()
        
        # Check that all generator mappings are initialized
        assert len(registry._load_generators) == 5
        assert len(registry._transform_generators) == 5
        assert len(registry._write_generators) == 2
    
    def test_get_load_generator(self):
        """Test getting load generators."""
        registry = ActionRegistry()
        
        # Test CloudFiles generator
        generator = registry.get_generator(ActionType.LOAD, LoadSourceType.CLOUDFILES)
        assert isinstance(generator, CloudFilesLoadGenerator)
        
        # Test with string sub_type
        generator = registry.get_generator(ActionType.LOAD, "cloudfiles")
        assert isinstance(generator, CloudFilesLoadGenerator)
        
        # Test Delta generator
        generator = registry.get_generator(ActionType.LOAD, LoadSourceType.DELTA)
        assert isinstance(generator, DeltaLoadGenerator)
    
    def test_get_transform_generator(self):
        """Test getting transform generators."""
        registry = ActionRegistry()
        
        # Test SQL transform generator
        generator = registry.get_generator(ActionType.TRANSFORM, TransformType.SQL)
        assert isinstance(generator, SQLTransformGenerator)
        
        # Test with string sub_type
        generator = registry.get_generator(ActionType.TRANSFORM, "sql")
        assert isinstance(generator, SQLTransformGenerator)
    
    def test_get_write_generator(self):
        """Test getting write generators."""
        registry = ActionRegistry()
        
        # Test streaming table generator
        generator = registry.get_generator(ActionType.WRITE, WriteTargetType.STREAMING_TABLE)
        assert isinstance(generator, StreamingTableWriteGenerator)
        
        # Test with string sub_type
        generator = registry.get_generator(ActionType.WRITE, "streaming_table")
        assert isinstance(generator, StreamingTableWriteGenerator)
    
    def test_error_handling(self):
        """Test error handling in registry."""
        registry = ActionRegistry()
        
        # Test invalid action type
        with pytest.raises(ValueError, match="Invalid action type"):
            registry.get_generator("invalid", "cloudfiles")
        
        # Test missing sub_type for load
        with pytest.raises(ValueError, match="Load actions require a sub_type"):
            registry.get_generator(ActionType.LOAD)
        
        # Test unknown load generator type (now raises LHPError)
        with pytest.raises(LHPError, match="Unknown load sub_type"):
            registry.get_generator(ActionType.LOAD, "unknown")
        
        # Test missing sub_type for transform
        with pytest.raises(ValueError, match="Transform actions require a sub_type"):
            registry.get_generator(ActionType.TRANSFORM)
        
        # Test unknown transform generator type (now raises LHPError)
        with pytest.raises(LHPError, match="Unknown transform sub_type"):
            registry.get_generator(ActionType.TRANSFORM, "unknown")
        
        # Test missing sub_type for write
        with pytest.raises(ValueError, match="Write actions require a sub_type"):
            registry.get_generator(ActionType.WRITE)
        
        # Test unknown write generator type (now raises LHPError)
        with pytest.raises(LHPError, match="Unknown write sub_type"):
            registry.get_generator(ActionType.WRITE, "unknown")
    
    def test_list_generators(self):
        """Test listing available generators."""
        registry = ActionRegistry()
        generators = registry.list_generators()
        
        assert "load" in generators
        assert "transform" in generators
        assert "write" in generators
        
        # Check load generators
        assert "cloudfiles" in generators["load"]
        assert "delta" in generators["load"]
        assert "sql" in generators["load"]
        assert "jdbc" in generators["load"]
        assert "python" in generators["load"]
        
        # Check transform generators
        assert "sql" in generators["transform"]
        assert "data_quality" in generators["transform"]
        assert "schema" in generators["transform"]
        assert "python" in generators["transform"]
        assert "temp_table" in generators["transform"]
        
        # Check write generators
        assert "streaming_table" in generators["write"]
        assert "materialized_view" in generators["write"]
    
    def test_is_generator_available(self):
        """Test checking if generator is available."""
        registry = ActionRegistry()
        
        # Test available generators
        assert registry.is_generator_available(ActionType.LOAD, "cloudfiles")
        assert registry.is_generator_available(ActionType.TRANSFORM, "sql")
        assert registry.is_generator_available(ActionType.WRITE, "streaming_table")
        
        # Test unavailable generators
        assert not registry.is_generator_available(ActionType.LOAD, "unknown")
        assert not registry.is_generator_available(ActionType.TRANSFORM, "unknown")
        assert not registry.is_generator_available(ActionType.WRITE, "unknown")
        
        # Test invalid action type
        assert not registry.is_generator_available("invalid", "cloudfiles")
    
    def test_all_generators_return_base_generator(self):
        """Test that all generators inherit from BaseActionGenerator."""
        registry = ActionRegistry()
        
        # Test all load generators
        for load_type in LoadSourceType:
            generator = registry.get_generator(ActionType.LOAD, load_type)
            assert isinstance(generator, BaseActionGenerator)
        
        # Test all transform generators
        for transform_type in TransformType:
            generator = registry.get_generator(ActionType.TRANSFORM, transform_type)
            assert isinstance(generator, BaseActionGenerator)
        
        # Test all write generators
        for write_type in WriteTargetType:
            generator = registry.get_generator(ActionType.WRITE, write_type)
            assert isinstance(generator, BaseActionGenerator)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
"""Tests for operational metadata functionality."""

import pytest
from pathlib import Path
import tempfile
import yaml

from lhp.utils.operational_metadata import OperationalMetadata
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.generators.write.streaming_table import StreamingTableWriteGenerator
from lhp.generators.write.materialized_view import MaterializedViewWriteGenerator


class TestOperationalMetadata:
    """Test operational metadata functionality."""
    
    def test_operational_metadata_initialization(self):
        """Test OperationalMetadata class initialization."""
        metadata = OperationalMetadata()
        assert metadata is not None
        assert '_ingestion_timestamp' in metadata.metadata_columns
    
    def test_should_add_metadata_flowgroup_level(self):
        """Test metadata flag at flowgroup level."""
        metadata = OperationalMetadata()
        
        # FlowGroup with metadata enabled
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=True,
            actions=[]
        )
        
        action = Action(name="test", type=ActionType.WRITE)
        
        assert metadata.should_add_metadata(flowgroup, action, {}) is True
    
    def test_should_add_metadata_action_level(self):
        """Test metadata flag at action level overrides flowgroup."""
        metadata = OperationalMetadata()
        
        # FlowGroup with metadata disabled
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=False,
            actions=[]
        )
        
        # Action with metadata enabled
        action = Action(
            name="test", 
            type=ActionType.WRITE,
            operational_metadata=True
        )
        
        assert metadata.should_add_metadata(flowgroup, action, {}) is True
    
    def test_should_add_metadata_preset_level(self):
        """Test metadata flag from preset configuration."""
        metadata = OperationalMetadata()
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[]
        )
        
        action = Action(name="test", type=ActionType.WRITE)
        preset_config = {"operational_metadata": True}
        
        assert metadata.should_add_metadata(flowgroup, action, preset_config) is True
    
    def test_get_required_imports(self):
        """Test getting required imports for operational metadata."""
        metadata = OperationalMetadata()
        imports = metadata.get_required_imports()
        
        assert 'from pyspark.sql import functions as F' in imports
        assert 'from pyspark.sql.functions import current_timestamp, input_file_name' in imports
    
    def test_generate_metadata_columns(self):
        """Test generating metadata column expressions."""
        metadata = OperationalMetadata()
        metadata.update_context("test_pipeline", "test_flowgroup")
        
        # Test for streaming table
        columns = metadata.generate_metadata_columns("streaming_table")
        assert '_ingestion_timestamp' in columns
        assert '_source_file' in columns
        assert '_pipeline_name' in columns
        assert 'test_pipeline' in columns['_pipeline_name']
        
        # Test for materialized view (no source file)
        columns = metadata.generate_metadata_columns("materialized_view")
        assert '_ingestion_timestamp' in columns
        assert '_source_file' not in columns
    
    def test_streaming_table_with_metadata(self):
        """Test streaming table generation with operational metadata."""
        generator = StreamingTableWriteGenerator()
        
        action = Action(
            name="write_bronze",
            type=ActionType.WRITE,
            operational_metadata=True,
            source="v_raw_data",  # ← Correct format: source is view name
            write_target={  # ← Correct format: write_target contains config
                "type": "streaming_table",
                "database": "bronze",
                "table": "test_table",
                "create_table": True
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        context = {
            "flowgroup": flowgroup,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Check for metadata columns
        assert "_ingestion_timestamp" in code
        assert "_source_file" in code
        assert "_pipeline_name" in code
        assert "F.current_timestamp()" in code
        assert "F.input_file_name()" in code
        assert "from pyspark.sql import functions as F" in generator.imports
    
    def test_materialized_view_with_metadata(self):
        """Test materialized view generation with operational metadata."""
        generator = MaterializedViewWriteGenerator()
        
        action = Action(
            name="write_silver",
            type=ActionType.WRITE,
            source="v_bronze_data",  # ← Correct format: source is view name
            write_target={  # ← Correct format: write_target contains config
                "type": "materialized_view",
                "database": "silver",
                "table": "aggregated_data"
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=True,
            actions=[action]
        )
        
        context = {
            "flowgroup": flowgroup,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Check for metadata columns (no source file for batch)
        assert "_ingestion_timestamp" in code
        assert "_pipeline_name" in code
        assert "_source_file" not in code  # Not included for materialized views
        assert "F.current_timestamp()" in code
    
    def test_metadata_disabled_by_default(self):
        """Test that metadata is disabled by default."""
        generator = StreamingTableWriteGenerator()
        
        action = Action(
            name="write_bronze",
            type=ActionType.WRITE,
            source="v_raw_data",  # ← Correct format: source is view name
            write_target={  # ← Correct format: write_target contains config
                "type": "streaming_table",
                "database": "bronze",
                "table": "test_table",
                "create_table": True
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[action]
        )
        
        context = {
            "flowgroup": flowgroup,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Should not have metadata columns
        assert "_ingestion_timestamp" not in code
        assert "_source_file" not in code
        assert "F.current_timestamp()" not in code
    
    def test_preset_configuration_integration(self):
        """Test operational metadata enabled through preset."""
        generator = StreamingTableWriteGenerator()
        
        action = Action(
            name="write_bronze",
            type=ActionType.WRITE,
            source="v_raw_data",  # ← Correct format: source is view name
            write_target={  # ← Correct format: write_target contains config
                "type": "streaming_table",
                "database": "bronze",
                "table": "test_table",
                "create_table": True
            }
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["bronze_with_metadata"],
            actions=[action]
        )
        
        context = {
            "flowgroup": flowgroup,
            "preset_config": {
                "operational_metadata": True,
                "other_settings": "value"
            }
        }
        
        code = generator.generate(action, context)
        
        # Should have metadata columns from preset
        assert "_ingestion_timestamp" in code
        assert "_pipeline_name" in code
    
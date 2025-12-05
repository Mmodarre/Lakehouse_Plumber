"""Tests for sink write generators of LakehousePlumber."""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.models.config import Action, ActionType, FlowGroup, ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig
from lhp.generators.write.sinks import (
    BaseSinkWriteGenerator,
    DeltaSinkWriteGenerator,
    KafkaSinkWriteGenerator,
    CustomSinkWriteGenerator
)
from lhp.generators.write.sink import SinkWriteGenerator
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestBaseSinkWriteGenerator:
    """Test base sink write generator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create a concrete implementation for testing abstract base class
        class ConcreteSinkGenerator(BaseSinkWriteGenerator):
            def generate(self, action, context):
                return "generated_code"
        
        self.generator = ConcreteSinkGenerator()
    
    def test_initialization(self):
        """Test that base sink generator initializes correctly."""
        assert self.generator is not None
        imports = self.generator.get_import_manager().get_consolidated_imports()
        assert "from pyspark import pipelines as dp" in imports
        assert "from pyspark.sql import functions as F" in imports
    
    def test_extract_source_views_string(self):
        """Test extracting source views from string."""
        views = self.generator._extract_source_views("v_customers")
        assert views == ["v_customers"]
    
    def test_extract_source_views_list_strings(self):
        """Test extracting source views from list of strings."""
        views = self.generator._extract_source_views(["v_customers", "v_orders"])
        assert views == ["v_customers", "v_orders"]
    
    def test_extract_source_views_list_dicts(self):
        """Test extracting source views from list of dicts with view keys."""
        views = self.generator._extract_source_views([
            {"view": "v_customers"},
            {"view": "v_orders"}
        ])
        assert views == ["v_customers", "v_orders"]
    
    def test_extract_source_views_list_mixed(self):
        """Test extracting source views from mixed list."""
        views = self.generator._extract_source_views([
            "v_customers",
            {"view": "v_orders"}
        ])
        assert views == ["v_customers", "v_orders"]
    
    def test_extract_source_views_dict_with_view(self):
        """Test extracting source views from dict with view key."""
        views = self.generator._extract_source_views({"view": "v_customers"})
        assert views == ["v_customers"]
    
    def test_extract_source_views_dict_without_view(self):
        """Test extracting source views from dict without view key."""
        views = self.generator._extract_source_views({"database": "test", "table": "customers"})
        assert views == []
    
    def test_extract_source_views_empty_list(self):
        """Test extracting source views from empty list."""
        views = self.generator._extract_source_views([])
        assert views == []
    
    def test_extract_source_views_none(self):
        """Test extracting source views from None."""
        views = self.generator._extract_source_views(None)
        assert views == []
    
    def test_get_operational_metadata_no_config(self):
        """Test operational metadata with no project config."""
        action = Action(name="test", type=ActionType.WRITE, write_target={"type": "sink", "sink_type": "delta"})
        context = {}
        
        add_metadata, metadata_columns = self.generator._get_operational_metadata(action, context)
        
        assert add_metadata is False
        assert metadata_columns == {}
    
    def test_get_operational_metadata_with_config(self):
        """Test operational metadata with project config."""
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        action = Action(
            name="test",
            type=ActionType.WRITE,
            write_target={"type": "sink", "sink_type": "delta"}
        )
        
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        add_metadata, metadata_columns = self.generator._get_operational_metadata(action, context)
        
        assert add_metadata is True
        assert "_ingestion_timestamp" in metadata_columns
    
    def test_get_operational_metadata_with_preset(self):
        """Test operational metadata with preset config."""
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                },
                presets={
                    "standard": {
                        "columns": ["_ingestion_timestamp"]
                    }
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            presets=["standard"]
        )
        
        action = Action(
            name="test",
            type=ActionType.WRITE,
            write_target={"type": "sink", "sink_type": "delta"},
            operational_metadata=["_ingestion_timestamp"]
        )
        
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        add_metadata, metadata_columns = self.generator._get_operational_metadata(action, context)
        
        assert add_metadata is True
        assert "_ingestion_timestamp" in metadata_columns


class TestDeltaSinkWriteGenerator:
    """Test Delta sink write generator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = DeltaSinkWriteGenerator()
    
    def test_generate_with_tablename(self):
        """Test Delta sink generation with tableName option."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_customers",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert 'name="delta_sink"' in code
        assert 'format="delta"' in code
        assert "tableName" in code or '"tableName"' in code
        assert "f_delta_sink_1" in code
        assert "v_customers" in code
    
    def test_generate_with_path(self):
        """Test Delta sink generation with path option."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_customers",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "path": "/path/to/delta/table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert 'name="delta_sink"' in code
        assert 'format="delta"' in code
        assert "path" in code or '"path"' in code
        assert "f_delta_sink_1" in code
    
    def test_generate_with_multiple_sources(self):
        """Test Delta sink generation with multiple source views."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source=["v_customers", "v_orders"],
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert "f_delta_sink_1" in code
        assert "f_delta_sink_2" in code
        assert "v_customers" in code
        assert "v_orders" in code
    
    def test_generate_with_comment(self):
        """Test Delta sink generation with comment."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_customers",
            description="Test description",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "comment": "Custom comment",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "Custom comment" in code
    
    def test_generate_with_description_fallback(self):
        """Test Delta sink generation using description as comment fallback."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_customers",
            description="Test description",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "Test description" in code
    
    def test_generate_with_operational_metadata(self):
        """Test Delta sink generation with operational metadata."""
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_customers",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        code = self.generator.generate(action, context)
        
        assert "Add operational metadata columns" in code
        assert "_ingestion_timestamp" in code


class TestKafkaSinkWriteGenerator:
    """Test Kafka sink write generator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = KafkaSinkWriteGenerator()
    
    def test_generate_basic_kafka(self):
        """Test basic Kafka sink generation."""
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert 'name="kafka_sink"' in code
        assert 'format="kafka"' in code
        assert "Kafka sink" in code
        assert "kafka.bootstrap.servers" in code or '"kafka.bootstrap.servers"' in code
        assert "test_topic" in code
        assert "f_kafka_sink_1" in code
        assert "v_data" in code
    
    def test_generate_event_hubs_detection(self):
        """Test Event Hubs detection via OAUTHBEARER mechanism."""
        action = Action(
            name="write_event_hubs_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "event_hubs_sink",
                "bootstrap_servers": "my-ns.servicebus.windows.net:9093",
                "topic": "my-event-hub",
                "options": {
                    "kafka.sasl.mechanism": "OAUTHBEARER",
                    "kafka.sasl.jaas.config": "test_config",
                    "kafka.sasl.oauthbearer.token.endpoint.url": "https://token.endpoint",
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.login.callback.handler.class": "test_handler"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "Event Hubs sink" in code
        assert "Event Hubs" in code
    
    def test_generate_with_options(self):
        """Test Kafka sink generation with additional options."""
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "PLAIN"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert "kafka.security.protocol" in code or '"kafka.security.protocol"' in code
    
    def test_generate_with_multiple_sources(self):
        """Test Kafka sink generation with multiple source views."""
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source=["v_data1", "v_data2"],
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "f_kafka_sink_1" in code
        assert "f_kafka_sink_2" in code
        assert "v_data1" in code
        assert "v_data2" in code
    
    def test_generate_with_comment(self):
        """Test Kafka sink generation with comment."""
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source="v_data",
            description="Test description",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic",
                "comment": "Custom Kafka comment"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "Custom Kafka comment" in code
    
    def test_generate_with_operational_metadata(self):
        """Test Kafka sink generation with operational metadata."""
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic"
            }
        )
        
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        code = self.generator.generate(action, context)
        
        assert "Add operational metadata columns FIRST" in code
        assert "_ingestion_timestamp" in code


class TestCustomSinkWriteGenerator:
    """Test custom sink write generator."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = CustomSinkWriteGenerator()
        self.temp_dir = Path(tempfile.mkdtemp())
        self.project_root = self.temp_dir / "test_project"
        self.project_root.mkdir()
    
    def teardown_method(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    def test_extract_datasink_format_name_success(self):
        """Test successful extraction of format name from class."""
        source_code = """
class MyCustomDataSink(DataSink):
    @classmethod
    def name(cls):
        return "my_custom_format"
"""
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "my_custom_format"
    
    def test_extract_datasink_format_name_with_inheritance(self):
        """Test extraction with class inheritance."""
        source_code = """
class MyCustomDataSink(DataSink, SomeMixin):
    @classmethod
    def name(cls):
        return "custom_format"
"""
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "custom_format"
    
    def test_extract_datasink_format_name_without_inheritance(self):
        """Test extraction without class inheritance."""
        source_code = """
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "simple_format"
"""
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "simple_format"
    
    def test_extract_datasink_format_name_missing_class(self):
        """Test extraction when class is not found."""
        source_code = """
class OtherClass:
    pass
"""
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "MyCustomDataSink"  # Falls back to class name
    
    def test_extract_datasink_format_name_missing_name_method(self):
        """Test extraction when name() method is missing."""
        source_code = """
class MyCustomDataSink:
    def other_method(self):
        pass
"""
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "MyCustomDataSink"  # Falls back to class name
    
    def test_extract_datasink_format_name_regex_failure(self):
        """Test extraction when regex fails."""
        source_code = "invalid python code {"
        format_name = self.generator._extract_datasink_format_name(source_code, "MyCustomDataSink")
        assert format_name == "MyCustomDataSink"  # Falls back to class name
    
    def test_generate_with_valid_sink_file(self):
        """Test generation with valid custom sink file."""
        # Create sink file
        sink_dir = self.project_root / "sinks"
        sink_dir.mkdir()
        sink_file = sink_dir / "my_sink.py"
        sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_custom_format"
""")
        
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        context = {
            "spec_dir": self.project_root
        }
        
        code = self.generator.generate(action, context)
        
        assert "dp.create_sink" in code
        assert 'name="custom_sink"' in code
        assert 'format="my_custom_format"' in code
        assert "MyCustomDataSink" in code
        assert "spark.dataSource.register" in code
        assert self.generator.custom_sink_code is not None
        assert self.generator.sink_file_path == "sinks/my_sink.py"
    
    def test_generate_missing_module_path(self):
        """Test generation with missing module_path raises error."""
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        with pytest.raises(Exception) as exc_info:
            self.generator.generate(action, {"spec_dir": self.project_root})
        
        assert "module_path" in str(exc_info.value).lower()
    
    def test_generate_missing_custom_sink_class(self):
        """Test generation with missing custom_sink_class raises error."""
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py"
            }
        )
        
        with pytest.raises(Exception) as exc_info:
            self.generator.generate(action, {"spec_dir": self.project_root})
        
        assert "custom_sink_class" in str(exc_info.value).lower()
    
    def test_generate_file_not_found(self):
        """Test generation when sink file doesn't exist."""
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/nonexistent.py",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        with pytest.raises(FileNotFoundError):
            self.generator.generate(action, {"spec_dir": self.project_root})
    
    def test_generate_with_substitution_manager(self):
        """Test generation with substitution manager."""
        sink_dir = self.project_root / "sinks"
        sink_dir.mkdir()
        sink_file = sink_dir / "my_sink.py"
        sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "${sink_format}"
""")
        
        # Create substitution manager without file (it can work without one)
        substitution_mgr = EnhancedSubstitutionManager(substitution_file=None, env="dev")
        substitution_mgr.mappings["sink_format"] = "substituted_format"
        
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        context = {
            "spec_dir": self.project_root,
            "substitution_manager": substitution_mgr
        }
        
        code = self.generator.generate(action, context)
        
        assert "substituted_format" in self.generator.custom_sink_code
    
    def test_generate_with_options(self):
        """Test generation with custom options."""
        sink_dir = self.project_root / "sinks"
        sink_dir.mkdir()
        sink_file = sink_dir / "my_sink.py"
        sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_format"
""")
        
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink",
                "options": {
                    "endpoint": "https://api.example.com",
                    "api_key": "secret"
                }
            }
        )
        
        context = {
            "spec_dir": self.project_root
        }
        
        code = self.generator.generate(action, context)
        
        assert "endpoint" in code or '"endpoint"' in code
        assert "api_key" in code or '"api_key"' in code
    
    def test_generate_with_multiple_sources(self):
        """Test generation with multiple source views."""
        sink_dir = self.project_root / "sinks"
        sink_dir.mkdir()
        sink_file = sink_dir / "my_sink.py"
        sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_format"
""")
        
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source=["v_data1", "v_data2"],
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        context = {
            "spec_dir": self.project_root
        }
        
        code = self.generator.generate(action, context)
        
        assert "f_custom_sink_1" in code
        assert "f_custom_sink_2" in code
        assert "v_data1" in code
        assert "v_data2" in code
    
    def test_generate_with_operational_metadata(self):
        """Test generation with operational metadata."""
        sink_dir = self.project_root / "sinks"
        sink_dir.mkdir()
        sink_file = sink_dir / "my_sink.py"
        sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_format"
""")
        
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        action = Action(
            name="write_custom_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        context = {
            "spec_dir": self.project_root,
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        code = self.generator.generate(action, context)
        
        assert "Add operational metadata columns" in code
        assert "_ingestion_timestamp" in code


class TestSinkWriteGenerator:
    """Test sink write generator dispatcher."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.generator = SinkWriteGenerator()
    
    def test_dispatch_to_delta(self):
        """Test dispatching to delta generator."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert 'format="delta"' in code
    
    def test_dispatch_to_kafka(self):
        """Test dispatching to kafka generator."""
        action = Action(
            name="write_kafka_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_sink",
                "bootstrap_servers": "localhost:9092",
                "topic": "test_topic"
            }
        )
        
        code = self.generator.generate(action, {})
        
        assert "dp.create_sink" in code
        assert 'format="kafka"' in code
    
    def test_dispatch_to_custom(self):
        """Test dispatching to custom generator."""
        temp_dir = Path(tempfile.mkdtemp())
        project_root = temp_dir / "test_project"
        project_root.mkdir()
        
        try:
            sink_dir = project_root / "sinks"
            sink_dir.mkdir()
            sink_file = sink_dir / "my_sink.py"
            sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_format"
""")
            
            action = Action(
                name="write_custom_sink",
                type=ActionType.WRITE,
                source="v_data",
                write_target={
                    "type": "sink",
                    "sink_type": "custom",
                    "sink_name": "custom_sink",
                    "module_path": "sinks/my_sink.py",
                    "custom_sink_class": "MyCustomDataSink"
                }
            )
            
            context = {
                "spec_dir": project_root
            }
            
            code = self.generator.generate(action, context)
            
            assert "dp.create_sink" in code
            assert "MyCustomDataSink" in code
            assert hasattr(self.generator, "custom_sink_code")
            assert self.generator.custom_sink_code is not None
        
        finally:
            shutil.rmtree(temp_dir)
    
    def test_unsupported_sink_type(self):
        """Test error when sink_type is unsupported."""
        action = Action(
            name="write_unknown_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "unknown",
                "sink_name": "unknown_sink"
            }
        )
        
        with pytest.raises(ValueError) as exc_info:
            self.generator.generate(action, {})
        
        assert "Unsupported sink_type" in str(exc_info.value)
        assert "unknown" in str(exc_info.value)
    
    def test_import_merging(self):
        """Test that imports are merged from sub-generators."""
        action = Action(
            name="write_delta_sink",
            type=ActionType.WRITE,
            source="v_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "delta_sink",
                "options": {
                    "tableName": "catalog.schema.table"
                }
            }
        )
        
        self.generator.generate(action, {})
        
        imports = self.generator.get_import_manager().get_consolidated_imports()
        assert "from pyspark import pipelines as dp" in imports
        assert "from pyspark.sql import functions as F" in imports
    
    def test_custom_sink_code_forwarding(self):
        """Test that custom sink code is forwarded from sub-generator."""
        temp_dir = Path(tempfile.mkdtemp())
        project_root = temp_dir / "test_project"
        project_root.mkdir()
        
        try:
            sink_dir = project_root / "sinks"
            sink_dir.mkdir()
            sink_file = sink_dir / "my_sink.py"
            sink_file.write_text("""
class MyCustomDataSink:
    @classmethod
    def name(cls):
        return "my_format"
""")
            
            action = Action(
                name="write_custom_sink",
                type=ActionType.WRITE,
                source="v_data",
                write_target={
                    "type": "sink",
                    "sink_type": "custom",
                    "sink_name": "custom_sink",
                    "module_path": "sinks/my_sink.py",
                    "custom_sink_class": "MyCustomDataSink"
                }
            )
            
            context = {
                "spec_dir": project_root
            }
            
            self.generator.generate(action, context)
            
            assert hasattr(self.generator, "custom_sink_code")
            assert self.generator.custom_sink_code is not None
            assert "MyCustomDataSink" in self.generator.custom_sink_code
            assert hasattr(self.generator, "sink_file_path")
            assert self.generator.sink_file_path == "sinks/my_sink.py"
        
        finally:
            shutil.rmtree(temp_dir)


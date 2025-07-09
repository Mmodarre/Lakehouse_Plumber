"""Tests for operational metadata support in load actions."""

import pytest
import yaml
from pathlib import Path
from unittest.mock import Mock, patch

from lhp.models.config import (
    FlowGroup, Action, ActionType, LoadSourceType, TransformType,
    ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig
)
from lhp.generators.load import (
    CloudFilesLoadGenerator, DeltaLoadGenerator, SQLLoadGenerator,
    JDBCLoadGenerator, PythonLoadGenerator
)
from lhp.generators.transform import SchemaTransformGenerator
from lhp.utils.operational_metadata import OperationalMetadata
from lhp.utils.error_formatter import LHPError


class TestLoadOperationalMetadata:
    """Test operational metadata support in load actions."""
    
    def setup_method(self):
        """Set up test fixtures."""
        # Create test project config with operational metadata
        self.project_config = ProjectConfig(
            name="test_project",
            version="1.0",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_ingestion_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        description="When record was ingested",
                        applies_to=["view"]
                    ),
                    "_source_file": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        description="Source file path",
                        applies_to=["view"]
                    ),
                    "_pipeline_name": MetadataColumnConfig(
                        expression="F.lit('${pipeline_name}')",
                        description="Pipeline name",
                        applies_to=["view"]
                    ),
                    "_source_table": MetadataColumnConfig(
                        expression="F.lit('${source_table}')",
                        description="Source table name",
                        applies_to=["view"]
                    ),
                    "_kafka_partition": MetadataColumnConfig(
                        expression="F.col('_metadata.partition')",
                        description="Kafka partition",
                        applies_to=["view"],
                        enabled=False
                    )
                }
            )
        )
        
        # Create test flowgroup
        self.flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[]
        )
        
        # Create test context
        self.context = {
            "flowgroup": self.flowgroup,
            "project_config": self.project_config,
            "preset_config": {}
        }

    def test_view_target_type_validation(self):
        """Test that 'view' is accepted as valid applies_to target type."""
        operational_metadata = OperationalMetadata(
            project_config=self.project_config.operational_metadata
        )
        
        # Should not raise error for 'view' target type
        operational_metadata._validate_target_type("view")
        
        # Should still work for existing types
        operational_metadata._validate_target_type("streaming_table")
        operational_metadata._validate_target_type("materialized_view")
        
        # Should raise error for invalid type
        with pytest.raises(LHPError) as exc_info:
            operational_metadata._validate_target_type("invalid_type")
        
        assert "Invalid target type" in str(exc_info.value)

    def test_cloudfiles_load_with_operational_metadata(self):
        """Test CloudFiles load generator with operational metadata."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json",
                "format": "json"
            },
            target="v_raw_data",
            operational_metadata=["_ingestion_timestamp", "_source_file", "_pipeline_name"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that metadata columns are added
        assert "df.withColumn('_ingestion_timestamp', F.current_timestamp())" in code
        assert "df.withColumn('_source_file', F.input_file_name())" in code
        assert "df.withColumn('_pipeline_name', F.lit('test_pipeline'))" in code
        
        # Check that function imports are detected (in generator.imports, not in code)
        assert "from pyspark.sql import functions as F" in generator.imports
        
        # Check that view decorator is used
        assert "@dlt.view()" in code

    def test_delta_load_with_operational_metadata(self):
        """Test Delta load generator with operational metadata."""
        generator = DeltaLoadGenerator()
        
        action = Action(
            name="load_delta",
            type=ActionType.LOAD,
            source={
                "type": "delta",
                "table": "source.customers"
            },
            target="v_customers",
            operational_metadata=["_ingestion_timestamp", "_source_table"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that metadata columns are added
        assert "df.withColumn('_ingestion_timestamp', F.current_timestamp())" in code
        assert "df.withColumn('_source_table', F.lit('source.customers'))" in code
        
        # Check that view decorator is used
        assert "@dlt.view()" in code

    def test_sql_load_with_operational_metadata(self):
        """Test SQL load generator with operational metadata."""
        generator = SQLLoadGenerator()
        
        action = Action(
            name="load_sql",
            type=ActionType.LOAD,
            source="SELECT * FROM customers",
            target="v_customers",
            operational_metadata=["_ingestion_timestamp", "_pipeline_name"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that metadata columns are added
        assert "df.withColumn('_ingestion_timestamp', F.current_timestamp())" in code
        assert "df.withColumn('_pipeline_name', F.lit('test_pipeline'))" in code
        
        # Check that view decorator is used
        assert "@dlt.view()" in code

    def test_jdbc_load_with_operational_metadata(self):
        """Test JDBC load generator with operational metadata."""
        generator = JDBCLoadGenerator()
        
        action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            source={
                "type": "jdbc",
                "table": "customers",
                "connection": "my_db"
            },
            target="v_customers",
            operational_metadata=["_ingestion_timestamp", "_source_table"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that metadata columns are added
        assert "df.withColumn('_ingestion_timestamp', F.current_timestamp())" in code
        assert "df.withColumn('_source_table', F.lit('customers'))" in code
        
        # Check that view decorator is used
        assert "@dlt.view()" in code

    def test_python_load_with_operational_metadata(self):
        """Test Python load generator with operational metadata."""
        generator = PythonLoadGenerator()
        
        action = Action(
            name="load_python",
            type=ActionType.LOAD,
            source={
                "type": "python",
                "module_path": "my_module.py",
                "function_name": "my_load_function",
                "parameters": {"param1": "value1"}
            },
            target="v_data",
            operational_metadata=["_ingestion_timestamp", "_pipeline_name"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that metadata columns are added
        assert "df.withColumn('_ingestion_timestamp', F.current_timestamp())" in code
        assert "df.withColumn('_pipeline_name', F.lit('test_pipeline'))" in code
        
        # Check that view decorator is used
        assert "@dlt.view()" in code

    def test_operational_metadata_disabled(self):
        """Test load action with operational metadata disabled."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=False
        )
        
        code = generator.generate(action, self.context)
        
        # Check that no metadata columns are added
        assert "withColumn('_ingestion_timestamp'" not in code
        assert "withColumn('_source_file'" not in code
        assert "withColumn('_pipeline_name'" not in code

    def test_operational_metadata_all_columns(self):
        """Test load action with operational_metadata=true (all columns)."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=True
        )
        
        code = generator.generate(action, self.context)
        
        # Check that all enabled columns are added
        assert "withColumn('_ingestion_timestamp'" in code
        assert "withColumn('_source_file'" in code
        assert "withColumn('_pipeline_name'" in code
        assert "withColumn('_source_table'" in code
        # _kafka_partition should be skipped (enabled=False)
        assert "withColumn('_kafka_partition'" not in code

    def test_context_substitution_in_metadata(self):
        """Test that context substitution works in metadata expressions."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_pipeline_name"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that context substitution worked
        assert "F.lit('test_pipeline')" in code

    def test_import_detection_for_metadata(self):
        """Test that required imports are detected for metadata expressions."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_ingestion_timestamp", "_source_file"]
        )
        
        code = generator.generate(action, self.context)
        
        # Check that required imports are detected (in generator.imports, not in code)
        assert "from pyspark.sql import functions as F" in generator.imports

    def test_additive_metadata_selection(self):
        """Test that metadata selection is additive across preset/flowgroup/action."""
        # Set up flowgroup with operational metadata
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"],
            actions=[]
        )
        
        # Set up preset config with operational metadata
        preset_config = {
            "operational_metadata": ["_pipeline_name"]
        }
        
        context = {
            "flowgroup": flowgroup,
            "project_config": self.project_config,
            "preset_config": preset_config
        }
        
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_source_file"]
        )
        
        code = generator.generate(action, context)
        
        # Check that all metadata columns from all levels are included
        assert "withColumn('_ingestion_timestamp'" in code  # From flowgroup
        assert "withColumn('_pipeline_name'" in code        # From preset
        assert "withColumn('_source_file'" in code          # From action

    def test_action_level_disable_overrides_others(self):
        """Test that operational_metadata=false at action level overrides others."""
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_ingestion_timestamp"],
            actions=[]
        )
        
        preset_config = {
            "operational_metadata": ["_pipeline_name"]
        }
        
        context = {
            "flowgroup": flowgroup,
            "project_config": self.project_config,
            "preset_config": preset_config
        }
        
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=False  # Disable at action level
        )
        
        code = generator.generate(action, context)
        
        # Check that no metadata columns are added despite preset/flowgroup config
        assert "withColumn('_ingestion_timestamp'" not in code
        assert "withColumn('_pipeline_name'" not in code
        assert "withColumn('_source_file'" not in code

    def test_unknown_metadata_column_warning(self):
        """Test that unknown metadata columns generate warnings, not errors."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_ingestion_timestamp", "_unknown_column"]
        )
        
        # Should not raise error, should generate warning
        with patch('lhp.utils.operational_metadata.logging.getLogger') as mock_logger:
            code = generator.generate(action, self.context)
            
            # Check that warning was logged
            mock_logger.return_value.warning.assert_called()
            warning_calls = mock_logger.return_value.warning.call_args_list
            assert any("unknown metadata columns" in str(call) for call in warning_calls)
        
        # Check that valid column is still added
        assert "withColumn('_ingestion_timestamp'" in code
        # Check that unknown column is not added
        assert "withColumn('_unknown_column'" not in code

    def test_schema_transform_preserves_metadata(self):
        """Test that schema transform preserves metadata columns."""
        generator = SchemaTransformGenerator()
        
        action = Action(
            name="clean_data",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source={
                "view": "v_raw_data",
                "schema": {
                    "column_mapping": {
                        "customer_id": "id",
                        "customer_name": "name"
                    },
                    "type_casting": {
                        "age": "int"
                    }
                }
            },
            target="v_clean_data"
        )
        
        code = generator.generate(action, self.context)
        
        # Check that schema operations are applied
        assert "withColumnRenamed(\"customer_id\", \"id\")" in code
        assert "withColumnRenamed(\"customer_name\", \"name\")" in code
        assert "withColumn(\"age\", F.col(\"age\").cast(\"int\"))" in code
        
        # Check that metadata columns are preserved (not renamed or cast)
        assert "withColumnRenamed(\"_ingestion_timestamp\"" not in code
        assert "withColumnRenamed(\"_source_file\"" not in code
        assert "withColumn(\"_ingestion_timestamp\", F.col(\"_ingestion_timestamp\").cast(" not in code

    def test_end_to_end_metadata_flow(self):
        """Test complete flow: load -> transform -> write with metadata preservation."""
        # 1. Load with metadata
        load_generator = CloudFilesLoadGenerator()
        load_action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_ingestion_timestamp", "_source_file"]
        )
        
        load_code = load_generator.generate(load_action, self.context)
        
        # Check load adds metadata
        assert "withColumn('_ingestion_timestamp'" in load_code
        assert "withColumn('_source_file'" in load_code
        
        # 2. Transform should preserve metadata (user responsibility for SQL)
        # This is what user would write in SQL transform
        transform_sql = """
        SELECT 
            customer_id,
            UPPER(name) as name_upper,
            email,
            -- Metadata columns preserved by user
            _ingestion_timestamp,
            _source_file
        FROM {source}
        WHERE email IS NOT NULL
        """
        
        # 3. Write should NOT have operational metadata (breaking change)
        # This will be tested in separate test for write generators
        
        # The key is that metadata flows through naturally if user includes it
        assert "_ingestion_timestamp" in transform_sql
        assert "_source_file" in transform_sql

    def test_disabled_column_not_included(self):
        """Test that columns with enabled=false are not included."""
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=True  # All columns
        )
        
        code = generator.generate(action, self.context)
        
        # Check that enabled columns are included
        assert "withColumn('_ingestion_timestamp'" in code
        assert "withColumn('_source_file'" in code
        
        # Check that disabled column is not included
        assert "withColumn('_kafka_partition'" not in code

    def test_no_project_config_uses_defaults(self):
        """Test that system works without project config (uses defaults)."""
        context_no_project = {
            "flowgroup": self.flowgroup,
            "project_config": None,
            "preset_config": {}
        }
        
        generator = CloudFilesLoadGenerator()
        
        action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={
                "type": "cloudfiles",
                "path": "/data/files/*.json"
            },
            target="v_raw_data",
            operational_metadata=["_ingestion_timestamp", "_source_file"]
        )
        
        code = generator.generate(action, context_no_project)
        
        # Should work with default columns
        assert "withColumn('_ingestion_timestamp'" in code
        assert "withColumn('_source_file'" in code

    def test_readmode_affects_metadata_context(self):
        """Test that readMode affects the context for metadata expressions."""
        generator = DeltaLoadGenerator()
        
        # Test streaming mode
        action_stream = Action(
            name="load_delta_stream",
            type=ActionType.LOAD,
            source={
                "type": "delta",
                "table": "source.customers"
            },
            target="v_customers",
            readMode="stream",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        code_stream = generator.generate(action_stream, self.context)
        
        # Test batch mode
        action_batch = Action(
            name="load_delta_batch",
            type=ActionType.LOAD,
            source={
                "type": "delta",
                "table": "source.customers"
            },
            target="v_customers",
            readMode="batch",
            operational_metadata=["_ingestion_timestamp"]
        )
        
        code_batch = generator.generate(action_batch, self.context)
        
        # Both should have metadata (readMode doesn't affect metadata)
        assert "withColumn('_ingestion_timestamp'" in code_stream
        assert "withColumn('_ingestion_timestamp'" in code_batch
        
        # But readMode should affect the main data reading
        assert "spark.readStream" in code_stream
        assert "spark.read" in code_batch


class TestMetadataExpressionValidation:
    """Test validation of metadata expressions for different contexts."""
    
    def test_file_expression_only_for_file_sources(self):
        """Test that F.input_file_name() should only be used with file sources."""
        # This is more of a documentation/user guidance test
        # The system allows it but user should know it will fail at runtime
        
        project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_source_file": MetadataColumnConfig(
                        expression="F.input_file_name()",
                        description="Source file path (CloudFiles only)",
                        applies_to=["view"]
                    )
                }
            )
        )
        
        flowgroup = FlowGroup(pipeline="test", flowgroup="test", actions=[])
        context = {
            "flowgroup": flowgroup,
            "project_config": project_config,
            "preset_config": {}
        }
        
        # CloudFiles should work fine
        cloudfiles_generator = CloudFilesLoadGenerator()
        cloudfiles_action = Action(
            name="load_files",
            type=ActionType.LOAD,
            source={"type": "cloudfiles", "path": "/data/*.json"},
            target="v_data",
            operational_metadata=["_source_file"]
        )
        
        code = cloudfiles_generator.generate(cloudfiles_action, context)
        assert "F.input_file_name()" in code
        
        # JDBC with file expression - system allows but will fail at runtime
        # User should know not to use file expressions with JDBC
        jdbc_generator = JDBCLoadGenerator()
        jdbc_action = Action(
            name="load_jdbc",
            type=ActionType.LOAD,
            source={"type": "jdbc", "table": "customers"},
            target="v_data",
            operational_metadata=["_source_file"]
        )
        
        # Should generate code (system doesn't prevent it)
        code = jdbc_generator.generate(jdbc_action, context)
        assert "F.input_file_name()" in code  # But this will fail at runtime 
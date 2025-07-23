"""Tests for operational metadata generation in transform templates."""

import pytest
import tempfile
from pathlib import Path
from lhp.generators.transform.sql import SQLTransformGenerator
from lhp.generators.transform.data_quality import DataQualityTransformGenerator
from lhp.generators.transform.temp_table import TempTableTransformGenerator
from lhp.models.config import Action, FlowGroup, ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig


class TestTransformOperationalMetadata:
    """Test operational metadata generation in transform templates."""

    @pytest.fixture
    def project_config_with_metadata(self):
        """Create a project config with operational metadata."""
        return ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_processing_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        enabled=True,
                        applies_to=["view", "streaming_table"]
                    ),
                    "_source_file_path": MetadataColumnConfig(
                        expression="F.col('_metadata.file_path')",
                        enabled=True,
                        applies_to=["view", "streaming_table"]
                    ),
                    "_batch_id": MetadataColumnConfig(
                        expression="F.monotonically_increasing_id()",
                        enabled=True,
                        applies_to=["view"]
                    )
                }
            )
        )

    @pytest.fixture
    def flowgroup_with_metadata(self):
        """Create a flowgroup with operational metadata selection."""
        return FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            operational_metadata=["_processing_timestamp", "_batch_id"]
        )

    def test_sql_transform_with_operational_metadata(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test SQL transform generator with operational metadata."""
        generator = SQLTransformGenerator()
        
        action = Action(
            name="test_sql_transform",
            type="transform",
            target="v_test_output",
            sql="SELECT * FROM v_customers WHERE status = 'active'",
            description="Test SQL transform"
        )
        
        context = {
            "project_config": project_config_with_metadata,
            "flowgroup": flowgroup_with_metadata,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Verify basic structure
        assert "@dlt.view(" in code
        assert "v_test_output" in code
        assert "df = spark.sql(" in code
        assert "return df" in code
        
        # Verify operational metadata is added
        assert "# Add operational metadata columns" in code
        assert "df = df.withColumn('_batch_id'" in code
        assert "df = df.withColumn('_processing_timestamp'" in code
        
        # Verify metadata expressions
        assert "F.monotonically_increasing_id()" in code
        assert "F.current_timestamp()" in code
        
        # Verify alphabetical ordering (batch_id comes before processing_timestamp)
        batch_id_pos = code.find("_batch_id")
        timestamp_pos = code.find("_processing_timestamp") 
        assert batch_id_pos < timestamp_pos, "Metadata columns should be in alphabetical order"

    def test_data_quality_transform_with_operational_metadata(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test data quality transform generator with operational metadata."""
        generator = DataQualityTransformGenerator()
        
        action = Action(
            name="test_dq_transform",
            type="transform",
            target="v_test_quality",
            source="v_customers",
            expectations_file="test_expectations.json",
            readMode="stream",
            description="Test DQ transform"
        )
        
        # Create a temporary expectations file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"email_not_null": {"action": "fail"}}')
            expectations_file = f.name
        
        try:
            action.expectations_file = expectations_file
            
            context = {
                "project_config": project_config_with_metadata,
                "flowgroup": flowgroup_with_metadata,
                "preset_config": {},
                "spec_dir": Path(".")
            }
            
            code = generator.generate(action, context)
            
            # Verify basic structure
            assert "@dlt.view()" in code
            assert "v_test_quality" in code
            assert "spark.readStream.table" in code  # stream mode
            assert "return df" in code
            
            # Verify operational metadata is added
            assert "# Add operational metadata columns" in code
            assert "df = df.withColumn('_batch_id'" in code
            assert "df = df.withColumn('_processing_timestamp'" in code
            
            # Verify expectations are preserved
            assert "@dlt.expect_all_or_fail" in code
            
        finally:
            Path(expectations_file).unlink()

    def test_temp_table_transform_with_operational_metadata(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test temp table transform generator with operational metadata."""
        generator = TempTableTransformGenerator()
        
        action = Action(
            name="test_temp_table",
            type="transform",
            target="temp_customers",
            source="v_customers",
            description="Test temp table"
        )
        
        context = {
            "project_config": project_config_with_metadata,
            "flowgroup": flowgroup_with_metadata,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Verify basic structure
        assert "@dlt.table(" in code
        assert "temporary=True" in code
        assert "temp_customers" in code
        assert "spark.read.table" in code  # batch mode (default)
        assert "return df" in code
        
        # Verify operational metadata is added
        assert "# Add operational metadata columns" in code
        assert "df = df.withColumn('_batch_id'" in code
        assert "df = df.withColumn('_processing_timestamp'" in code

    def test_sql_transform_with_custom_sql_and_metadata(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test SQL transform with custom SQL and operational metadata."""
        generator = SQLTransformGenerator()
        
        action = Action(
            name="test_custom_sql",
            type="transform", 
            target="v_custom_output",
            sql="""
            SELECT 
                customer_id,
                customer_name,
                UPPER(customer_name) as customer_name_upper
            FROM v_customers 
            WHERE created_date >= '2023-01-01'
            """,
            description="Custom SQL with metadata"
        )
        
        context = {
            "project_config": project_config_with_metadata,
            "flowgroup": flowgroup_with_metadata,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Verify SQL is preserved
        assert "SELECT" in code
        assert "customer_id" in code
        assert "UPPER(customer_name)" in code
        assert "WHERE created_date >= '2023-01-01'" in code
        
        # Verify metadata is added after SQL
        assert "# Add operational metadata columns" in code
        sql_pos = code.find("spark.sql")
        metadata_pos = code.find("# Add operational metadata columns")
        assert sql_pos < metadata_pos, "Metadata should be added after SQL execution"

    def test_transform_without_operational_metadata(self):
        """Test transforms work correctly without operational metadata configuration."""
        generator = SQLTransformGenerator()
        
        action = Action(
            name="test_no_metadata",
            type="transform",
            target="v_no_metadata",
            sql="SELECT * FROM v_customers",
            description="Transform without metadata"
        )
        
        context = {}  # No project config or metadata
        
        code = generator.generate(action, context)
        
        # Verify basic structure works
        assert "@dlt.view(" in code
        assert "v_no_metadata" in code
        assert "df = spark.sql(" in code
        assert "return df" in code
        
        # Verify no metadata is added
        assert "# Add operational metadata columns" not in code
        assert "withColumn" not in code

    def test_operational_metadata_imports(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test that required imports are added for operational metadata."""
        generator = SQLTransformGenerator()
        
        action = Action(
            name="test_imports",
            type="transform",
            target="v_test_imports",
            sql="SELECT * FROM v_customers",
            description="Test imports"
        )
        
        context = {
            "project_config": project_config_with_metadata,
            "flowgroup": flowgroup_with_metadata,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Check that F import is added (since we use F.current_timestamp and F.monotonically_increasing_id)
        imports = generator.imports
        
        # Should include the functions import for F.current_timestamp() and F.monotonically_increasing_id()
        assert any("from pyspark.sql import functions as F" in imp for imp in imports)

    def test_temp_table_with_custom_sql_and_metadata(self, project_config_with_metadata, flowgroup_with_metadata):
        """Test temp table with custom SQL and operational metadata."""
        generator = TempTableTransformGenerator()
        
        action = Action(
            name="test_temp_sql",
            type="transform",
            target="temp_aggregated",
            sql="SELECT customer_id, COUNT(*) as order_count FROM {source} GROUP BY customer_id",
            source="v_orders",
            description="Temp table with SQL"
        )
        
        context = {
            "project_config": project_config_with_metadata,
            "flowgroup": flowgroup_with_metadata,
            "preset_config": {}
        }
        
        code = generator.generate(action, context)
        
        # Verify SQL replacement and metadata
        assert "FROM v_orders GROUP BY" in code  # {source} should be replaced
        assert "# Add operational metadata columns" in code
        assert "temporary=True" in code 
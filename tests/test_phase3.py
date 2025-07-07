"""Tests for Phase 3: Action Generators."""

import pytest
from pathlib import Path
from lhp.models.config import Action, ActionType, TransformType
from lhp.generators.load import (
    CloudFilesLoadGenerator,
    DeltaLoadGenerator,
    SQLLoadGenerator,
    JDBCLoadGenerator,
    PythonLoadGenerator
)
from lhp.generators.transform import (
    SQLTransformGenerator,
    DataQualityTransformGenerator,
    SchemaTransformGenerator,
    PythonTransformGenerator,
    TempTableTransformGenerator
)
from lhp.generators.write import (
    StreamingTableWriteGenerator,
    MaterializedViewWriteGenerator
)
from lhp.utils.substitution import EnhancedSubstitutionManager


class TestLoadGenerators:
    """Test load action generators."""
    
    def test_cloudfiles_generator(self):
        """Test CloudFiles load generator."""
        generator = CloudFilesLoadGenerator()
        action = Action(
            name="load_raw_files",
            type=ActionType.LOAD,
            target="v_raw_files",
            source={
                "type": "cloudfiles",
                "path": "/mnt/data/raw",
                "format": "json",
                "readMode": "stream",
                "schema_evolution_mode": "addNewColumns",
                "reader_options": {
                    "multiLine": "true"
                }
            },
            description="Load raw JSON files"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_raw_files" in code
        assert "spark.readStream" in code
        assert 'cloudFiles.format", "json"' in code
        assert 'multiLine", "true"' in code
    
    def test_delta_generator(self):
        """Test Delta load generator."""
        generator = DeltaLoadGenerator()
        action = Action(
            name="load_customers",
            type=ActionType.LOAD,
            target="v_customers",
            source={
                "type": "delta",
                "catalog": "main",
                "database": "bronze",
                "table": "customers",
                "readMode": "stream",
                "read_change_feed": True,
                "where_clause": ["active = true"],
                "select_columns": ["id", "name", "email"]
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers" in code
        assert "spark.readStream" in code
        assert "readChangeFeed" in code
        assert "main.bronze.customers" in code
        assert 'where("active = true")' in code
        assert "select([" in code
    
    def test_sql_generator(self):
        """Test SQL load generator."""
        generator = SQLLoadGenerator()
        action = Action(
            name="load_metrics",
            type=ActionType.LOAD,
            target="v_metrics",
            source="SELECT * FROM metrics WHERE date > current_date() - 7"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_metrics" in code
        assert "spark.sql" in code
        assert "SELECT * FROM metrics" in code
    
    def test_jdbc_generator_with_secrets(self):
        """Test JDBC load generator with secret substitution."""
        generator = JDBCLoadGenerator()
        substitution_mgr = EnhancedSubstitutionManager()
        substitution_mgr.default_secret_scope = "db_secrets"
        
        action = Action(
            name="load_external",
            type=ActionType.LOAD,
            target="v_external_data",
            source={
                "type": "jdbc",
                "url": "jdbc:postgresql://${secret:db/host}:5432/mydb",
                "user": "${secret:db/username}",
                "password": "${secret:db/password}",
                "driver": "org.postgresql.Driver",
                "table": "external_table"
            }
        )
        
        code = generator.generate(action, {"substitution_manager": substitution_mgr})
        
        # Verify placeholders are replaced with correct scope
        assert 'dbutils.secrets.get(scope="db", key="host")' in code
        assert 'dbutils.secrets.get(scope="db", key="username")' in code
        assert 'dbutils.secrets.get(scope="db", key="password")' in code
    
    def test_python_generator(self):
        """Test Python load generator."""
        generator = PythonLoadGenerator()
        action = Action(
            name="load_custom",
            type=ActionType.LOAD,
            target="v_custom_data",
            source={
                "type": "python",
                "module_path": "custom_loaders",
                "function_name": "load_custom_data",
                "parameters": {
                    "start_date": "2024-01-01",
                    "batch_size": 1000
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_custom_data" in code
        assert "load_custom_data(spark, parameters)" in code
        assert '"start_date": "2024-01-01"' in code
        assert "from custom_loaders import load_custom_data" in generator.imports


class TestTransformGenerators:
    """Test transform action generators."""
    
    def test_sql_transform_generator(self):
        """Test SQL transform generator."""
        generator = SQLTransformGenerator()
        action = Action(
            name="transform_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source=["v_customers"],
            target="v_customers_clean",
            sql="SELECT * FROM v_customers WHERE email IS NOT NULL"
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view(comment=" in code
        assert "v_customers_clean" in code
        assert "return spark.sql(" in code
        assert "SELECT * FROM v_customers WHERE email IS NOT NULL" in code
    
    def test_data_quality_generator(self):
        """Test data quality transform generator."""
        generator = DataQualityTransformGenerator()
        
        # Create expectations file
        import tempfile
        import yaml
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            expectations = {
                "email IS NOT NULL": {"action": "warn", "name": "email_not_null"},
                "age >= 18": {"action": "drop", "name": "age_check"},
                "id IS NOT NULL": {"action": "fail", "name": "id_not_null"}
            }
            yaml.dump(expectations, f)
            expectations_file = f.name
        
        action = Action(
            name="validate_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.DATA_QUALITY,
            source="v_customers_clean",
            target="v_customers_validated",
            readMode="stream",
            expectations_file=expectations_file
        )
        
        code = generator.generate(action, {"spec_dir": Path(expectations_file).parent})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers_validated" in code
        assert "@dlt.expect_all_or_fail" in code
        assert "@dlt.expect_all_or_drop" in code
        assert "@dlt.expect_all" in code
        
        # Clean up
        Path(expectations_file).unlink()
    
    def test_python_transform_generator(self):
        """Test Python transform generator."""
        generator = PythonTransformGenerator()
        action = Action(
            name="enrich_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.PYTHON,
            target="v_customers_enriched",
            source={
                "type": "python",
                "module_path": "transformations.py",
                "function_name": "enrich_customers",
                "sources": ["v_customers_validated"],
                "parameters": {"enrichment_type": "full"}
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.view()" in code
        assert "v_customers_enriched" in code
        assert "enrich_customers" in code
        assert 'spark.read.table("v_customers_validated")' in code
    
    def test_temp_table_generator(self):
        """Test temporary table generator."""
        generator = TempTableTransformGenerator()
        action = Action(
            name="staging_customers",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.TEMP_TABLE,
            target="customers_staging",
            source={
                "source": "v_customers_enriched",
                "comment": "Staging table for customers"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "dlt.create_streaming_table" in code
        assert "customers_staging_temp" in code
        assert "temporary=True" in code


class TestWriteGenerators:
    """Test write action generators."""
    
    def test_streaming_table_generator(self):
        """Test streaming table write generator."""
        generator = StreamingTableWriteGenerator()
        action = Action(
            name="write_customers",
            type=ActionType.WRITE,
            source="v_customers_final",
            write_target={
                "type": "streaming_table",
                "database": "silver",
                "table": "customers",
                "partition_columns": ["year", "month"],
                "cluster_columns": ["customer_id"],
                "table_properties": {
                    "quality": "silver"
                }
            }
        )
        
        code = generator.generate(action, {})
        
        # Check generated code - standard mode creates table and append flow
        assert "dlt.create_streaming_table" in code
        assert "@dlt.append_flow(" in code
        assert "silver.customers" in code
        assert "spark.readStream.table" in code
    
    def test_materialized_view_generator(self):
        """Test materialized view write generator."""
        generator = MaterializedViewWriteGenerator()
        action = Action(
            name="write_summary",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "database": "gold",
                "table": "customer_summary",
                "refresh_schedule": "@daily",
                "sql": "SELECT region, COUNT(*) as customer_count FROM silver.customers GROUP BY region"
            }
        )
        
        code = generator.generate(action, {})
        
        # Verify generated code
        assert "@dlt.table(" in code
        assert 'name="gold.customer_summary"' in code
        assert 'refresh_schedule="@daily"' in code
        assert "spark.sql" in code
        assert "GROUP BY region" in code


def test_generator_imports():
    """Test that generators manage imports correctly."""
    # Load generator
    load_gen = CloudFilesLoadGenerator()
    assert "import dlt" in load_gen.imports
    
    # Transform generator with additional imports
    schema_gen = SchemaTransformGenerator()
    assert "import dlt" in schema_gen.imports
    assert "from pyspark.sql import functions as F" in schema_gen.imports
    assert "from pyspark.sql.types import StructType" in schema_gen.imports
    
    # Write generator
    mv_gen = MaterializedViewWriteGenerator()
    assert "import dlt" in mv_gen.imports
    assert "from pyspark.sql import DataFrame" in mv_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
"""Tests for transform action generators of LakehousePlumber."""

import pytest
from pathlib import Path
import tempfile
import yaml
from lhp.models.config import Action, ActionType, TransformType
from lhp.generators.transform import (
    SQLTransformGenerator,
    DataQualityTransformGenerator,
    SchemaTransformGenerator,
    PythonTransformGenerator,
    TempTableTransformGenerator
)


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
        assert "df = spark.sql(" in code
        assert "return df" in code
        assert "SELECT * FROM v_customers WHERE email IS NOT NULL" in code
    
    def test_data_quality_generator(self):
        """Test data quality transform generator."""
        generator = DataQualityTransformGenerator()
        
        # Create expectations file
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
        
        # Verify generated code uses correct pattern
        assert "@dlt.table(" in code
        assert "temporary=True" in code
        assert "def customers_staging():" in code
        # Verify it does NOT use the old incorrect pattern
        assert "dlt.create_streaming_table" not in code
        assert "customers_staging_temp" not in code


def test_transform_generator_imports():
    """Test that transform generators manage imports correctly."""
    # Transform generator with additional imports
    schema_gen = SchemaTransformGenerator()
    assert "import dlt" in schema_gen.imports
    assert "from pyspark.sql import functions as F" in schema_gen.imports
    assert "from pyspark.sql.types import StructType" in schema_gen.imports


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
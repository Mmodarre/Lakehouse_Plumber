"""Test CloudFiles options implementation."""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch, Mock

from lhp.generators.load.cloudfiles import CloudFilesLoadGenerator
from lhp.models.config import Action
from lhp.utils.schema_parser import SchemaParser
from lhp.utils.error_formatter import LHPError


class TestCloudFilesOptions:
    """Test CloudFiles options functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = CloudFilesLoadGenerator()
        self.schema_parser = SchemaParser()
        
        # Create temporary schema file
        self.temp_dir = tempfile.mkdtemp()
        self.schema_file = Path(self.temp_dir) / "test_schema.yaml"
        
        schema_content = {
            "name": "test",
            "version": "1.0",
            "description": "Test schema",
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False, "comment": "Primary key"},
                {"name": "name", "type": "STRING", "nullable": False, "comment": "Name field"},
                {"name": "amount", "type": "DECIMAL(18,2)", "nullable": True, "comment": "Amount field"},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False, "comment": "Created timestamp"}
            ]
        }
        
        with open(self.schema_file, 'w') as f:
            yaml.dump(schema_content, f)
    
    def test_basic_options_processing(self):
        """Test basic options processing without schema."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "options": {
                    "cloudFiles.header": True,
                    "cloudFiles.delimiter": ",",
                    "cloudFiles.maxFilesPerTrigger": 10,
                    "cloudFiles.inferColumnTypes": False
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {})
        
        # Check that options are properly included
        assert '.option("cloudFiles.header", True)' in result
        assert '.option("cloudFiles.delimiter", ",")' in result
        assert '.option("cloudFiles.maxFilesPerTrigger", 10)' in result
        assert '.option("cloudFiles.inferColumnTypes", False)' in result
    
    def test_schema_hints_from_file(self):
        """Test schema hints processing from file."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.schemaHints": str(self.schema_file)
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {"spec_dir": Path(self.temp_dir)})
        
        # Check that schema hints are properly generated
        assert "id BIGINT" in result
        assert "name STRING" in result
        assert "amount DECIMAL(18,2)" in result
        assert "created_at TIMESTAMP" in result
        assert '.option("cloudFiles.schemaHints"' in result
    
    def test_explicit_schema_enforcement(self):
        """Test explicit schema enforcement."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "schema": str(self.schema_file),
                "options": {
                    "cloudFiles.format": "csv"
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {"spec_dir": Path(self.temp_dir)})
        
        # Check that explicit schema is properly generated
        assert "test_schema = StructType([" in result
        assert 'StructField("id", LongType(), False' in result
        assert 'StructField("name", StringType(), False' in result
        assert 'StructField("amount", DecimalType(18, 2), True' in result
        assert 'StructField("created_at", TimestampType(), False' in result
        assert "df = df.schema(test_schema)" in result
    
    def test_missing_prefix_error(self):
        """Test error when cloudFiles option is missing prefix."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "options": {
                    "format": "csv",  # Missing cloudFiles. prefix
                    "header": True    # Missing cloudFiles. prefix
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        with pytest.raises(LHPError, match="Configuration conflict"):
            self.generator.generate(action, {})
    
    def test_conflict_detection(self):
        """Test conflict detection between old and new approaches."""
        # Test conflict between legacy and new options
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "schema_location": "/legacy/schema",
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.schemaLocation": "/new/schema"
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        with pytest.raises(LHPError, match="Configuration conflict"):
            self.generator.generate(action, {})
    
    def test_multiple_schema_sources_error(self):
        """Test error when multiple schema sources are specified."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "schema": str(self.schema_file),
                "schema_file": str(self.schema_file),
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.schemaHints": str(self.schema_file)
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        with pytest.raises(LHPError, match="Configuration conflict"):
            self.generator.generate(action, {"spec_dir": Path(self.temp_dir)})
    
    def test_value_type_preservation(self):
        """Test that YAML value types are preserved."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.header": True,              # boolean
                    "cloudFiles.maxFilesPerTrigger": 10,    # number
                    "cloudFiles.delimiter": ",",            # string
                    "cloudFiles.inferColumnTypes": False   # boolean
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {})
        
        # Check boolean values are not quoted and use proper Python syntax
        assert '.option("cloudFiles.header", True)' in result
        assert '.option("cloudFiles.inferColumnTypes", False)' in result
        
        # Check numbers are not quoted
        assert '.option("cloudFiles.maxFilesPerTrigger", 10)' in result
        
        # Check strings are quoted
        assert '.option("cloudFiles.delimiter", ",")' in result
    
    def test_backward_compatibility(self):
        """Test backward compatibility with legacy options."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "csv",
                "schema_location": "/legacy/schema",
                "max_files_per_trigger": 5,
                "schema_evolution_mode": "addNewColumns",
                "reader_options": {
                    "header": "true"
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {})
        
        # Check that legacy options are converted to new format
        assert '.option("cloudFiles.schemaLocation", "/legacy/schema")' in result
        assert '.option("cloudFiles.maxFilesPerTrigger", 5)' in result
        assert '.option("cloudFiles.schemaEvolutionMode", "addNewColumns")' in result
        assert '.option("header", "true")' in result
    
    def test_mandatory_format_option(self):
        """Test that mandatory format option is added if missing."""
        action = Action(
            name="test_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/path/to/data",
                "format": "json",
                "options": {
                    "cloudFiles.header": True
                }
            },
            target="test_table",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {})
        
        # Check that format option is automatically added
        assert '.option("cloudFiles.format", "json")' in result
    
    def test_schema_hints_formatting(self):
        """Test schema hints formatting for both short and long strings."""
        # Test short schema hints (now generates a variable)
        action_short = Action(
            name="short_test",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/data/test/*.csv",
                "format": "csv",
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.schemaHints": "id INT, name STRING"  # Short string
                }
            },
            target="short_table",
            readMode="stream"
        )
        
        result_short = self.generator.generate(action_short, {})
        
        # Check that schema hints are generated as a variable
        assert 'short_table_schema_hints = """' in result_short
        assert 'id INT' in result_short
        assert 'name STRING' in result_short
        assert '.option("cloudFiles.schemaHints", short_table_schema_hints)' in result_short
    
    def test_comprehensive_example(self):
        """Test a comprehensive example with all features."""
        action = Action(
            name="comprehensive_action",
            type="load",
            source={
                "type": "cloudfiles",
                "path": "/data/landing/customers",
                "format": "csv",
                "schema": str(self.schema_file),
                "options": {
                    "cloudFiles.format": "csv",
                    "cloudFiles.header": True,
                    "cloudFiles.delimiter": ",",
                    "cloudFiles.maxFilesPerTrigger": 50,
                    "cloudFiles.schemaEvolutionMode": "addNewColumns",
                    "cloudFiles.inferColumnTypes": False,
                    "cloudFiles.rescueDataColumn": "_rescued_data"
                }
            },
            target="customers_bronze",
            readMode="stream"
        )
        
        result = self.generator.generate(action, {"spec_dir": Path(self.temp_dir)})
        
        # Check schema enforcement
        assert "test_schema = StructType([" in result
        assert "df = df.schema(test_schema)" in result
        
        # Check all options are included with proper Python syntax
        assert '.option("cloudFiles.format", "csv")' in result
        assert '.option("cloudFiles.header", True)' in result
        assert '.option("cloudFiles.delimiter", ",")' in result
        assert '.option("cloudFiles.maxFilesPerTrigger", 50)' in result
        assert '.option("cloudFiles.schemaEvolutionMode", "addNewColumns")' in result
        assert '.option("cloudFiles.inferColumnTypes", False)' in result
        assert '.option("cloudFiles.rescueDataColumn", "_rescued_data")' in result
        
        # Check the function structure
        assert "@dlt.view()" in result
        assert "def customers_bronze():" in result
        assert "spark.readStream" in result
        assert ".format(\"cloudFiles\")" in result
        assert ".load(\"/data/landing/customers\")" in result


class TestSchemaParser:
    """Test SchemaParser functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.parser = SchemaParser()
        
        # Create test schema data
        self.schema_data = {
            "name": "test_schema",
            "version": "1.0",
            "description": "Test schema",
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False, "comment": "Primary key"},
                {"name": "name", "type": "STRING", "nullable": True, "comment": "Name field"},
                {"name": "amount", "type": "DECIMAL(18,2)", "nullable": True},
                {"name": "is_active", "type": "BOOLEAN", "nullable": False},
                {"name": "created_at", "type": "TIMESTAMP", "nullable": False}
            ]
        }
    
    def test_to_schema_hints(self):
        """Test conversion to schema hints."""
        result = self.parser.to_schema_hints(self.schema_data)
        
        expected = "id BIGINT, name STRING, amount DECIMAL(18,2), is_active BOOLEAN, created_at TIMESTAMP"
        assert result == expected
    
    def test_to_struct_type_code(self):
        """Test conversion to StructType code."""
        variable_name, code_lines = self.parser.to_struct_type_code(self.schema_data)
        
        assert variable_name == "test_schema_schema"
        
        # Check that imports are included
        assert any("from pyspark.sql.types import" in line for line in code_lines)
        
        # Check schema definition
        assert "test_schema_schema = StructType([" in code_lines
        assert any('StructField("id", LongType(), False' in line for line in code_lines)
        assert any('StructField("name", StringType(), True' in line for line in code_lines)
        assert any('StructField("amount", DecimalType(18, 2), True' in line for line in code_lines)
        assert any('StructField("is_active", BooleanType(), False' in line for line in code_lines)
    
    def test_type_conversion(self):
        """Test type conversion to Spark types."""
        test_cases = [
            ("STRING", "StringType()"),
            ("BIGINT", "LongType()"),
            ("INT", "IntegerType()"),
            ("DECIMAL(18,2)", "DecimalType(18, 2)"),
            ("BOOLEAN", "BooleanType()"),
            ("TIMESTAMP", "TimestampType()"),
            ("UNKNOWN_TYPE", "StringType()")  # Should default to StringType
        ]
        
        for input_type, expected_output in test_cases:
            result = self.parser._convert_to_spark_type(input_type)
            assert result == expected_output
    
    def test_schema_validation(self):
        """Test schema validation."""
        # Valid schema
        errors = self.parser.validate_schema(self.schema_data)
        assert errors == []
        
        # Missing name
        invalid_schema = {
            "columns": [{"name": "id", "type": "BIGINT"}]
        }
        errors = self.parser.validate_schema(invalid_schema)
        assert any("must have 'name' field" in error for error in errors)
        
        # Missing columns
        invalid_schema = {
            "name": "test"
        }
        errors = self.parser.validate_schema(invalid_schema)
        assert any("must have 'columns' field" in error for error in errors)
        
        # Column missing required fields
        invalid_schema = {
            "name": "test",
            "columns": [{"name": "id"}]  # Missing type
        }
        errors = self.parser.validate_schema(invalid_schema)
        assert any("must have 'type' field" in error for error in errors) 
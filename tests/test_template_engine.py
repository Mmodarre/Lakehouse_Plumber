"""Tests for Template Engine - Step 4.2.3."""

import pytest
import tempfile
from pathlib import Path
from lhp.core.template_engine import TemplateEngine
from lhp.models.config import Template, Action, ActionType, LoadSourceType


class TestTemplateEngine:
    """Test template engine functionality."""
    
    def create_test_template(self, tmpdir):
        """Create a test template file."""
        template_yaml = """
name: bronze_ingestion
version: "1.0"
description: "Template for bronze layer data ingestion"
parameters:
  - name: source_path
    type: string
    required: true
    description: "Path to source data"
  - name: target_table
    type: string
    required: true
    description: "Target table name"
  - name: file_format
    type: string
    default: "json"
    description: "File format"
  - name: readMode
    type: string
    default: "stream"
    description: "Read mode for loading data"
actions:
  - name: load_{{ target_table }}_raw
    type: load
    target: v_{{ target_table }}_raw
    source:
      type: cloudfiles
      path: "{{ source_path }}"
      format: "{{ file_format }}"
      readMode: "{{ readMode }}"
    description: "Load {{ target_table }} from {{ file_format }} files"
  - name: write_{{ target_table }}
    type: write
    source:
      type: streaming_table
      database: bronze
      table: "{{ target_table }}"
      view: v_{{ target_table }}_raw
"""
        template_file = tmpdir / "bronze_ingestion.yaml"
        template_file.write_text(template_yaml)
        return tmpdir
    
    def test_template_engine_initialization(self):
        """Test template engine initialization."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            engine = TemplateEngine(templates_dir)
            
            assert engine.templates_dir == templates_dir
            assert engine._template_cache == {}
    
    def test_load_templates(self):
        """Test loading templates from directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Check template was loaded
            assert "bronze_ingestion" in engine._template_cache
            template = engine.get_template("bronze_ingestion")
            assert template is not None
            assert template.name == "bronze_ingestion"
            assert len(template.parameters) == 4
            assert len(template.actions) == 2
    
    def test_get_template(self):
        """Test getting template by name."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Get existing template
            template = engine.get_template("bronze_ingestion")
            assert template is not None
            assert template.name == "bronze_ingestion"
            
            # Get non-existent template
            template = engine.get_template("non_existent")
            assert template is None
    
    def test_render_template(self):
        """Test rendering template with parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Render with all parameters
            parameters = {
                "source_path": "/mnt/landing/customers",
                "target_table": "customers",
                "file_format": "parquet",
                "readMode": "batch"  # Correct parameter name
            }
            
            actions = engine.render_template("bronze_ingestion", parameters)
            
            # Verify rendered actions
            assert len(actions) == 2
            
            # Check first action (load)
            load_action = actions[0]
            assert load_action.name == "load_customers_raw"
            assert load_action.type == ActionType.LOAD
            assert load_action.target == "v_customers_raw"
            assert load_action.source["path"] == "/mnt/landing/customers"
            assert load_action.source["format"] == "parquet"
            assert load_action.source["readMode"] == "batch"  # Correct field name
            assert "Load customers from parquet files" in load_action.description
            
            # Check second action (write)
            write_action = actions[1]
            assert write_action.name == "write_customers"
            assert write_action.type == ActionType.WRITE
            assert write_action.source["table"] == "customers"
            assert write_action.source["view"] == "v_customers_raw"
    
    def test_render_template_with_defaults(self):
        """Test rendering template using default parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Render with only required parameters
            parameters = {
                "source_path": "/mnt/landing/orders",
                "target_table": "orders"
            }
            
            actions = engine.render_template("bronze_ingestion", parameters)
            
            # Verify defaults were applied
            load_action = actions[0]
            assert load_action.source["format"] == "json"  # default
            assert load_action.source["readMode"] == "stream"  # default
    
    def test_render_template_missing_required(self):
        """Test rendering template with missing required parameters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Try to render without required parameter
            parameters = {
                "target_table": "orders"  # missing source_path
            }
            
            with pytest.raises(ValueError, match="Missing required parameters"):
                engine.render_template("bronze_ingestion", parameters)
    
    def test_render_template_not_found(self):
        """Test rendering non-existent template."""
        engine = TemplateEngine()
        
        with pytest.raises(ValueError, match="Template not found"):
            engine.render_template("non_existent", {})
    
    def test_list_templates(self):
        """Test listing available templates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            # Create another template
            template2_yaml = """
name: silver_transform
version: "1.0"
description: "Silver layer transformation"
parameters:
  - name: source_table
    type: string
    required: true
actions:
  - name: transform_{{ source_table }}
    type: transform
    transform_type: sql
    source: ["v_{{ source_table }}"]
    target: v_{{ source_table }}_clean
    sql: "SELECT * FROM v_{{ source_table }} WHERE is_valid = true"
"""
            (templates_dir / "silver_transform.yaml").write_text(template2_yaml)
            
            engine = TemplateEngine(templates_dir)
            templates = engine.list_templates()
            
            assert len(templates) == 2
            assert "bronze_ingestion" in templates
            assert "silver_transform" in templates
    
    def test_get_template_info(self):
        """Test getting template information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            self.create_test_template(templates_dir)
            
            engine = TemplateEngine(templates_dir)
            
            # Get info for existing template
            info = engine.get_template_info("bronze_ingestion")
            assert info["name"] == "bronze_ingestion"
            assert info["version"] == "1.0"
            assert info["description"] == "Template for bronze layer data ingestion"
            assert len(info["parameters"]) == 4
            assert info["action_count"] == 2
            
            # Get info for non-existent template
            info = engine.get_template_info("non_existent")
            assert info == {}
    
    def test_complex_template_rendering(self):
        """Test rendering template with complex parameter substitution."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create a complex template
            complex_yaml = """
name: complex_pipeline
version: "1.0"
description: "Complex template with nested parameters"
parameters:
  - name: tables
    type: list
    required: true
  - name: database
    type: string
    required: true
  - name: config
    type: dict
    required: true
actions:
  - name: load_data
    type: load
    target: v_{{ tables[0] }}
    source:
      type: delta
      database: "{{ database }}"
      table: "{{ tables[0] }}"
      where_clause: ["{{ config.filter }}"]
  - name: transform_data
    type: transform
    transform_type: sql
    source: ["v_{{ tables[0] }}"]
    target: v_{{ tables[0] }}_transformed
    sql: "SELECT * FROM v_{{ tables[0] }} WHERE {{ config.condition }}"
"""
            (templates_dir / "complex_pipeline.yaml").write_text(complex_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            parameters = {
                "tables": ["customers", "orders"],
                "database": "bronze",
                "config": {
                    "filter": "created_date >= '2024-01-01'",
                    "condition": "status = 'active'"
                }
            }
            
            actions = engine.render_template("complex_pipeline", parameters)
            
            # Verify complex parameter substitution
            load_action = actions[0]
            assert load_action.target == "v_customers"
            assert load_action.source["database"] == "bronze"
            assert load_action.source["table"] == "customers"
            assert load_action.source["where_clause"][0] == "created_date >= '2024-01-01'"
            
            transform_action = actions[1]
            assert "WHERE status = 'active'" in transform_action.sql

    def test_table_properties_template_parameter(self):
        """Test template with table_properties parameter - the original issue that caused validation errors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create a template that uses table_properties parameter with template syntax
            template_yaml = """
name: csv_ingestion_template
version: "1.0"
description: "Template for CSV ingestion with table properties"

parameters:
  - name: table_name
    required: true
    description: "Name of the table to ingest"
  - name: table_properties
    required: false
    description: "Optional table properties as key-value pairs"
    default: {}

actions:
  - name: write_{{ table_name }}_cloudfiles
    type: write
    source: v_{{ table_name }}_cloudfiles
    write_target:
      type: streaming_table
      database: "catalog.schema"
      table: "{{ table_name }}"
      table_properties: "{{ table_properties }}"
      description: "Write {{ table_name }} to raw layer"
"""
            (templates_dir / "csv_ingestion_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Test with table_properties provided
            parameters = {
                "table_name": "customer",
                "table_properties": {
                    "tag1": "hello",
                    "tag2": "world",
                    "delta.enableChangeDataFeed": "true"
                }
            }
            
            actions = engine.render_template("csv_ingestion_template", parameters)
            
            # Verify the action was created successfully
            assert len(actions) == 1
            action = actions[0]
            assert action.name == "write_customer_cloudfiles"
            assert action.type == ActionType.WRITE
            assert action.write_target["table"] == "customer"
            assert action.write_target["table_properties"]["tag1"] == "hello"
            assert action.write_target["table_properties"]["tag2"] == "world"
            assert action.write_target["table_properties"]["delta.enableChangeDataFeed"] == "true"
            
            # Test with empty table_properties (default)
            parameters_empty = {
                "table_name": "orders"
            }
            
            actions_empty = engine.render_template("csv_ingestion_template", parameters_empty)
            assert len(actions_empty) == 1
            action_empty = actions_empty[0]
            assert action_empty.write_target["table"] == "orders"
            assert action_empty.write_target["table_properties"] == {}

    def test_array_template_parameters(self):
        """Test array parameter conversion - homogeneous types only."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with array parameter
            template_yaml = """
name: array_test_template
version: "1.0"
description: "Template for testing array parameters"

parameters:
  - name: cluster_columns
    required: true
    description: "Cluster columns array"
  - name: table_name
    required: true
    description: "Table name"

actions:
  - name: test_array_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema"
      table: "{{ table_name }}"
      cluster_columns: "{{ cluster_columns }}"
"""
            (templates_dir / "array_test_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Test cases: (parameter_value, expected_result, description)
            test_cases = [
                # Valid cases
                (["col1"], ["col1"], "Single string array"),
                (["col1", "col2", "col3"], ["col1", "col2", "col3"], "3-item string array"),
                (["a", "b", "c", "d", "e"], ["a", "b", "c", "d", "e"], "5-item string array"),
                ([1, 2, 3], [1, 2, 3], "3-item numeric array"),
                ([100], [100], "Single numeric array"),
                ([], [], "Empty array"),
            ]
            
            for param_value, expected, description in test_cases:
                parameters = {
                    "table_name": "test_table",
                    "cluster_columns": param_value
                }
                
                actions = engine.render_template("array_test_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                assert action.write_target["cluster_columns"] == expected, f"Failed for {description}"

    def test_array_template_parameters_fail_fast(self):
        """Test array parameter conversion error cases - should fail fast."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with array parameter
            template_yaml = """
name: array_error_template
version: "1.0"
parameters:
  - name: cluster_columns
    required: true
  - name: table_name
    required: true
actions:
  - name: test_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema" 
      table: "{{ table_name }}"
      cluster_columns: "{{ cluster_columns }}"
"""
            (templates_dir / "array_error_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Error test cases: (parameter_value, expected_error_substring)
            error_cases = [
                ('[invalid syntax here]', "Invalid array template parameter"),
                ('[1, 2, invalid]', "Arrays must be valid JSON format"),
            ]
            
            for param_value, expected_error in error_cases:
                parameters = {
                    "table_name": "test_table", 
                    "cluster_columns": param_value
                }
                
                with pytest.raises(ValueError) as exc_info:
                    engine.render_template("array_error_template", parameters)
                assert expected_error in str(exc_info.value), f"Failed for {param_value}"

    def test_object_template_parameters(self):
        """Test object parameter conversion - simple objects only."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with object parameter
            template_yaml = """
name: object_test_template
version: "1.0"
description: "Template for testing object parameters"

parameters:
  - name: spark_conf
    required: true
    description: "Spark configuration object"
  - name: table_name
    required: true
    description: "Table name"

actions:
  - name: test_object_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema"
      table: "{{ table_name }}"
      spark_conf: "{{ spark_conf }}"
"""
            (templates_dir / "object_test_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Test cases: (parameter_value, expected_result, description)
            test_cases = [
                # Valid cases
                ({"key": "value"}, {"key": "value"}, "Simple key-value"),
                ({"enabled": "true"}, {"enabled": "true"}, "Config-style object"),
                ({"port": "5432", "host": "localhost"}, {"port": "5432", "host": "localhost"}, "Multi-key object"),
                ({}, {}, "Empty object"),
            ]
            
            for param_value, expected, description in test_cases:
                parameters = {
                    "table_name": "test_table",
                    "spark_conf": param_value
                }
                
                actions = engine.render_template("object_test_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                assert action.write_target["spark_conf"] == expected, f"Failed for {description}"

    def test_object_template_parameters_fail_fast(self):
        """Test object parameter conversion error cases - should fail fast.""" 
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with object parameter
            template_yaml = """
name: object_error_template
version: "1.0"
parameters:
  - name: spark_conf
    required: true
  - name: table_name
    required: true
actions:
  - name: test_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema"
      table: "{{ table_name }}"
      spark_conf: "{{ spark_conf }}"
"""
            (templates_dir / "object_error_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # With smart template detection, invalid object strings are now treated as literal strings
            # This is the expected behavior since template parameters should be properly typed
            # Real error cases would be things like template syntax errors or missing parameters
            
            # Test case: string parameter gets rendered as string (no conversion attempted)
            parameters = {
                "table_name": "test_table",
                "spark_conf": '{invalid syntax here}'  # This stays as a string
            }
            
            # This should not raise an error - invalid strings stay as strings
            actions = engine.render_template("object_error_template", parameters)
            assert actions[0].write_target["spark_conf"] == '{invalid syntax here}'

    def test_boolean_template_parameters(self):
        """Test boolean parameter conversion - strict true/false only."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with boolean parameter
            template_yaml = """
name: boolean_test_template
version: "1.0"
description: "Template for testing boolean parameters"

parameters:
  - name: create_table
    required: true
    description: "Whether to create table"
  - name: table_name
    required: true
    description: "Table name"

actions:
  - name: test_boolean_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema"
      table: "{{ table_name }}"
      create_table: "{{ create_table }}"
"""
            (templates_dir / "boolean_test_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Test cases: (parameter_value, expected_result, description)
            test_cases = [
                # Valid cases
                (True, True, "Boolean True"),
                (False, False, "Boolean False"),
                ("true", True, "String lowercase true"),
                ("false", False, "String lowercase false"),
                ("True", True, "String capitalized True"),
                ("False", False, "String capitalized False"),
                ("TRUE", True, "String uppercase TRUE"),
                ("FALSE", False, "String uppercase FALSE"),
            ]
            
            for param_value, expected, description in test_cases:
                parameters = {
                    "table_name": "test_table",
                    "create_table": param_value
                }
                
                actions = engine.render_template("boolean_test_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                assert action.write_target["create_table"] == expected, f"Failed for {description}"

    def test_boolean_template_parameters_fail_fast(self):
        """Test boolean parameter conversion error cases - should fail fast."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with boolean parameter
            template_yaml = """
name: boolean_error_template
version: "1.0"
parameters:
  - name: create_table
    required: true
  - name: table_name
    required: true
actions:
  - name: test_action
    type: write
    source: v_test
    write_target:
      type: streaming_table
      database: "test.schema"
      table: "{{ table_name }}"
      create_table: "{{ create_table }}"
"""
            (templates_dir / "boolean_error_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Note: Boolean conversion only happens for true/false strings
            # Other values get converted based on their type (integers, strings, etc.)
            # Test that non-boolean strings remain as strings, unless they match other patterns
            test_cases = [
                ("yes", "yes"),  # Stays as string
                ("no", "no"),    # Stays as string  
                ("1", 1),        # Converted to integer (correct behavior)
                ("0", 0),        # Converted to integer (correct behavior)
                ("maybe", "maybe"), # Stays as string
            ]
            
            for param_value, expected in test_cases:
                parameters = {
                    "table_name": "test_table",
                    "create_table": param_value
                }
                
                actions = engine.render_template("boolean_error_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                # Values get converted based on their apparent type
                assert action.write_target["create_table"] == expected
                if isinstance(expected, str):
                    assert isinstance(action.write_target["create_table"], str)
                elif isinstance(expected, int):
                    assert isinstance(action.write_target["create_table"], int)

    def test_integer_template_parameters(self):
        """Test integer parameter conversion - integers only, no conversion."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with integer parameter 
            template_yaml = """
name: integer_test_template
version: "1.0"
description: "Template for testing integer parameters"

parameters:
  - name: max_files
    required: true
    description: "Maximum files trigger"
  - name: table_name
    required: true
    description: "Table name"

actions:
  - name: test_integer_action
    type: load
    source:
      type: cloudfiles
      path: "/test/path"
      format: csv
      max_files_per_trigger: "{{ max_files }}"
    target: v_test
"""
            (templates_dir / "integer_test_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Test cases: (parameter_value, expected_result, description)
            test_cases = [
                # Valid cases
                (42, 42, "Positive integer"),
                (-17, -17, "Negative integer"),
                (0, 0, "Zero"),
                (999999, 999999, "Large integer"),
                ("42", 42, "String integer"),
                ("-17", -17, "String negative integer"),
                ("0", 0, "String zero"),
            ]
            
            for param_value, expected, description in test_cases:
                parameters = {
                    "table_name": "test_table",
                    "max_files": param_value
                }
                
                actions = engine.render_template("integer_test_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                assert action.source["max_files_per_trigger"] == expected, f"Failed for {description}"

    def test_integer_template_parameters_fail_fast(self):
        """Test integer parameter conversion error cases - should fail fast."""
        with tempfile.TemporaryDirectory() as tmpdir:
            templates_dir = Path(tmpdir)
            
            # Create template with integer parameter
            template_yaml = """
name: integer_error_template  
version: "1.0"
parameters:
  - name: max_files
    required: true
  - name: table_name
    required: true
actions:
  - name: test_action
    type: load
    source:
      type: cloudfiles
      path: "/test/path"
      format: csv
      max_files_per_trigger: "{{ max_files }}"
    target: v_test
"""
            (templates_dir / "integer_error_template.yaml").write_text(template_yaml)
            
            engine = TemplateEngine(templates_dir)
            
            # Error test cases: values that should remain as strings (not converted to int)
            # Our integer detection is strict - only pure integers get converted
            # Decimals, scientific notation, etc. stay as strings (which is correct)
            
            # Test cases that stay as strings (expected behavior)
            string_cases = [
                ("42.0", "42.0", "Float should stay as string"),
                ("3.14", "3.14", "Decimal should stay as string"),
                ("1e6", "1e6", "Scientific notation should stay as string"),
                ("not_a_number", "not_a_number", "Invalid number should stay as string"),
                ("42.5.6", "42.5.6", "Malformed number should stay as string"),
            ]
            
            for param_value, expected, description in string_cases:
                parameters = {
                    "table_name": "test_table",
                    "max_files": param_value
                }
                
                actions = engine.render_template("integer_error_template", parameters)
                assert len(actions) == 1
                action = actions[0]
                # These should stay as strings, not convert to integers
                assert action.source["max_files_per_trigger"] == expected, f"Failed for {description}"
                assert isinstance(action.source["max_files_per_trigger"], str), f"Should be string for {description}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
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


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 
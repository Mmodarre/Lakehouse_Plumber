"""Tests for job_name inheritance in multi-flowgroup YAML arrays."""

import pytest
from pathlib import Path
import tempfile
from lhp.parsers.yaml_parser import YAMLParser


class TestJobNameInheritance:
    """Test job_name inheritance in multi-flowgroup arrays."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.parser = YAMLParser()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_job_name_inherited_in_array(self):
        """Test that job_name is inherited from document level to array items."""
        yaml_content = """pipeline: test_pipeline
job_name: bronze_job
flowgroups:
  - flowgroup: fg1
    actions:
      - name: load_fg1
        type: load
        source: raw.table1
        target: v_fg1
  - flowgroup: fg2
    actions:
      - name: load_fg2
        type: load
        source: raw.table2
        target: v_fg2
"""
        yaml_file = self.temp_dir / "test.yaml"
        yaml_file.write_text(yaml_content)
        
        # Parse the file
        flowgroups = self.parser.parse_flowgroups_from_file(yaml_file)
        
        # Should have 2 flowgroups, both with job_name inherited
        assert len(flowgroups) == 2
        assert flowgroups[0].flowgroup == "fg1"
        assert flowgroups[0].job_name == "bronze_job"
        assert flowgroups[1].flowgroup == "fg2"
        assert flowgroups[1].job_name == "bronze_job"
    
    def test_job_name_override_in_array_item(self):
        """Test that job_name can be overridden in individual array items."""
        yaml_content = """pipeline: test_pipeline
job_name: bronze_job
flowgroups:
  - flowgroup: fg1
    actions:
      - name: load_fg1
        type: load
        source: raw.table1
        target: v_fg1
  - flowgroup: fg2
    job_name: silver_job
    actions:
      - name: load_fg2
        type: load
        source: raw.table2
        target: v_fg2
"""
        yaml_file = self.temp_dir / "test.yaml"
        yaml_file.write_text(yaml_content)
        
        # Parse the file
        flowgroups = self.parser.parse_flowgroups_from_file(yaml_file)
        
        # fg1 should inherit bronze_job, fg2 should override with silver_job
        assert len(flowgroups) == 2
        assert flowgroups[0].job_name == "bronze_job"
        assert flowgroups[1].job_name == "silver_job"
    
    def test_job_name_with_other_inheritable_fields(self):
        """Test that job_name inherits along with other fields."""
        yaml_content = """pipeline: test_pipeline
job_name: bronze_job
use_template: test_template
presets:
  - bronze_preset
operational_metadata: true
flowgroups:
  - flowgroup: fg1
    template_parameters:
      table_name: table1
  - flowgroup: fg2
    template_parameters:
      table_name: table2
    presets:
      - custom_preset
"""
        yaml_file = self.temp_dir / "test.yaml"
        yaml_file.write_text(yaml_content)
        
        # Parse the file
        flowgroups = self.parser.parse_flowgroups_from_file(yaml_file)
        
        # Both should inherit job_name, pipeline, use_template
        assert len(flowgroups) == 2
        
        # fg1 inherits everything
        assert flowgroups[0].job_name == "bronze_job"
        assert flowgroups[0].pipeline == "test_pipeline"
        assert flowgroups[0].use_template == "test_template"
        assert flowgroups[0].presets == ["bronze_preset"]
        
        # fg2 inherits job_name, pipeline, use_template but overrides presets
        assert flowgroups[1].job_name == "bronze_job"
        assert flowgroups[1].pipeline == "test_pipeline"
        assert flowgroups[1].use_template == "test_template"
        assert flowgroups[1].presets == ["custom_preset"]


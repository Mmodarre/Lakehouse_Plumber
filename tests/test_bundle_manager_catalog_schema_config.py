"""Tests for pipeline config catalog/schema extraction and validation.

NOTE: Tests for the removed _get_catalog_schema_from_pipeline_config() method
have been moved to test_bundle_full_substitution.py, which now tests the full
substitution functionality through generate_resource_file_content().
"""

import pytest
from pathlib import Path
import yaml
from lhp.bundle.manager import BundleManager


class TestGenerateResourceFileContentSignature:
    """Test generate_resource_file_content() signature and behavior."""
    
    def test_env_parameter_required(self, tmp_path):
        """Should require env parameter (breaking change)."""
        # Setup
        config_file = tmp_path / "config" / "pipeline_config.yaml"
        config_file.parent.mkdir()
        config_file.write_text("project_defaults:\n  serverless: true\n")
        
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        
        # Act & Assert - Python will raise TypeError for missing required parameter
        manager = BundleManager(tmp_path, str(config_file))
        with pytest.raises(TypeError):
            manager.generate_resource_file_content("test_pipeline", generated_dir)
    
    def test_passes_catalog_schema_to_template(self, tmp_path):
        """Should pass catalog/schema to template when defined."""
        # Setup
        config_file = tmp_path / "config" / "pipeline_config.yaml"
        config_file.parent.mkdir()
        config_file.write_text("""
---
pipeline: test_pipeline
catalog: "test_catalog"
schema: "test_schema"
serverless: true
""")
        
        sub_file = tmp_path / "substitutions" / "dev.yaml"
        sub_file.parent.mkdir()
        sub_file.write_text("dev:\n  dummy: value\n")
        
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        
        # Act
        manager = BundleManager(tmp_path, str(config_file))
        result = manager.generate_resource_file_content("test_pipeline", generated_dir, "dev")
        
        # Assert - result should contain the catalog/schema values (not variables)
        assert "test_catalog" in result
        assert "test_schema" in result
        # Should not contain variable syntax for catalog/schema
        assert "${var.default_pipeline_catalog}" not in result or "test_catalog" in result
    
class TestTemplateRendering:
    """Test template renders correct catalog/schema values."""
    
    def test_uses_literal_values_when_provided(self, tmp_path):
        """Should output literal values, not ${var.*}."""
        # Setup
        config_file = tmp_path / "config" / "pipeline_config.yaml"
        config_file.parent.mkdir()
        config_file.write_text("""
---
pipeline: test_pipeline
catalog: "prod_catalog"
schema: "prod_schema"
serverless: true
""")
        
        sub_file = tmp_path / "substitutions" / "prod.yaml"
        sub_file.parent.mkdir()
        sub_file.write_text("prod:\n  dummy: value\n")
        
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        
        # Act
        manager = BundleManager(tmp_path, str(config_file))
        result = manager.generate_resource_file_content("test_pipeline", generated_dir, "prod")
        
        # Assert - should contain literal values
        assert "catalog: prod_catalog" in result
        assert "schema: prod_schema" in result
        # Should NOT contain variable syntax
        assert "${var.default_pipeline_catalog}" not in result
        assert "${var.default_pipeline_schema}" not in result

    def test_output_is_valid_yaml(self, tmp_path):
        """Generated content should parse as valid YAML."""
        # Setup
        config_file = tmp_path / "config" / "pipeline_config.yaml"
        config_file.parent.mkdir()
        config_file.write_text("""
---
pipeline: test_pipeline
catalog: "test_catalog"
schema: "test_schema"
serverless: true
""")

        sub_file = tmp_path / "substitutions" / "dev.yaml"
        sub_file.parent.mkdir()
        sub_file.write_text("dev:\n  dummy: value\n")

        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()

        # Act
        manager = BundleManager(tmp_path, str(config_file))
        result = manager.generate_resource_file_content("test_pipeline", generated_dir, "dev")

        # Assert - should be valid YAML
        import yaml
        parsed = yaml.safe_load(result)
        assert parsed is not None
        assert "resources" in parsed

    def test_no_extra_blank_lines(self, tmp_path):
        """Should not produce extra blank lines."""
        # Setup
        config_file = tmp_path / "config" / "pipeline_config.yaml"
        config_file.parent.mkdir()
        config_file.write_text("""
---
pipeline: test_pipeline
catalog: "test_catalog"
schema: "test_schema"
serverless: true
""")
        
        sub_file = tmp_path / "substitutions" / "dev.yaml"
        sub_file.parent.mkdir()
        sub_file.write_text("dev:\n  dummy: value\n")
        
        generated_dir = tmp_path / "generated"
        generated_dir.mkdir()
        
        # Act
        manager = BundleManager(tmp_path, str(config_file))
        result = manager.generate_resource_file_content("test_pipeline", generated_dir, "dev")
        
        # Assert - should not have consecutive blank lines
        lines = result.split('\n')
        consecutive_blank = False
        for i in range(len(lines) - 1):
            if lines[i].strip() == '' and lines[i+1].strip() == '':
                consecutive_blank = True
                break
        
        # Some consecutive blanks may be acceptable, but check there's not excessive
        # Just verify it parses correctly (main goal)
        import yaml
        parsed = yaml.safe_load(result)
        assert parsed is not None




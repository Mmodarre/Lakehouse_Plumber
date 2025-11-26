"""
Test suite for full pipeline config substitution functionality.

Tests LHP token substitution across ALL fields in pipeline_config.yaml,
including nested structures, lists, and dict values.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import tempfile
import shutil
import yaml

from lhp.bundle.manager import BundleManager
from lhp.core.services.pipeline_config_loader import PipelineConfigLoader


class TestFullPipelineConfigSubstitution:
    """Test full substitution support for pipeline_config.yaml"""
    
    @pytest.fixture
    def temp_project(self):
        """Create temporary project structure"""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)
        
        # Create directories
        (project_root / "substitutions").mkdir(parents=True)
        (project_root / "config").mkdir(parents=True)
        (project_root / "pipelines").mkdir(parents=True)
        (project_root / "generated").mkdir(parents=True)
        
        # Create dev substitution file with comprehensive tokens
        sub_content = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze_dev",
                "node_type": "Standard_D8ds_v5",
                "policy_id": "dev-policy-123",
                "ops_email": "dev-ops@company.com",
                "environment": "development",
                "event_log_catalog": "dev_meta"
            }
        }
        with open(project_root / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)
        
        yield project_root
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    @pytest.fixture
    def pipeline_config_with_tokens(self, temp_project):
        """Create pipeline_config.yaml with tokens across various fields"""
        config_content = """
---
# Project defaults
serverless: false

---
# Pipeline with tokens in multiple fields
pipeline: test_pipeline
catalog: "{catalog}"
schema: "{bronze_schema}"
serverless: false
clusters:
  - label: default
    node_type_id: "{node_type}"
    policy_id: "{policy_id}"
notifications:
  - email_recipients:
      - "{ops_email}"
    alerts:
      - on-update-failure
tags:
  environment: "{environment}"
  team: data-engineering
event_log:
  name: test_events
  schema: _meta
  catalog: "{event_log_catalog}"
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        return config_path
    
    def test_full_substitution_node_type_and_policy(self, temp_project, pipeline_config_with_tokens):
        """Test token substitution in node_type_id and policy_id"""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(pipeline_config_with_tokens)
        )
        
        # Mock template renderer to capture context
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        # Generate resource content
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify substitution occurred
        assert captured_context is not None
        config = captured_context["pipeline_config"]
        
        # Check clusters substitution
        assert config["clusters"][0]["node_type_id"] == "Standard_D8ds_v5"
        assert config["clusters"][0]["policy_id"] == "dev-policy-123"
    
    def test_full_substitution_email_recipients(self, temp_project, pipeline_config_with_tokens):
        """Test token substitution in notification email list"""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(pipeline_config_with_tokens)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify email substitution
        config = captured_context["pipeline_config"]
        assert config["notifications"][0]["email_recipients"][0] == "dev-ops@company.com"
    
    def test_full_substitution_tags(self, temp_project, pipeline_config_with_tokens):
        """Test token substitution in tag values"""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(pipeline_config_with_tokens)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify tag substitution
        config = captured_context["pipeline_config"]
        assert config["tags"]["environment"] == "development"
        assert config["tags"]["team"] == "data-engineering"  # Literal value preserved
    
    def test_full_substitution_event_log(self, temp_project, pipeline_config_with_tokens):
        """Test token substitution in nested event_log structure"""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(pipeline_config_with_tokens)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify event_log substitution
        config = captured_context["pipeline_config"]
        assert config["event_log"]["catalog"] == "dev_meta"
        assert config["event_log"]["name"] == "test_events"  # Literal preserved
        assert config["event_log"]["schema"] == "_meta"  # Literal preserved
    
    def test_catalog_schema_validation_still_works(self, temp_project, pipeline_config_with_tokens):
        """Test that catalog/schema validation is still enforced after substitution"""
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(pipeline_config_with_tokens)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify catalog/schema were resolved and validated
        assert captured_context["catalog"] == "dev_catalog"
        assert captured_context["schema"] == "bronze_dev"
    
    def test_missing_substitution_file_uses_raw_config(self, temp_project):
        """Test that missing substitution file gracefully uses raw config"""
        # Create config with tokens
        config_content = """
---
pipeline: test_pipeline
catalog: prod_catalog
schema: prod_schema
serverless: true
clusters:
  - label: default
    node_type_id: "Standard_D16ds_v5"
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        # Remove substitution file
        (temp_project / "substitutions" / "dev.yaml").unlink()
        
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("test_pipeline", output_dir, "dev")
        
        # Verify raw values are used (no substitution)
        config = captured_context["pipeline_config"]
        assert config["catalog"] == "prod_catalog"
        assert config["schema"] == "prod_schema"
        assert config["clusters"][0]["node_type_id"] == "Standard_D16ds_v5"
    
    def test_mixed_literal_and_token_values(self, temp_project):
        """Test configuration with both literal values and tokens"""
        # Create substitution file
        sub_content = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze_dev"
            }
        }
        with open(temp_project / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)
        
        # Create config with mixed values
        config_content = """
---
pipeline: mixed_pipeline
catalog: "{catalog}"
schema: "{bronze_schema}"
serverless: false
edition: ADVANCED
continuous: true
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("mixed_pipeline", output_dir, "dev")
        
        config = captured_context["pipeline_config"]
        
        # Tokens resolved
        assert config["catalog"] == "dev_catalog"
        assert config["schema"] == "bronze_dev"
        
        # Literals preserved
        assert config["serverless"] == False
        assert config["edition"] == "ADVANCED"
        assert config["continuous"] == True
    
    def test_partial_catalog_schema_definition_raises_error(self, temp_project):
        """Test that defining only catalog OR schema raises an error"""
        # Create config with only catalog
        config_content = """
---
pipeline: invalid_pipeline
catalog: "{catalog}"
serverless: true
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path)
        )
        
        output_dir = temp_project / "generated"
        
        # Should raise ValueError for partial definition
        with pytest.raises(ValueError) as exc_info:
            manager.generate_resource_file_content("invalid_pipeline", output_dir, "dev")
        
        assert "BOTH catalog AND schema" in str(exc_info.value)
    
    def test_empty_catalog_after_substitution_raises_error(self, temp_project):
        """Test that empty catalog after substitution raises an error"""
        # Create substitution with empty value
        sub_content = {
            "dev": {
                "catalog": "",
                "bronze_schema": "bronze_dev"
            }
        }
        with open(temp_project / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)
        
        config_content = """
---
pipeline: empty_catalog_pipeline
catalog: "{catalog}"
schema: "{bronze_schema}"
serverless: true
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path)
        )
        
        output_dir = temp_project / "generated"
        
        # Should raise ValueError - empty strings are treated as missing
        # due to falsy evaluation in Python
        with pytest.raises(ValueError) as exc_info:
            manager.generate_resource_file_content("empty_catalog_pipeline", output_dir, "dev")
        
        # Empty strings are treated as missing, not as empty
        assert "BOTH catalog AND schema" in str(exc_info.value)
    
    def test_unresolved_tokens_pass_through(self, temp_project):
        """Test that unresolved tokens pass through (EnhancedSubstitutionManager behavior)"""
        # Create substitution with some but not all tokens
        sub_content = {
            "dev": {
                "catalog": "dev_catalog",
                "bronze_schema": "bronze_dev"
            }
            # Missing: nonexistent_token
        }
        with open(temp_project / "substitutions" / "dev.yaml", "w") as f:
            yaml.dump(sub_content, f)
        
        config_content = """
---
pipeline: unresolved_pipeline
catalog: "{catalog}"
schema: "{bronze_schema}"
serverless: true
tags:
  unknown: "{nonexistent_token}"
"""
        config_path = temp_project / "config" / "pipeline_config.yaml"
        with open(config_path, "w") as f:
            f.write(config_content)
        
        manager = BundleManager(
            project_root=temp_project,
            pipeline_config_path=str(config_path)
        )
        
        captured_context = None
        def mock_render(template_name, context):
            nonlocal captured_context
            captured_context = context
            return "mock_content"
        
        manager.template_renderer.render_template = mock_render
        
        output_dir = temp_project / "generated"
        result = manager.generate_resource_file_content("unresolved_pipeline", output_dir, "dev")
        
        config = captured_context["pipeline_config"]
        
        # Resolved tokens work
        assert config["catalog"] == "dev_catalog"
        assert config["schema"] == "bronze_dev"
        
        # Unresolved tokens pass through (EnhancedSubstitutionManager preserves them)
        # This is expected behavior - not an error
        assert "{nonexistent_token}" in config["tags"]["unknown"]


class TestSubstitutionWithExistingTests:
    """Ensure full substitution doesn't break existing test scenarios"""
    
    def test_backward_compatibility_no_tokens(self):
        """Test that configs without tokens still work"""
        temp_dir = tempfile.mkdtemp()
        project_root = Path(temp_dir)
        
        try:
            # Setup
            (project_root / "substitutions").mkdir(parents=True)
            (project_root / "config").mkdir(parents=True)
            (project_root / "generated").mkdir(parents=True)
            
            # Create substitution file (but won't be used)
            sub_content = {"dev": {"catalog": "dev_catalog"}}
            with open(project_root / "substitutions" / "dev.yaml", "w") as f:
                yaml.dump(sub_content, f)
            
            # Create config WITHOUT tokens
            config_content = """
---
pipeline: no_tokens_pipeline
catalog: literal_catalog
schema: literal_schema
serverless: true
"""
            config_path = project_root / "config" / "pipeline_config.yaml"
            with open(config_path, "w") as f:
                f.write(config_content)
            
            manager = BundleManager(
                project_root=project_root,
                pipeline_config_path=str(config_path)
            )
            
            captured_context = None
            def mock_render(template_name, context):
                nonlocal captured_context
                captured_context = context
                return "mock_content"
            
            manager.template_renderer.render_template = mock_render
            
            output_dir = project_root / "generated"
            result = manager.generate_resource_file_content("no_tokens_pipeline", output_dir, "dev")
            
            # Verify literals are preserved
            config = captured_context["pipeline_config"]
            assert config["catalog"] == "literal_catalog"
            assert config["schema"] == "literal_schema"
            
        finally:
            shutil.rmtree(temp_dir)


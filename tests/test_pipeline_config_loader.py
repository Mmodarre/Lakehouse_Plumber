"""Tests for PipelineConfigLoader service."""

import pytest
import yaml
from pathlib import Path

from lhp.core.services.pipeline_config_loader import PipelineConfigLoader


class TestConfigLoading:
    """Test config file loading and parsing."""
    
    def test_no_config_file_uses_defaults(self, tmp_path):
        """When no config specified, use DEFAULT_PIPELINE_CONFIG."""
        loader = PipelineConfigLoader(tmp_path, config_file_path=None)
        
        config = loader.get_pipeline_config("any_pipeline")
        
        # Should get default config
        assert config["serverless"] is True
        assert config["edition"] == "ADVANCED"
        assert config["channel"] == "CURRENT"
        assert config["continuous"] is False
    
    def test_load_multi_document_yaml(self, tmp_path):
        """Parse YAML with --- separators correctly."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/valid_multi_doc.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Check project defaults loaded
        assert loader.project_defaults["serverless"] is True
        assert loader.project_defaults["edition"] == "ADVANCED"
        
        # Check pipeline-specific configs loaded
        assert "test_pipeline_1" in loader.pipeline_configs
        assert "test_pipeline_2" in loader.pipeline_configs
        
        assert loader.pipeline_configs["test_pipeline_1"]["serverless"] is False
        assert loader.pipeline_configs["test_pipeline_2"]["continuous"] is True
    
    def test_load_project_defaults_only(self, tmp_path):
        """Handle config with only project_defaults, no pipelines."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/project_defaults_only.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Project defaults should be loaded
        assert loader.project_defaults["serverless"] is True
        assert loader.project_defaults["edition"] == "PRO"
        assert loader.project_defaults["photon"] is False
        
        # No pipeline-specific configs
        assert len(loader.pipeline_configs) == 0
    
    def test_load_no_project_defaults(self, tmp_path):
        """Handle config with only pipeline sections, no project_defaults."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/no_project_defaults.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # No project defaults
        assert loader.project_defaults == {}
        
        # Pipeline-specific config should be loaded
        assert "solo_pipeline" in loader.pipeline_configs
        assert loader.pipeline_configs["solo_pipeline"]["serverless"] is False
        assert loader.pipeline_configs["solo_pipeline"]["edition"] == "CORE"
    
    def test_missing_explicit_config_raises_error(self, tmp_path):
        """FileNotFoundError when explicit config doesn't exist."""
        nonexistent_path = "nonexistent/config.yaml"
        
        with pytest.raises(FileNotFoundError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=nonexistent_path)
        
        assert "Pipeline config file not found" in str(exc_info.value)
        assert nonexistent_path in str(exc_info.value)
    
    def test_invalid_yaml_raises_error(self, tmp_path):
        """yaml.YAMLError for invalid YAML syntax."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/invalid_yaml.yaml"
        
        with pytest.raises(yaml.YAMLError):
            PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
    
    def test_empty_config_file_uses_defaults(self, tmp_path):
        """Empty file returns defaults."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/empty_file.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Should have empty project defaults and no pipelines
        assert loader.project_defaults == {}
        assert loader.pipeline_configs == {}
        
        # But get_pipeline_config should still return defaults
        config = loader.get_pipeline_config("any_pipeline")
        assert config["serverless"] is True
        assert config["edition"] == "ADVANCED"


class TestConfigMerging:
    """Test config merge logic."""
    
    def test_pipeline_inherits_project_defaults(self, tmp_path):
        """Pipeline not in config gets project_defaults."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/valid_multi_doc.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Request a pipeline not explicitly in the config
        config = loader.get_pipeline_config("unlisted_pipeline")
        
        # Should inherit from project_defaults
        assert config["serverless"] is True  # from project_defaults
        assert config["edition"] == "ADVANCED"  # from project_defaults
        assert config["continuous"] is False  # from project_defaults
    
    def test_pipeline_overrides_project_defaults(self, tmp_path):
        """Pipeline-specific values override project defaults."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/valid_multi_doc.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        config = loader.get_pipeline_config("test_pipeline_1")
        
        # Overridden value
        assert config["serverless"] is False  # overridden
        
        # Inherited values
        assert config["edition"] == "ADVANCED"  # inherited from project_defaults
        assert config["continuous"] is False  # inherited from project_defaults
        
        # Pipeline-specific new key
        assert "clusters" in config
        assert config["clusters"][0]["label"] == "default"
    
    def test_deep_merge_nested_dicts(self, tmp_path):
        """Nested dicts merge recursively (autoscale config)."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/clusters_config.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        config = loader.get_pipeline_config("cluster_pipeline")
        
        # Check deep merge worked
        assert config["serverless"] is False
        assert "clusters" in config
        cluster = config["clusters"][0]
        assert cluster["node_type_id"] == "Standard_D16ds_v5"
        assert cluster["autoscale"]["min_workers"] == 2
        assert cluster["autoscale"]["max_workers"] == 10
        assert cluster["autoscale"]["mode"] == "ENHANCED"
    
    def test_lists_replaced_not_merged(self, tmp_path):
        """Lists override completely (notifications)."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/notifications_config.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Project defaults have one notification
        project_config = loader.get_pipeline_config("unlisted_pipeline")
        assert len(project_config["notifications"]) == 1
        assert project_config["notifications"][0]["email_recipients"][0] == "admin@company.com"
        
        # Pipeline override replaces completely (doesn't append)
        override_config = loader.get_pipeline_config("override_pipeline")
        assert len(override_config["notifications"]) == 1
        assert override_config["notifications"][0]["email_recipients"][0] == "team@company.com"
        assert "admin@company.com" not in str(override_config["notifications"])
    
    def test_pipeline_not_in_config_uses_defaults(self, tmp_path):
        """Pipeline not in file uses DEFAULT_PIPELINE_CONFIG only."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/no_project_defaults.yaml"
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        # Request a pipeline that's not in the config file
        config = loader.get_pipeline_config("other_pipeline")
        
        # Should get pure defaults (no project_defaults exist)
        assert config["serverless"] is True
        assert config["edition"] == "ADVANCED"
        assert config["channel"] == "CURRENT"
        assert config["continuous"] is False


class TestConfigValidation:
    """Test configuration validation."""
    
    def test_validate_edition_allowed_values(self, tmp_path):
        """Edition must be CORE, PRO, or ADVANCED."""
        # Create a config with valid edition
        config_content = """
project_defaults:
  edition: CORE
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)
        
        # Should not raise
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        config = loader.get_pipeline_config("test")
        assert config["edition"] == "CORE"
    
    def test_validate_channel_allowed_values(self, tmp_path):
        """Channel must be CURRENT or PREVIEW."""
        # Create a config with valid channel
        config_content = """
project_defaults:
  channel: PREVIEW
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)
        
        # Should not raise
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        config = loader.get_pipeline_config("test")
        assert config["channel"] == "PREVIEW"
    
    def test_invalid_edition_helpful_error(self, tmp_path):
        """Error message includes allowed values."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/invalid_edition.yaml"
        
        with pytest.raises(ValueError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        error_msg = str(exc_info.value)
        assert "Invalid edition 'PREMIUM'" in error_msg
        assert "ADVANCED" in error_msg
        assert "CORE" in error_msg
        assert "PRO" in error_msg
    
    def test_invalid_channel_helpful_error(self, tmp_path):
        """Error message includes allowed values."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/invalid_channel.yaml"
        
        with pytest.raises(ValueError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        
        error_msg = str(exc_info.value)
        assert "Invalid channel 'BETA'" in error_msg
        assert "CURRENT" in error_msg
        assert "PREVIEW" in error_msg
    
    def test_unknown_top_level_keys_ignored(self, tmp_path):
        """Unknown keys ignored (forward compatibility)."""
        config_content = """
project_defaults:
  serverless: true
  future_feature: enabled
  unknown_setting: 123
"""
        config_file = tmp_path / "config.yaml"
        config_file.write_text(config_content)
        
        # Should not raise - unknown keys just passed through
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        config = loader.get_pipeline_config("test")
        
        # Known keys work
        assert config["serverless"] is True
        
        # Unknown keys are included (pass-through)
        assert config["future_feature"] == "enabled"
        assert config["unknown_setting"] == 123
    
    def test_cluster_structure_not_validated(self, tmp_path):
        """Cluster config passed through without validation."""
        fixture_path = Path(__file__).parent / "fixtures/pipeline_configs/clusters_config.yaml"
        
        # Should not raise even though we don't validate cluster structure
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(fixture_path))
        config = loader.get_pipeline_config("cluster_pipeline")
        
        # Cluster config is present and passed through
        assert "clusters" in config
        assert isinstance(config["clusters"], list)
        assert len(config["clusters"]) == 1


class TestPipelineAsList:
    """Test list-based pipeline syntax for applying config to multiple pipelines."""
    
    def test_pipeline_as_list_expands_to_multiple_configs(self, tmp_path):
        """Test that pipeline as list expands to multiple pipeline configs."""
        config_content = """project_defaults:
  serverless: true
  edition: ADVANCED
---
pipeline:
  - pipeline_1
  - pipeline_2
serverless: false
edition: PRO
clusters:
  - label: default
    node_type_id: Standard_D16ds_v5
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        # Both pipelines should exist
        assert "pipeline_1" in loader.pipeline_configs
        assert "pipeline_2" in loader.pipeline_configs
        
        # Both should have the same config values
        assert loader.pipeline_configs["pipeline_1"]["serverless"] is False
        assert loader.pipeline_configs["pipeline_2"]["serverless"] is False
        assert loader.pipeline_configs["pipeline_1"]["edition"] == "PRO"
        assert loader.pipeline_configs["pipeline_2"]["edition"] == "PRO"
    
    def test_pipeline_list_with_single_item(self, tmp_path):
        """Test that pipeline list with single item works correctly."""
        config_content = """project_defaults:
  serverless: true
---
pipeline:
  - solo_pipeline
serverless: false
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        assert "solo_pipeline" in loader.pipeline_configs
        assert loader.pipeline_configs["solo_pipeline"]["serverless"] is False
    
    def test_pipeline_empty_list_raises_error(self, tmp_path):
        """Test that empty pipeline list raises LHPError."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: []
serverless: false
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        from lhp.utils.error_formatter import LHPError
        
        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        error_str = str(exc_info.value)
        assert "Empty pipeline list" in error_str or "empty pipeline" in error_str.lower()
        assert "At least one" in error_str or "required" in error_str.lower()
    
    def test_pipeline_duplicate_in_same_list_raises_error(self, tmp_path):
        """Test that duplicate pipeline in same list raises LHPError."""
        config_content = """project_defaults:
  serverless: true
---
pipeline:
  - duplicate_pipeline
  - duplicate_pipeline
serverless: false
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        from lhp.utils.error_formatter import LHPError
        
        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        error_str = str(exc_info.value)
        assert "Duplicate" in error_str or "duplicate" in error_str.lower()
        assert "duplicate_pipeline" in error_str
    
    def test_pipeline_duplicate_across_documents_raises_error(self, tmp_path):
        """Test that duplicate pipeline across documents raises LHPError."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze_pipeline
serverless: false
---
pipeline:
  - silver_pipeline
  - bronze_pipeline
edition: PRO
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        from lhp.utils.error_formatter import LHPError
        
        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        error_str = str(exc_info.value)
        assert "Duplicate" in error_str or "duplicate" in error_str.lower()
        assert "bronze_pipeline" in error_str
    
    def test_pipeline_string_still_works(self, tmp_path):
        """Test backward compatibility - string pipeline still works."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: bronze_pipeline
serverless: false
edition: PRO
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        assert "bronze_pipeline" in loader.pipeline_configs
        assert loader.pipeline_configs["bronze_pipeline"]["serverless"] is False
        assert loader.pipeline_configs["bronze_pipeline"]["edition"] == "PRO"
    
    def test_pipeline_list_deep_copies_config(self, tmp_path):
        """Test that each pipeline gets independent deep copy of config."""
        config_content = """project_defaults:
  serverless: true
---
pipeline:
  - pipe1
  - pipe2
tags:
  env: dev
  nested:
    key: value
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        # Modify one config
        loader.pipeline_configs["pipe1"]["tags"]["modified"] = "yes"
        loader.pipeline_configs["pipe1"]["tags"]["nested"]["key"] = "changed"
        
        # Other config should not be affected
        assert "modified" not in loader.pipeline_configs["pipe2"]["tags"]
        assert loader.pipeline_configs["pipe2"]["tags"]["nested"]["key"] == "value"
    
    def test_pipeline_error_shows_document_numbers(self, tmp_path):
        """Test that error messages show which documents have conflicts."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: duplicate_pipeline
serverless: false
---
pipeline: duplicate_pipeline
edition: PRO
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        from lhp.utils.error_formatter import LHPError
        
        with pytest.raises(LHPError) as exc_info:
            PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        error_message = str(exc_info.value)
        assert "document" in error_message.lower()
        # Should mention document numbers
        assert "2" in error_message or "3" in error_message
    
    def test_pipeline_invalid_type_skipped_with_warning(self, tmp_path, caplog):
        """Test that invalid pipeline types are skipped with a warning."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: 456
serverless: false
---
pipeline: valid_pipeline
edition: PRO
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        # Invalid type should be skipped, but valid_pipeline should be loaded
        assert "valid_pipeline" in loader.pipeline_configs
        assert loader.pipeline_configs["valid_pipeline"]["edition"] == "PRO"
        
        # Should have warning in log
        assert "invalid pipeline type" in caplog.text.lower() or "invalid" in caplog.text.lower()
    
    def test_pipeline_mixed_types_in_same_config(self, tmp_path):
        """Test mixing string and list pipeline in different documents."""
        config_content = """project_defaults:
  serverless: true
---
pipeline: string_pipeline
serverless: false
tags:
  type: string
---
pipeline:
  - list_pipeline_1
  - list_pipeline_2
edition: PRO
tags:
  type: list
"""
        config_file = tmp_path / "pipeline_config.yaml"
        config_file.write_text(config_content)
        
        loader = PipelineConfigLoader(tmp_path, config_file_path=str(config_file))
        
        # All three pipelines should exist
        assert "string_pipeline" in loader.pipeline_configs
        assert "list_pipeline_1" in loader.pipeline_configs
        assert "list_pipeline_2" in loader.pipeline_configs
        
        # Verify configs
        assert loader.pipeline_configs["string_pipeline"]["serverless"] is False
        assert loader.pipeline_configs["string_pipeline"]["tags"]["type"] == "string"
        assert loader.pipeline_configs["list_pipeline_1"]["edition"] == "PRO"
        assert loader.pipeline_configs["list_pipeline_1"]["tags"]["type"] == "list"
        assert loader.pipeline_configs["list_pipeline_2"]["edition"] == "PRO"


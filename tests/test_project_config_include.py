"""Tests for project configuration include field functionality."""

import pytest
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import yaml

# Note: The actual implementation will be done later - these tests define the expected behavior


class TestProjectConfigInclude:
    """Test cases for project configuration include field."""

    def test_load_project_config_with_include(self, tmp_path):
        """Test loading project configuration with include field."""
        # Given: A project configuration with include patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "description": "Test project with include patterns",
            "include": [
                "bronze_*.yaml",
                "silver/**/*.yaml",
                "gold/customers.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Include patterns should be parsed and stored
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.name == "test_project"
        # assert config.include == ["bronze_*.yaml", "silver/**/*.yaml", "gold/customers.yaml"]

    def test_load_project_config_without_include(self, tmp_path):
        """Test loading project configuration without include field (backwards compatibility)."""
        # Given: A project configuration without include field
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "description": "Test project without include patterns"
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Include should be None/empty for backwards compatibility
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.name == "test_project"
        # assert config.include is None or config.include == []

    def test_load_project_config_with_empty_include(self, tmp_path):
        """Test loading project configuration with empty include list."""
        # Given: A project configuration with empty include list
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "description": "Test project with empty include",
            "include": []
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Include should be empty list
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.name == "test_project"
        # assert config.include == []

    def test_validate_include_patterns_valid(self):
        """Test validation of valid include patterns."""
        # Given: Valid include patterns
        valid_patterns = [
            "*.yaml",
            "bronze_*.yaml",
            "silver/**/*.yaml",
            "gold/specific_file.yaml",
            "pipelines/*/bronze/*.yaml",
            "data/**/raw_*.yaml"
        ]
        
        # When: Validating the patterns
        # Expected: All patterns should be valid
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(Path("."))
        # 
        # for pattern in valid_patterns:
        #     assert loader._validate_include_pattern(pattern) is True

    def test_validate_include_patterns_invalid(self):
        """Test validation of invalid include patterns."""
        # Given: Invalid include patterns
        invalid_patterns = [
            "",  # Empty pattern
            None,  # None pattern
            123,  # Non-string pattern
            "invalid[pattern",  # Invalid regex characters
            "***/invalid",  # Invalid glob syntax
        ]
        
        # When: Validating the patterns
        # Expected: Invalid patterns should raise ValueError
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(Path("."))
        # 
        # for pattern in invalid_patterns:
        #     with pytest.raises(ValueError):
        #         loader._validate_include_pattern(pattern)

    def test_project_config_with_include_validation_error(self, tmp_path):
        """Test project configuration with invalid include patterns."""
        # Given: A project configuration with invalid include patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "valid_*.yaml",
                "",  # Invalid empty pattern
                "invalid[pattern"  # Invalid regex
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Should raise appropriate error for invalid patterns
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # from lhp.utils.error_formatter import LHPError
        # 
        # loader = ProjectConfigLoader(tmp_path)
        # with pytest.raises(LHPError) as exc_info:
        #     loader.load_project_config()
        # 
        # assert "Invalid include pattern" in str(exc_info.value)

    def test_include_patterns_type_validation(self, tmp_path):
        """Test validation of include field type."""
        # Given: A project configuration with wrong type for include
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": "single_string_instead_of_list"  # Wrong type
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Should raise appropriate type error
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # from lhp.utils.error_formatter import LHPError
        # 
        # loader = ProjectConfigLoader(tmp_path)
        # with pytest.raises(LHPError) as exc_info:
        #     loader.load_project_config()
        # 
        # assert "must be a list" in str(exc_info.value)

    def test_include_patterns_with_both_extensions(self, tmp_path):
        """Test include patterns handling both .yaml and .yml extensions."""
        # Given: A project configuration with patterns for both extensions
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "*.yaml",
                "*.yml",
                "bronze_*.yaml",
                "silver_*.yml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Should handle both extensions correctly
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.include == ["*.yaml", "*.yml", "bronze_*.yaml", "silver_*.yml"]

    def test_include_patterns_with_operational_metadata(self, tmp_path):
        """Test include patterns work with operational metadata configuration."""
        # Given: A project configuration with both include and operational metadata
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "bronze_*.yaml",
                "silver/**/*.yaml"
            ],
            "operational_metadata": {
                "columns": {
                    "ingestion_timestamp": {
                        "expression": "current_timestamp()",
                        "description": "Timestamp when record was ingested"
                    }
                }
            }
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Both include and operational metadata should be loaded
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.include == ["bronze_*.yaml", "silver/**/*.yaml"]
        # assert config.operational_metadata is not None
        # assert "ingestion_timestamp" in config.operational_metadata.columns

    def test_include_patterns_case_sensitivity(self, tmp_path):
        """Test include patterns are case sensitive."""
        # Given: A project configuration with case-sensitive patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "Bronze_*.yaml",
                "bronze_*.yaml",
                "BRONZE_*.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Case-sensitive patterns should be preserved
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config.include == ["Bronze_*.yaml", "bronze_*.yaml", "BRONZE_*.yaml"]

    def test_include_patterns_with_complex_paths(self, tmp_path):
        """Test include patterns with complex directory structures."""
        # Given: A project configuration with complex path patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "pipelines/*/bronze/*.yaml",
                "pipelines/ingestion/**/raw_*.yaml",
                "pipelines/data/bronze/customers.yaml",
                "pipelines/events/*/silver/**/*.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Complex patterns should be preserved
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # expected = [
        #     "pipelines/*/bronze/*.yaml",
        #     "pipelines/ingestion/**/raw_*.yaml",
        #     "pipelines/data/bronze/customers.yaml",
        #     "pipelines/events/*/silver/**/*.yaml"
        # ]
        # assert config.include == expected

    def test_include_patterns_empty_file_handling(self, tmp_path):
        """Test handling of empty or missing lhp.yaml file."""
        # Given: Empty lhp.yaml file
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("")
        
        # When: Loading the project configuration
        # Expected: Should handle empty file gracefully
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config is None  # Empty file should return None

    def test_include_patterns_missing_file_handling(self, tmp_path):
        """Test handling of missing lhp.yaml file."""
        # Given: No lhp.yaml file
        
        # When: Loading the project configuration
        # Expected: Should handle missing file gracefully
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # assert config is None  # Missing file should return None

    def test_include_patterns_yaml_parsing_error(self, tmp_path):
        """Test handling of YAML parsing errors in include patterns."""
        # Given: Invalid YAML in lhp.yaml
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("""
name: test_project
version: "1.0"
include: [
  "valid_*.yaml",
  invalid_yaml: without_quotes
]
""")
        
        # When: Loading the project configuration
        # Expected: Should raise appropriate YAML parsing error
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # from lhp.utils.error_formatter import LHPError
        # 
        # loader = ProjectConfigLoader(tmp_path)
        # with pytest.raises(LHPError) as exc_info:
        #     loader.load_project_config()
        # 
        # assert "Invalid YAML" in str(exc_info.value)

    def test_include_patterns_with_relative_paths(self, tmp_path):
        """Test include patterns with relative path specifications."""
        # Given: A project configuration with relative path patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "./pipelines/bronze/*.yaml",
                "../shared/templates/*.yaml",
                "~/user/patterns/*.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Relative paths should be preserved as-is
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # expected = [
        #     "./pipelines/bronze/*.yaml",
        #     "../shared/templates/*.yaml",
        #     "~/user/patterns/*.yaml"
        # ]
        # assert config.include == expected

    def test_include_patterns_duplicate_handling(self, tmp_path):
        """Test handling of duplicate include patterns."""
        # Given: A project configuration with duplicate patterns
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "bronze_*.yaml",
                "silver_*.yaml",
                "bronze_*.yaml",  # Duplicate
                "gold_*.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # When: Loading the project configuration
        # Expected: Duplicates should be preserved (filtering happens at runtime)
        
        # This will be implemented later
        # from lhp.core.project_config_loader import ProjectConfigLoader
        # loader = ProjectConfigLoader(tmp_path)
        # config = loader.load_project_config()
        # 
        # expected = ["bronze_*.yaml", "silver_*.yaml", "bronze_*.yaml", "gold_*.yaml"]
        # assert config.include == expected


class TestProjectConfigIncludeIntegration:
    """Integration tests for project configuration include functionality."""

    def test_project_config_include_with_orchestrator(self, tmp_path):
        """Test project configuration include field works with ActionOrchestrator."""
        # Given: A project with configuration and actual pipeline files
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "include": [
                "bronze_*.yaml",
                "silver/**/*.yaml"
            ]
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # Create pipeline files
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        # Files that should be included
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "bronze_orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        
        silver_dir = pipelines_dir / "silver"
        silver_dir.mkdir()
        (silver_dir / "customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        
        # Files that should NOT be included
        (pipelines_dir / "gold_customers.yaml").write_text("pipeline: gold\nflowgroup: customers")
        
        # When: Orchestrator uses the project configuration
        # Expected: Only included files should be processed
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(tmp_path)
        # 
        # # The orchestrator should use the include patterns from project config
        # assert orchestrator.project_config.include == ["bronze_*.yaml", "silver/**/*.yaml"]

    def test_backwards_compatibility_no_include_field(self, tmp_path):
        """Test backwards compatibility when include field is not present."""
        # Given: A project without include field (legacy configuration)
        config_content = {
            "name": "test_project",
            "version": "1.0",
            "description": "Legacy project without include field"
        }
        
        config_file = tmp_path / "lhp.yaml"
        with open(config_file, "w") as f:
            yaml.dump(config_content, f)
        
        # Create pipeline files
        pipelines_dir = tmp_path / "pipelines"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "silver_orders.yaml").write_text("pipeline: silver\nflowgroup: orders")
        
        # When: Loading and using the configuration
        # Expected: Should work as before (include all files)
        
        # This will be implemented later
        # from lhp.core.orchestrator import ActionOrchestrator
        # orchestrator = ActionOrchestrator(tmp_path)
        # 
        # # Should work with all files when no include field is present
        # assert orchestrator.project_config.include is None or orchestrator.project_config.include == [] 
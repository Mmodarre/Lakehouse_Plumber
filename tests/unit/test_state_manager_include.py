"""Tests for StateManager.get_current_yaml_files() with include filtering."""

import pytest
import tempfile
import json
from pathlib import Path
from unittest.mock import Mock, patch

# Note: The actual implementation will be done later - these tests define the expected behavior


class TestStateManagerIncludeFiltering:
    """Test cases for StateManager file discovery with include patterns."""

    def test_get_current_yaml_files_with_include_patterns(self, tmp_path):
        """Test file discovery with include patterns."""
        # Given: A project with various YAML files and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "bronze_orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        (pipelines_dir / "silver_customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        (pipelines_dir / "gold_customers.yaml").write_text("pipeline: gold\nflowgroup: customers")
        
        # Mock project config with include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Getting current YAML files with include patterns
        # Expected: Only files matching include patterns should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # # Mock the project config to return include patterns
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "bronze_customers.yaml",
        #     pipelines_dir / "bronze_orders.yaml",
        #     pipelines_dir / "silver_customers.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_no_include_patterns(self, tmp_path):
        """Test file discovery without include patterns (backwards compatibility)."""
        # Given: A project with various YAML files and no include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "silver_orders.yaml").write_text("pipeline: silver\nflowgroup: orders")
        (pipelines_dir / "gold_products.yaml").write_text("pipeline: gold\nflowgroup: products")
        
        # When: Getting current YAML files without include patterns
        # Expected: All YAML files should be returned (backwards compatibility)
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # # Mock the project config to return no include patterns
        # with patch.object(state_manager, '_get_include_patterns', return_value=[]):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "bronze_customers.yaml",
        #     pipelines_dir / "silver_orders.yaml",
        #     pipelines_dir / "gold_products.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_empty_include_patterns(self, tmp_path):
        """Test file discovery with empty include patterns list."""
        # Given: A project with YAML files and empty include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        (pipelines_dir / "customers.yaml").write_text("pipeline: test\nflowgroup: customers")
        (pipelines_dir / "orders.yaml").write_text("pipeline: test\nflowgroup: orders")
        
        # When: Getting current YAML files with empty include patterns
        # Expected: All YAML files should be returned (empty list = no filtering)
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=[]):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "customers.yaml",
        #     pipelines_dir / "orders.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_recursive_patterns(self, tmp_path):
        """Test file discovery with recursive glob patterns."""
        # Given: A project with nested directories and recursive patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create nested directory structure
        bronze_dir = pipelines_dir / "bronze"
        bronze_raw_dir = bronze_dir / "raw"
        silver_dir = pipelines_dir / "silver"
        
        bronze_dir.mkdir(parents=True)
        bronze_raw_dir.mkdir(parents=True)
        silver_dir.mkdir(parents=True)
        
        # Create test files
        (bronze_dir / "customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (bronze_raw_dir / "events.yaml").write_text("pipeline: bronze\nflowgroup: events")
        (silver_dir / "customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        (pipelines_dir / "gold_customers.yaml").write_text("pipeline: gold\nflowgroup: customers")
        
        # Include pattern for recursive matching
        include_patterns = ["bronze/**/*.yaml"]
        
        # When: Getting current YAML files with recursive patterns
        # Expected: Only files in bronze directory tree should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     bronze_dir / "customers.yaml",
        #     bronze_raw_dir / "events.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_specific_pipeline_with_include(self, tmp_path):
        """Test file discovery for specific pipeline with include patterns."""
        # Given: A project with multiple pipelines and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create pipeline directories
        bronze_pipeline = pipelines_dir / "bronze_pipeline"
        silver_pipeline = pipelines_dir / "silver_pipeline"
        bronze_pipeline.mkdir(parents=True)
        silver_pipeline.mkdir(parents=True)
        
        # Create test files
        (bronze_pipeline / "customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (bronze_pipeline / "orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        (silver_pipeline / "customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        
        # Include patterns
        include_patterns = ["bronze_pipeline/*.yaml"]
        
        # When: Getting current YAML files for specific pipeline with include patterns
        # Expected: Only files matching both pipeline filter and include patterns
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files(pipeline="bronze_pipeline")
        # 
        # expected = {
        #     bronze_pipeline / "customers.yaml",
        #     bronze_pipeline / "orders.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_multiple_patterns(self, tmp_path):
        """Test file discovery with multiple include patterns."""
        # Given: A project with files and multiple include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "silver_orders.yaml").write_text("pipeline: silver\nflowgroup: orders")
        (pipelines_dir / "gold_products.yaml").write_text("pipeline: gold\nflowgroup: products")
        (pipelines_dir / "config.json").write_text("{}")  # Non-YAML file
        
        # Multiple include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Getting current YAML files with multiple patterns
        # Expected: Files matching any of the patterns should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "bronze_customers.yaml",
        #     pipelines_dir / "silver_orders.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_both_yaml_and_yml_extensions(self, tmp_path):
        """Test file discovery with both .yaml and .yml extensions."""
        # Given: A project with both .yaml and .yml files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files with both extensions
        (pipelines_dir / "customers.yaml").write_text("pipeline: test\nflowgroup: customers")
        (pipelines_dir / "orders.yml").write_text("pipeline: test\nflowgroup: orders")
        (pipelines_dir / "products.yaml").write_text("pipeline: test\nflowgroup: products")
        
        # Include patterns for both extensions
        include_patterns = ["*.yaml", "*.yml"]
        
        # When: Getting current YAML files with patterns for both extensions
        # Expected: Files with both extensions should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "customers.yaml",
        #     pipelines_dir / "orders.yml",
        #     pipelines_dir / "products.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_no_matches(self, tmp_path):
        """Test file discovery when no files match include patterns."""
        # Given: A project with files that don't match include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "silver_orders.yaml").write_text("pipeline: silver\nflowgroup: orders")
        
        # Include patterns that don't match any files
        include_patterns = ["gold_*.yaml", "platinum_*.yaml"]
        
        # When: Getting current YAML files with non-matching patterns
        # Expected: Empty set should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # assert yaml_files == set()

    def test_get_current_yaml_files_nonexistent_pipelines_dir(self, tmp_path):
        """Test file discovery when pipelines directory doesn't exist."""
        # Given: A project without pipelines directory
        project_root = tmp_path
        
        # Include patterns
        include_patterns = ["*.yaml"]
        
        # When: Getting current YAML files with non-existent pipelines directory
        # Expected: Empty set should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # assert yaml_files == set()

    def test_get_current_yaml_files_complex_nested_patterns(self, tmp_path):
        """Test file discovery with complex nested directory patterns."""
        # Given: A project with complex nested structure
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        
        # Create complex directory structure
        data_bronze = pipelines_dir / "data" / "bronze"
        events_bronze = pipelines_dir / "events" / "bronze"
        ingestion_raw = pipelines_dir / "ingestion" / "raw"
        ingestion_nested = pipelines_dir / "ingestion" / "nested" / "deep"
        
        data_bronze.mkdir(parents=True)
        events_bronze.mkdir(parents=True)
        ingestion_raw.mkdir(parents=True)
        ingestion_nested.mkdir(parents=True)
        
        # Create test files
        (data_bronze / "customers.yaml").write_text("pipeline: data\nflowgroup: customers")
        (events_bronze / "orders.yaml").write_text("pipeline: events\nflowgroup: orders")
        (ingestion_raw / "raw_events.yaml").write_text("pipeline: ingestion\nflowgroup: raw_events")
        (ingestion_nested / "raw_logs.yaml").write_text("pipeline: ingestion\nflowgroup: raw_logs")
        (pipelines_dir / "gold_summary.yaml").write_text("pipeline: gold\nflowgroup: summary")
        
        # Complex include patterns
        include_patterns = [
            "*/bronze/*.yaml",
            "ingestion/**/raw_*.yaml"
        ]
        
        # When: Getting current YAML files with complex patterns
        # Expected: Files matching either pattern should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     data_bronze / "customers.yaml",
        #     events_bronze / "orders.yaml",
        #     ingestion_raw / "raw_events.yaml",
        #     ingestion_nested / "raw_logs.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_case_sensitivity(self, tmp_path):
        """Test file discovery with case-sensitive patterns."""
        # Given: A project with files with different cases
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create test files with different cases
        (pipelines_dir / "Bronze_customers.yaml").write_text("pipeline: Bronze\nflowgroup: customers")
        (pipelines_dir / "bronze_orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        (pipelines_dir / "BRONZE_products.yaml").write_text("pipeline: BRONZE\nflowgroup: products")
        
        # Case-sensitive include pattern
        include_patterns = ["bronze_*.yaml"]
        
        # When: Getting current YAML files with case-sensitive pattern
        # Expected: Only exact case matches should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "bronze_orders.yaml"
        # }
        # assert yaml_files == expected

    def test_get_current_yaml_files_with_project_config_integration(self, tmp_path):
        """Test file discovery integrating with project configuration."""
        # Given: A project with lhp.yaml containing include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create project configuration
        config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"
  - "silver/**/*.yaml"
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # Create test files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "bronze_orders.yaml").write_text("pipeline: bronze\nflowgroup: orders")
        
        silver_dir = pipelines_dir / "silver"
        silver_dir.mkdir()
        (silver_dir / "customers.yaml").write_text("pipeline: silver\nflowgroup: customers")
        
        (pipelines_dir / "gold_products.yaml").write_text("pipeline: gold\nflowgroup: products")
        
        # When: Getting current YAML files with project config integration
        # Expected: Files matching project config include patterns should be returned
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # # Should automatically use include patterns from project config
        # yaml_files = state_manager.get_current_yaml_files()
        # 
        # expected = {
        #     pipelines_dir / "bronze_customers.yaml",
        #     pipelines_dir / "bronze_orders.yaml",
        #     silver_dir / "customers.yaml"
        # }
        # assert yaml_files == expected


class TestStateManagerIncludeIntegration:
    """Integration tests for StateManager with include functionality."""

    def test_find_new_yaml_files_with_include_patterns(self, tmp_path):
        """Test finding new YAML files with include patterns."""
        # Given: A project with files and existing state, plus include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create existing files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")
        (pipelines_dir / "silver_orders.yaml").write_text("pipeline: silver\nflowgroup: orders")
        (pipelines_dir / "gold_products.yaml").write_text("pipeline: gold\nflowgroup: products")
        
        # Create state with some tracked files
        state_file = project_root / ".lhp_state.json"
        state_data = {
            "version": "1.0",
            "last_updated": "2023-01-01T00:00:00",
            "environments": {
                "dev": {
                    "generated/bronze_customers.py": {
                        "source_yaml": "pipelines/bronze_customers.yaml",
                        "generated_path": "generated/bronze_customers.py",
                        "environment": "dev",
                        "pipeline": "bronze",
                        "flowgroup": "customers",
                        "source_yaml_checksum": "abc123"
                    }
                }
            }
        }
        
        with open(state_file, "w") as f:
            json.dump(state_data, f)
        
        # Include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Finding new YAML files with include patterns
        # Expected: Only new files matching include patterns should be found
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     new_files = state_manager.find_new_yaml_files("dev")
        # 
        # expected = [pipelines_dir / "silver_orders.yaml"]
        # assert new_files == expected

    def test_find_stale_files_with_include_patterns(self, tmp_path):
        """Test finding stale files with include patterns."""
        # Given: A project with modified files and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers\n# modified")
        (pipelines_dir / "gold_products.yaml").write_text("pipeline: gold\nflowgroup: products\n# modified")
        
        # Create state with old checksums
        state_file = project_root / ".lhp_state.json"
        state_data = {
            "version": "1.0",
            "last_updated": "2023-01-01T00:00:00",
            "environments": {
                "dev": {
                    "generated/bronze_customers.py": {
                        "source_yaml": "pipelines/bronze_customers.yaml",
                        "generated_path": "generated/bronze_customers.py",
                        "environment": "dev",
                        "pipeline": "bronze",
                        "flowgroup": "customers",
                        "source_yaml_checksum": "old_checksum"
                    },
                    "generated/gold_products.py": {
                        "source_yaml": "pipelines/gold_products.yaml",
                        "generated_path": "generated/gold_products.py",
                        "environment": "dev",
                        "pipeline": "gold",
                        "flowgroup": "products",
                        "source_yaml_checksum": "old_checksum"
                    }
                }
            }
        }
        
        with open(state_file, "w") as f:
            json.dump(state_data, f)
        
        # Include patterns
        include_patterns = ["bronze_*.yaml"]
        
        # When: Finding stale files with include patterns
        # Expected: Only stale files matching include patterns should be found
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     stale_files = state_manager.find_stale_files("dev")
        # 
        # # Should only find bronze file as stale (gold file excluded by include pattern)
        # assert len(stale_files) == 1
        # assert stale_files[0].flowgroup == "customers"

    def test_get_files_needing_generation_with_include_patterns(self, tmp_path):
        """Test getting files needing generation with include patterns."""
        # Given: A project with mixed file states and include patterns
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create files
        (pipelines_dir / "bronze_customers.yaml").write_text("pipeline: bronze\nflowgroup: customers")  # New
        (pipelines_dir / "bronze_orders.yaml").write_text("pipeline: bronze\nflowgroup: orders\n# modified")  # Stale
        (pipelines_dir / "silver_products.yaml").write_text("pipeline: silver\nflowgroup: products")  # Up-to-date
        (pipelines_dir / "gold_summary.yaml").write_text("pipeline: gold\nflowgroup: summary")  # Excluded
        
        # Create state
        state_file = project_root / ".lhp_state.json"
        state_data = {
            "version": "1.0",
            "last_updated": "2023-01-01T00:00:00",
            "environments": {
                "dev": {
                    "generated/bronze_orders.py": {
                        "source_yaml": "pipelines/bronze_orders.yaml",
                        "generated_path": "generated/bronze_orders.py",
                        "environment": "dev",
                        "pipeline": "bronze",
                        "flowgroup": "orders",
                        "source_yaml_checksum": "old_checksum"
                    },
                    "generated/silver_products.py": {
                        "source_yaml": "pipelines/silver_products.yaml",
                        "generated_path": "generated/silver_products.py",
                        "environment": "dev",
                        "pipeline": "silver",
                        "flowgroup": "products",
                        "source_yaml_checksum": "current_checksum"
                    },
                    "generated/gold_summary.py": {
                        "source_yaml": "pipelines/gold_summary.yaml",
                        "generated_path": "generated/gold_summary.py",
                        "environment": "dev",
                        "pipeline": "gold",
                        "flowgroup": "summary",
                        "source_yaml_checksum": "old_checksum"
                    }
                }
            }
        }
        
        with open(state_file, "w") as f:
            json.dump(state_data, f)
        
        # Include patterns
        include_patterns = ["bronze_*.yaml", "silver_*.yaml"]
        
        # When: Getting files needing generation with include patterns
        # Expected: Only files matching include patterns should be considered
        
        # This will be implemented later
        # from lhp.core.state_manager import StateManager
        # state_manager = StateManager(project_root)
        # 
        # with patch.object(state_manager, '_get_include_patterns', return_value=include_patterns):
        #     generation_info = state_manager.get_files_needing_generation("dev")
        # 
        # # Should find new bronze_customers, stale bronze_orders, up-to-date silver_products
        # # Gold file should be excluded by include pattern
        # assert len(generation_info["new"]) == 1
        # assert generation_info["new"][0].name == "bronze_customers.yaml"
        # assert len(generation_info["stale"]) == 1
        # assert generation_info["stale"][0].flowgroup == "orders"
        # assert len(generation_info["up_to_date"]) == 1
        # assert generation_info["up_to_date"][0].flowgroup == "products" 

    def test_find_orphaned_files_with_include_patterns_changes(self, tmp_path):
        """Test that find_orphaned_files identifies files that no longer match include patterns."""
        # Given: A project with include patterns and generated files
        project_root = tmp_path
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        
        # Create project configuration with initial include patterns
        config_content = """
name: test_project
version: "1.0"
include:
  - "*.yaml"  # Initially include all files
"""
        (project_root / "lhp.yaml").write_text(config_content)
        
        # Create test YAML files
        (pipelines_dir / "bronze_customers.yaml").write_text("""
pipeline: bronze_layer
flowgroup: customers
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers
    target: v_customers
""")
        
        (pipelines_dir / "silver_orders.yaml").write_text("""
pipeline: silver_layer
flowgroup: orders
actions:
  - name: load_orders
    type: load
    source:
      type: cloudfiles
      path: /data/orders
    target: v_orders
""")
        
        # Create state manager and track some files
        from lhp.core.state_manager import StateManager, FileState
        state_manager = StateManager(project_root)
        
        # Simulate files being generated and tracked
        bronze_file_state = FileState(
            source_yaml="pipelines/bronze_customers.yaml",
            generated_path="generated/bronze_layer/customers.py",
            checksum="abc123",
            source_yaml_checksum="def456",
            timestamp="2023-01-01T00:00:00",
            environment="dev",
            pipeline="bronze_layer",
            flowgroup="customers"
        )
        
        silver_file_state = FileState(
            source_yaml="pipelines/silver_orders.yaml",
            generated_path="generated/silver_layer/orders.py",
            checksum="xyz789",
            source_yaml_checksum="uvw012",
            timestamp="2023-01-01T00:00:00",
            environment="dev",
            pipeline="silver_layer",
            flowgroup="orders"
        )
        
        # Track these files in state
        state_manager._state.environments["dev"] = {
            "generated/bronze_layer/customers.py": bronze_file_state,
            "generated/silver_layer/orders.py": silver_file_state
        }
        
        # Initially, no files should be orphaned (all match include patterns)
        orphaned_files = state_manager.find_orphaned_files("dev")
        assert len(orphaned_files) == 0
        
        # When: Update include patterns to exclude silver files
        updated_config_content = """
name: test_project
version: "1.0"
include:
  - "bronze_*.yaml"  # Only include bronze files now
"""
        (project_root / "lhp.yaml").write_text(updated_config_content)
        
        # Then: silver_orders.yaml should now be considered orphaned
        orphaned_files = state_manager.find_orphaned_files("dev")
        assert len(orphaned_files) == 1
        assert orphaned_files[0].source_yaml == "pipelines/silver_orders.yaml"
        assert orphaned_files[0].flowgroup == "orders"
        
        # bronze file should still not be orphaned
        orphaned_sources = [f.source_yaml for f in orphaned_files]
        assert "pipelines/bronze_customers.yaml" not in orphaned_sources
        
        # When: Delete a YAML file that was still included
        (pipelines_dir / "bronze_customers.yaml").unlink()
        
        # Then: Both files should be orphaned (one missing, one excluded)
        orphaned_files = state_manager.find_orphaned_files("dev")
        assert len(orphaned_files) == 2
        
        orphaned_sources = [f.source_yaml for f in orphaned_files]
        assert "pipelines/bronze_customers.yaml" in orphaned_sources  # Missing file
        assert "pipelines/silver_orders.yaml" in orphaned_sources   # Excluded by include patterns 
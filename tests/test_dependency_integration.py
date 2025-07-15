"""Integration tests for dependency tracking functionality."""

import tempfile
import pytest
from pathlib import Path
from datetime import datetime

from lhp.core.state_manager import StateManager
from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestDependencyTrackingIntegration:
    """Integration tests for dependency tracking workflow."""
    
    def test_state_manager_dependency_integration(self):
        """Test StateManager integration with dependency tracking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create YAML file that uses the preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: load_data
    type: load
    source: "SELECT * FROM source_table"
    target: processed_data
""")
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("catalog: dev_catalog\nschema: dev_schema")
            
            # Initialize state manager and track a file
            state_manager = StateManager(project_root)
            state_manager.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Verify file was tracked with dependencies
            tracked_files = state_manager.get_generated_files("dev")
            assert len(tracked_files) == 1
            
            file_state = list(tracked_files.values())[0]
            assert file_state.file_dependencies is not None
            assert "presets/bronze_layer.yaml" in file_state.file_dependencies
            
            # Verify global dependencies
            assert state_manager._state.global_dependencies is not None
            assert "dev" in state_manager._state.global_dependencies
            
            # Test staleness detection
            stale_files = state_manager.find_stale_files("dev")
            assert len(stale_files) == 0  # Should be up-to-date
            
            # Change preset and verify staleness
            preset_file.write_text("name: bronze_layer\nversion: '2.0'")
            stale_files = state_manager.find_stale_files("dev")
            assert len(stale_files) == 1
            
            # Verify detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            assert len(staleness_info["files"]) == 1
            file_info = list(staleness_info["files"].values())[0]
            assert "File dependencies changed" in file_info["reasons"]
    
    def test_dependency_resolver_integration(self):
        """Test StateDependencyResolver integration with complex dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset files with inheritance
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            base_preset = preset_dir / "base_layer.yaml"
            base_preset.write_text("name: base_layer\nversion: '1.0'")
            
            bronze_preset = preset_dir / "bronze_layer.yaml"
            bronze_preset.write_text("name: bronze_layer\nversion: '1.0'\nextends: base_layer")
            
            # Create template file
            template_dir = project_root / "templates"
            template_dir.mkdir()
            template_file = template_dir / "ingestion_template.yaml"
            template_file.write_text("""
name: ingestion_template
version: "1.0"
parameters:
  - name: table_name
    type: string
    required: true
actions:
  - name: load_{table_name}
    type: load
    source: "SELECT * FROM {table_name}"
    target: raw_{table_name}
""")
            
            # Create YAML file with multiple dependency types
            yaml_file = project_root / "complex.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: complex_flowgroup
presets:
  - bronze_layer
use_template: ingestion_template
template_parameters:
  table_name: orders
actions:
  - name: additional_transform
    type: transform
    source: raw_orders
    target: processed_orders
""")
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("catalog: dev_catalog\nschema: dev_schema")
            
            # Create project config
            project_config = project_root / "lhp.yaml"
            project_config.write_text("name: test_project\nversion: '1.0'")
            
            # Initialize dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test file dependency resolution
            file_deps = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert "presets/bronze_layer.yaml" in file_deps
            assert "presets/base_layer.yaml" in file_deps  # Transitive dependency
            assert "templates/ingestion_template.yaml" in file_deps
            
            # Test global dependency resolution
            global_deps = resolver.resolve_global_dependencies("dev")
            assert "substitutions/dev.yaml" in global_deps
            assert "lhp.yaml" in global_deps
            
            # Test composite checksum calculation
            all_deps = [str(yaml_file.relative_to(project_root))] + list(file_deps.keys())
            composite_checksum = resolver.calculate_composite_checksum(all_deps)
            assert composite_checksum
            
            # Modify a dependency and verify checksum changes
            bronze_preset.write_text("name: bronze_layer\nversion: '2.0'\nextends: base_layer")
            new_composite_checksum = resolver.calculate_composite_checksum(all_deps)
            assert composite_checksum != new_composite_checksum
    
    def test_multi_environment_integration(self):
        """Test dependency tracking across multiple environments."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source: "SELECT * FROM {catalog}.{schema}.source_table"
    target: data
""")
            
            # Create substitution files for different environments
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            
            dev_substitution = substitution_dir / "dev.yaml"
            dev_substitution.write_text("catalog: dev_catalog\nschema: dev_schema")
            
            prod_substitution = substitution_dir / "prod.yaml"
            prod_substitution.write_text("catalog: prod_catalog\nschema: prod_schema")
            
            # Initialize state manager and track files in both environments
            state_manager = StateManager(project_root)
            
            # Track file in dev environment
            state_manager.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("dev/test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Track file in prod environment
            state_manager.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("prod/test_flowgroup.py"),
                environment="prod",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Verify both environments are tracked
            dev_files = state_manager.get_generated_files("dev")
            prod_files = state_manager.get_generated_files("prod")
            assert len(dev_files) == 1
            assert len(prod_files) == 1
            
            # Verify global dependencies are environment-specific
            dev_global = state_manager._state.global_dependencies["dev"]
            prod_global = state_manager._state.global_dependencies["prod"]
            assert dev_global.substitution_file.path == "substitutions/dev.yaml"
            assert prod_global.substitution_file.path == "substitutions/prod.yaml"
            
            # Change dev substitution and verify only dev is affected
            dev_substitution.write_text("catalog: updated_dev_catalog\nschema: updated_dev_schema")
            
            dev_stale = state_manager.find_stale_files("dev")
            prod_stale = state_manager.find_stale_files("prod")
            assert len(dev_stale) == 1
            assert len(prod_stale) == 0
            
            # Verify detailed staleness info
            dev_staleness = state_manager.get_detailed_staleness_info("dev")
            prod_staleness = state_manager.get_detailed_staleness_info("prod")
            
            assert len(dev_staleness["global_changes"]) > 0
            assert len(prod_staleness["global_changes"]) == 0
    
    def test_save_and_load_state_with_dependencies(self):
        """Test saving and loading state with dependency information."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: load_data
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("catalog: dev_catalog\nschema: dev_schema")
            
            # Initialize state manager and track file
            state_manager = StateManager(project_root)
            state_manager.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Save state
            state_manager._save_state()
            
            # Create new state manager and load state
            new_state_manager = StateManager(project_root)
            new_state_manager._load_state()
            
            # Verify state was loaded correctly
            loaded_files = new_state_manager.get_generated_files("dev")
            assert len(loaded_files) == 1
            
            file_state = list(loaded_files.values())[0]
            assert file_state.file_dependencies is not None
            assert "presets/bronze_layer.yaml" in file_state.file_dependencies
            
            # Verify global dependencies were loaded
            assert new_state_manager._state.global_dependencies is not None
            assert "dev" in new_state_manager._state.global_dependencies
            
            # Test staleness detection after loading
            stale_files = new_state_manager.find_stale_files("dev")
            assert len(stale_files) == 0  # Should be up-to-date
            
            # Change preset and verify staleness detection still works
            preset_file.write_text("name: bronze_layer\nversion: '2.0'")
            stale_files = new_state_manager.find_stale_files("dev")
            assert len(stale_files) == 1
    
    def test_orphaned_files_with_dependencies(self):
        """Test orphaned file detection with dependency changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: old_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Initialize state manager and track file
            state_manager = StateManager(project_root)
            state_manager.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("old_pipeline/test_flowgroup.py"),
                environment="dev",
                pipeline="old_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Change pipeline name in YAML
            yaml_file.write_text("""
pipeline: new_pipeline
flowgroup: test_flowgroup
actions:
  - name: load_data
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Find orphaned files
            orphaned_files = state_manager.find_orphaned_files("dev")
            assert len(orphaned_files) == 1
            assert orphaned_files[0].generated_path == "old_pipeline/test_flowgroup.py" 
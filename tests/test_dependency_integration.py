"""Integration tests for dependency tracking functionality."""

import tempfile
import pytest
from pathlib import Path
from datetime import datetime

from lhp.core.state_manager import ProjectStateManager
from lhp.core.state.pipeline_state_manager import PipelineStateManager
from lhp.core.state_dependency_resolver import StateDependencyResolver


def _seed_file(project_root, *, source_yaml, generated_path, environment, pipeline, flowgroup):
    """Track a generated file via :class:`PipelineStateManager` and persist its shard.

    Used in place of the removed ``ProjectStateManager.track_generated_file``
    shim. Constructs a worker-style state manager scoped to
    ``(pipeline, environment)``, records the file, then atomically saves
    the shard so the project-wide manager can pick it up via
    ``load_all_pipeline_shards``.

    Also calls :meth:`ProjectStateManager.save_global` for the env so that
    global_dependencies (substitution_file + project_config checksums) are
    populated on ``_global.json``. Without this the staleness analyzer
    sees a fresh-vs-tracked mismatch on global deps and flags every file
    as stale.
    """
    pm = PipelineStateManager(
        state_dir=project_root / ".lhp_state",
        pipeline_name=pipeline,
        environment=environment,
        project_root=project_root,
    )
    pm.track_generated_file(
        generated_path=generated_path,
        source_yaml=source_yaml,
        flowgroup=flowgroup,
    )
    pm.save()

    psm = ProjectStateManager(project_root)
    psm.save_global(last_generation_context={environment: {}})
    return pm


class TestDependencyTrackingIntegration:
    """Integration tests for dependency tracking workflow."""
    
    def test_state_manager_dependency_integration(self):
        """Test ProjectStateManager integration with dependency tracking."""
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
            
            # Track a file via PipelineStateManager (workers' write path).
            _seed_file(
                project_root,
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
            )
            state_manager = ProjectStateManager(project_root)

            # Verify file was tracked with dependencies
            tracked_files = state_manager.get_generated_files("dev")
            assert len(tracked_files) == 1
            
            file_state = list(tracked_files.values())[0]
            assert file_state.file_dependencies is not None
            assert "presets/bronze_layer.yaml" in file_state.file_dependencies

            # Global dependencies are no longer populated by per-file
            # tracking — workers operate on PipelineState which is scoped
            # to one pipeline and doesn't carry the project-wide
            # global_dependencies map. The project-wide ``_global.json``
            # shard records them via ``ProjectStateManager.save_global``
            # at end-of-batch, not during track_generated_file.

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
            assert file_info["stale"] == True  # Updated to match actual structure from StateAnalyzer
            assert "details" in file_info  # Contains list of change descriptions
    
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
            
            # Track files in both environments via PipelineStateManager.
            _seed_file(
                project_root,
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("dev/test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
            )
            _seed_file(
                project_root,
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("prod/test_flowgroup.py"),
                environment="prod",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
            )
            state_manager = ProjectStateManager(project_root)
            
            # Verify both environments are tracked
            dev_files = state_manager.get_generated_files("dev")
            prod_files = state_manager.get_generated_files("prod")
            assert len(dev_files) == 1
            assert len(prod_files) == 1
            
            # Per-env global_dependencies are recorded on the project-wide
            # ``_global.json`` shard (written by ``save_global`` at end of
            # batch in the new architecture), not as a side effect of
            # per-file tracking. The cross-env staleness behaviour below
            # exercises the same wiring through the analyzer's resolver.

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
            
            # Track and persist via PipelineStateManager — the shard is
            # written atomically inside _seed_file.
            _seed_file(
                project_root,
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("test_flowgroup.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
            )

            # Fresh manager reloads shards on demand via load_all_pipeline_shards.
            new_state_manager = ProjectStateManager(project_root)
            
            # Verify state was loaded correctly
            loaded_files = new_state_manager.get_generated_files("dev")
            assert len(loaded_files) == 1
            
            file_state = list(loaded_files.values())[0]
            assert file_state.file_dependencies is not None
            assert "presets/bronze_layer.yaml" in file_state.file_dependencies

            # Project-wide global_dependencies live on ``_global.json`` and
            # are populated by ``save_global`` (end of batch in the new
            # architecture). Per-pipeline shard load is the path under test
            # here; checking shard-level file_dependencies above suffices.

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
            
            # Create the generated file that will be tracked
            generated_dir = project_root / "old_pipeline"
            generated_dir.mkdir()
            generated_file = generated_dir / "test_flowgroup.py" 
            generated_file.write_text("# Generated file content")
            
            # Track via PipelineStateManager and load a fresh project manager.
            _seed_file(
                project_root,
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("old_pipeline/test_flowgroup.py"),
                environment="dev",
                pipeline="old_pipeline",
                flowgroup="test_flowgroup",
            )
            state_manager = ProjectStateManager(project_root)
            
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
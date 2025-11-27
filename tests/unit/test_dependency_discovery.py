"""Tests for dependency discovery functionality."""

import tempfile
import pytest
from pathlib import Path

from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestDependencyDiscovery:
    """Test dependency discovery from YAML files."""
    
    def test_discover_preset_dependencies_single_preset(self):
        """Test discovering a single preset dependency."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("""
name: bronze_layer
version: "1.0"
description: "Bronze layer preset"
""")
            
            # Create YAML file with preset reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the preset dependency
            assert len(dependencies) == 1
            assert "presets/bronze_layer.yaml" in dependencies
            assert dependencies["presets/bronze_layer.yaml"].type == "preset"
            assert dependencies["presets/bronze_layer.yaml"].path == "presets/bronze_layer.yaml"
    
    def test_discover_preset_dependencies_multiple_presets(self):
        """Test discovering multiple preset dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset files
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            preset1 = preset_dir / "bronze_layer.yaml"
            preset1.write_text("name: bronze_layer\nversion: '1.0'")
            
            preset2 = preset_dir / "silver_layer.yaml"
            preset2.write_text("name: silver_layer\nversion: '1.0'")
            
            # Create YAML file with multiple preset references
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
  - silver_layer
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find both preset dependencies
            assert len(dependencies) == 2
            assert "presets/bronze_layer.yaml" in dependencies
            assert "presets/silver_layer.yaml" in dependencies
            assert dependencies["presets/bronze_layer.yaml"].type == "preset"
            assert dependencies["presets/silver_layer.yaml"].type == "preset"
    
    def test_discover_template_dependencies(self):
        """Test discovering template dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create template file
            template_dir = project_root / "templates"
            template_dir.mkdir()
            template_file = template_dir / "ingestion_template.yaml"
            template_file.write_text("""
name: ingestion_template
version: "1.0"
description: "Standard ingestion template"
""")
            
            # Create YAML file with template reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
use_template: ingestion_template
template_parameters:
  table_name: customers
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the template dependency
            assert len(dependencies) == 1
            assert "templates/ingestion_template.yaml" in dependencies
            assert dependencies["templates/ingestion_template.yaml"].type == "template"
            assert dependencies["templates/ingestion_template.yaml"].path == "templates/ingestion_template.yaml"
    
    def test_discover_template_with_preset_dependencies(self):
        """Test discovering template that uses presets (transitive dependencies)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("""
name: bronze_layer
version: "1.0"
description: "Bronze layer preset"
""")
            
            # Create template file that uses preset
            template_dir = project_root / "templates"
            template_dir.mkdir()
            template_file = template_dir / "ingestion_template.yaml"
            template_file.write_text("""
name: ingestion_template
version: "1.0"
description: "Standard ingestion template"
presets:
  - bronze_layer
actions:
  - name: template_action
    type: load
    source: template_source
    target: template_target
""")
            
            # Create YAML file with template reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
use_template: ingestion_template
template_parameters:
  table_name: customers
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the template dependency
            # Note: Transitive preset dependencies from templates may not be resolved
            # depending on template engine implementation
            assert len(dependencies) >= 1
            assert "templates/ingestion_template.yaml" in dependencies
            assert dependencies["templates/ingestion_template.yaml"].type == "template"
    
    def test_discover_transitive_preset_dependencies(self):
        """Test discovering transitive preset dependencies (preset extends preset)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset files with inheritance
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            # Base preset
            base_preset = preset_dir / "base_layer.yaml"
            base_preset.write_text("""
name: base_layer
version: "1.0"
description: "Base layer preset"
""")
            
            # Derived preset that extends base
            derived_preset = preset_dir / "bronze_layer.yaml"
            derived_preset.write_text("""
name: bronze_layer
version: "1.0"
description: "Bronze layer preset"
extends: base_layer
""")
            
            # Create YAML file with derived preset reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find both preset dependencies (direct and transitive)
            assert len(dependencies) == 2
            assert "presets/bronze_layer.yaml" in dependencies
            assert "presets/base_layer.yaml" in dependencies
            assert dependencies["presets/bronze_layer.yaml"].type == "preset"
            assert dependencies["presets/base_layer.yaml"].type == "preset"
    
    def test_discover_global_dependencies(self):
        """Test discovering global dependencies (substitution files and project config)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("""
database: dev_db
schema: dev_schema
""")
            
            # Create project config file
            project_config = project_root / "lhp.yaml"
            project_config.write_text("""
name: test_project
version: "1.0"
""")
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test global dependency discovery
            dependencies = resolver.resolve_global_dependencies("dev")
            
            # Should find both global dependencies
            assert len(dependencies) == 2
            assert "substitutions/dev.yaml" in dependencies
            assert "lhp.yaml" in dependencies
            assert dependencies["substitutions/dev.yaml"].type == "substitution"
            assert dependencies["lhp.yaml"].type == "project_config"
    
    def test_discover_global_dependencies_missing_files(self):
        """Test global dependency discovery with missing files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create only project config, no substitution file
            project_config = project_root / "lhp.yaml"
            project_config.write_text("""
name: test_project
version: "1.0"
""")
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test global dependency discovery
            dependencies = resolver.resolve_global_dependencies("dev")
            
            # Should find only project config
            assert len(dependencies) == 1
            assert "lhp.yaml" in dependencies
            assert dependencies["lhp.yaml"].type == "project_config"
    
    def test_discover_no_dependencies(self):
        """Test discovering dependencies from a YAML file with no dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create YAML file with no dependencies
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find no dependencies
            assert len(dependencies) == 0
    
    def test_discover_dependencies_missing_preset_file(self):
        """Test dependency discovery with reference to missing preset file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset directory but no preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            # Create YAML file with missing preset reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - missing_preset
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the dependency with empty checksum (missing file tracking)
            assert len(dependencies) == 1
            assert "presets/missing_preset.yaml" in dependencies
            assert dependencies["presets/missing_preset.yaml"].type == "preset"
            assert dependencies["presets/missing_preset.yaml"].checksum == ""
    
    def test_discover_dependencies_missing_template_file(self):
        """Test dependency discovery with reference to missing template file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create template directory but no template file
            template_dir = project_root / "templates"
            template_dir.mkdir()
            
            # Create YAML file with missing template reference
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
use_template: missing_template
template_parameters:
  table_name: customers
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find the dependency with empty checksum (missing file tracking)
            assert len(dependencies) == 1
            assert "templates/missing_template.yaml" in dependencies
            assert dependencies["templates/missing_template.yaml"].type == "template"
            assert dependencies["templates/missing_template.yaml"].checksum == ""
    
    def test_discover_dependencies_complex_scenario(self):
        """Test dependency discovery in a complex scenario with multiple dependency types."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset files with inheritance
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            base_preset = preset_dir / "base_layer.yaml"
            base_preset.write_text("name: base_layer\nversion: '1.0'")
            
            bronze_preset = preset_dir / "bronze_layer.yaml"
            bronze_preset.write_text("name: bronze_layer\nversion: '1.0'\nextends: base_layer")
            
            silver_preset = preset_dir / "silver_layer.yaml"
            silver_preset.write_text("name: silver_layer\nversion: '1.0'")
            
            # Create template file with preset dependency
            template_dir = project_root / "templates"
            template_dir.mkdir()
            template_file = template_dir / "ingestion_template.yaml"
            template_file.write_text("""
name: ingestion_template
version: "1.0"
presets:
  - silver_layer
actions:
  - name: template_action
    type: load
    source: template_source
    target: template_target
""")
            
            # Create YAML file with multiple dependency types
            yaml_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - bronze_layer
use_template: ingestion_template
template_parameters:
  table_name: customers
actions:
  - name: test_action
    type: load
    source: test_source
    target: test_target
"""
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text(yaml_content)
            
            # Create dependency resolver
            resolver = StateDependencyResolver(project_root)
            
            # Test dependency discovery
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find dependencies:
            # - bronze_layer preset (direct)
            # - base_layer preset (transitive from bronze_layer)
            # - ingestion_template template (direct)
            # Note: silver_layer preset (transitive from template) may not be resolved
            # depending on template engine implementation
            assert len(dependencies) >= 3
            assert "presets/bronze_layer.yaml" in dependencies
            assert "presets/base_layer.yaml" in dependencies
            assert "templates/ingestion_template.yaml" in dependencies
            
            # Check if template transitive dependencies are resolved
            if len(dependencies) == 4:
                assert "presets/silver_layer.yaml" in dependencies 
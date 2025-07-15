"""Tests for edge cases and error handling in dependency tracking."""

import tempfile
import pytest
import json
from pathlib import Path
from datetime import datetime

from lhp.core.state_manager import StateManager, FileState, DependencyInfo
from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestEdgeCasesAndErrorHandling:
    """Test edge cases and error handling in dependency tracking."""
    
    def test_malformed_yaml_file_handling(self):
        """Test handling of malformed YAML files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create malformed YAML file
            yaml_file = project_root / "malformed.yaml"
            yaml_file.write_text("invalid: yaml: content: [unclosed")
            
            # Should handle gracefully and return empty dependencies
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0
    
    def test_missing_yaml_file_handling(self):
        """Test handling of missing YAML files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Try to resolve dependencies for non-existent file
            yaml_file = project_root / "missing.yaml"
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0
    
    def test_circular_preset_dependencies(self):
        """Test handling of circular preset dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create preset files with circular dependency
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            preset_a = preset_dir / "preset_a.yaml"
            preset_a.write_text("name: preset_a\nversion: '1.0'\nextends: preset_b")
            
            preset_b = preset_dir / "preset_b.yaml"
            preset_b.write_text("name: preset_b\nversion: '1.0'\nextends: preset_a")
            
            # Create YAML file that uses one of the presets
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - preset_a
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle circular dependency gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should find at least the direct preset
            assert "presets/preset_a.yaml" in dependencies
            # Should not crash due to circular dependency
            assert len(dependencies) >= 1
    
    def test_invalid_preset_reference(self):
        """Test handling of invalid preset references."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create YAML file with invalid preset reference
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - nonexistent_preset
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should create entry for missing preset
            assert "presets/nonexistent_preset.yaml" in dependencies
            assert dependencies["presets/nonexistent_preset.yaml"].checksum == ""
    
    def test_invalid_template_reference(self):
        """Test handling of invalid template references."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create YAML file with invalid template reference
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
use_template: nonexistent_template
template_parameters:
  param1: value1
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should create entry for missing template
            assert "templates/nonexistent_template.yaml" in dependencies
            assert dependencies["templates/nonexistent_template.yaml"].checksum == ""
    
    def test_empty_yaml_file_handling(self):
        """Test handling of empty YAML files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create empty YAML file
            yaml_file = project_root / "empty.yaml"
            yaml_file.write_text("")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0
    
    def test_binary_file_as_yaml(self):
        """Test handling of binary files treated as YAML."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create binary file with .yaml extension
            yaml_file = project_root / "binary.yaml"
            yaml_file.write_bytes(b'\x00\x01\x02\x03\x04\x05')
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0
    
    def test_corrupted_state_file_handling(self):
        """Test handling of corrupted state files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create corrupted state file
            state_file = project_root / ".lhp_state.json"
            state_file.write_text("invalid json content")
            
            # Should handle gracefully and start with empty state
            state_manager._load_state()
            assert len(state_manager.get_generated_files("dev")) == 0
    
    def test_state_file_with_missing_fields(self):
        """Test handling of state files with missing fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create state file with missing required fields
            state_file = project_root / ".lhp_state.json"
            state_data = {
                "environments": {
                    "dev": {
                        "test.py": {
                            "source_yaml": "test.yaml",
                            "generated_path": "test.py",
                            "checksum": "abc123"
                            # Missing required fields
                        }
                    }
                }
            }
            state_file.write_text(json.dumps(state_data))
            
            # Should handle gracefully and migrate/fix the state
            state_manager._load_state()
            # Should not crash
            files = state_manager.get_generated_files("dev")
            # May have empty state or migrated state
            assert isinstance(files, dict)
    
    def test_permission_denied_file_access(self):
        """Test handling of permission denied when accessing files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Make file unreadable (this might not work on all systems)
            try:
                yaml_file.chmod(0o000)
                
                # Should handle gracefully
                dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
                assert len(dependencies) == 0
                
            finally:
                # Restore permissions for cleanup
                yaml_file.chmod(0o644)
    
    def test_very_large_yaml_file(self):
        """Test handling of very large YAML files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create very large YAML file
            yaml_file = project_root / "large.yaml"
            large_content = """
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
"""
            # Add many actions to make it large
            for i in range(1000):
                large_content += f"""
  - name: action_{i}
    type: load
    source: "SELECT * FROM table_{i}"
    target: data_{i}
"""
            
            yaml_file.write_text(large_content)
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0  # No preset/template dependencies
    
    def test_unicode_characters_in_files(self):
        """Test handling of Unicode characters in files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create preset with Unicode characters
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "unicode_preset.yaml"
            preset_file.write_text("name: unicode_preset\ndescription: 'Testing with émojis 🚀 and 中文'")
            
            # Create YAML file that uses the preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - unicode_preset
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert "presets/unicode_preset.yaml" in dependencies
    
    def test_extremely_long_file_paths(self):
        """Test handling of extremely long file paths."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create deeply nested directory structure
            deep_dir = project_root
            for i in range(10):
                deep_dir = deep_dir / f"very_long_directory_name_{i}"
                deep_dir.mkdir()
            
            # Create YAML file in deep directory
            yaml_file = deep_dir / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            assert len(dependencies) == 0
    
    def test_checksum_calculation_edge_cases(self):
        """Test edge cases in checksum calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Test with empty file
            empty_file = project_root / "empty.txt"
            empty_file.touch()
            
            # Test with non-existent file
            missing_file = project_root / "missing.txt"
            
            # Test composite checksum with mixed files
            file_paths = [
                str(empty_file.relative_to(project_root)),
                str(missing_file.relative_to(project_root))
            ]
            
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            assert composite_checksum  # Should not be empty
    
    def test_state_manager_edge_cases(self):
        """Test edge cases in state manager operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Test with empty environment
            stale_files = state_manager.find_stale_files("nonexistent_env")
            assert len(stale_files) == 0
            
            # Test with invalid pipeline
            generation_info = state_manager.get_files_needing_generation("dev", "nonexistent_pipeline")
            assert len(generation_info["stale"]) == 0
            assert len(generation_info["new"]) == 0
    
    def test_dependency_resolution_with_malformed_presets(self):
        """Test dependency resolution with malformed preset files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create malformed preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "malformed_preset.yaml"
            preset_file.write_text("invalid: yaml: [unclosed")
            
            # Create YAML file that uses the preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
presets:
  - malformed_preset
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Should still track the preset file even if malformed
            assert "presets/malformed_preset.yaml" in dependencies
    
    def test_concurrent_state_access(self):
        """Test potential concurrent access to state files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create multiple state managers
            state_manager1 = StateManager(project_root)
            state_manager2 = StateManager(project_root)
            
            # Create YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Track file with first state manager
            state_manager1.track_generated_file(
                source_yaml=yaml_file.relative_to(project_root),
                generated_path=Path("test.py"),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            # Load state with second state manager
            state_manager2._load_state()
            
            # Both should work without crashing
            files1 = state_manager1.get_generated_files("dev")
            files2 = state_manager2.get_generated_files("dev")
            
            assert len(files1) >= 0
            assert len(files2) >= 0
    
    def test_invalid_environment_names(self):
        """Test handling of invalid environment names."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Test with various invalid environment names
            invalid_envs = ["", "env with spaces", "env/with/slashes", "env\with\backslashes"]
            
            for env in invalid_envs:
                # Should handle gracefully
                stale_files = state_manager.find_stale_files(env)
                assert len(stale_files) == 0
    
    def test_file_system_edge_cases(self):
        """Test edge cases related to file system operations."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create file with special characters in name
            special_file = project_root / "file with spaces & symbols!.yaml"
            special_file.write_text("""
pipeline: test_pipeline
flowgroup: test_flowgroup
actions:
  - name: test_action
    type: load
    source: "SELECT * FROM table"
    target: data
""")
            
            # Should handle gracefully
            dependencies = resolver.resolve_file_dependencies(special_file, "dev")
            assert len(dependencies) == 0
    
    def test_memory_intensive_operations(self):
        """Test operations that might consume significant memory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Track many files
            for i in range(100):
                yaml_file = project_root / f"test_{i}.yaml"
                yaml_file.write_text(f"""
pipeline: test_pipeline
flowgroup: test_flowgroup_{i}
actions:
  - name: test_action_{i}
    type: load
    source: "SELECT * FROM table_{i}"
    target: data_{i}
""")
                
                state_manager.track_generated_file(
                    source_yaml=yaml_file.relative_to(project_root),
                    generated_path=Path(f"test_{i}.py"),
                    environment="dev",
                    pipeline="test_pipeline",
                    flowgroup=f"test_flowgroup_{i}"
                )
            
            # Should handle many files gracefully
            files = state_manager.get_generated_files("dev")
            assert len(files) == 100
            
            # Test staleness detection with many files
            stale_files = state_manager.find_stale_files("dev")
            assert len(stale_files) >= 0  # Should not crash 
"""Tests for composite checksum calculation functionality."""

import tempfile
import pytest
from pathlib import Path
from datetime import datetime

from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestCompositeChecksumCalculation:
    """Test composite checksum calculation functionality."""
    
    def test_calculate_composite_checksum_single_file(self):
        """Test calculating composite checksum for a single file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create a test file
            test_file = project_root / "test.yaml"
            test_file.write_text("test content")
            
            # Calculate composite checksum
            file_paths = [str(test_file.relative_to(project_root))]
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should return a non-empty string
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
    
    def test_calculate_composite_checksum_multiple_files(self):
        """Test calculating composite checksum for multiple files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test files
            test_file1 = project_root / "test1.yaml"
            test_file1.write_text("test content 1")
            
            test_file2 = project_root / "test2.yaml"
            test_file2.write_text("test content 2")
            
            # Calculate composite checksum
            file_paths = [
                str(test_file1.relative_to(project_root)),
                str(test_file2.relative_to(project_root))
            ]
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should return a non-empty string
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
    
    def test_calculate_composite_checksum_deterministic(self):
        """Test that composite checksum calculation is deterministic."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test files
            test_file1 = project_root / "test1.yaml"
            test_file1.write_text("test content 1")
            
            test_file2 = project_root / "test2.yaml"
            test_file2.write_text("test content 2")
            
            # Calculate composite checksum multiple times
            file_paths = [
                str(test_file1.relative_to(project_root)),
                str(test_file2.relative_to(project_root))
            ]
            
            checksum1 = resolver.calculate_composite_checksum(file_paths)
            checksum2 = resolver.calculate_composite_checksum(file_paths)
            
            # Should be the same
            assert checksum1 == checksum2
    
    def test_calculate_composite_checksum_order_independent(self):
        """Test that composite checksum calculation is order-independent (sorted internally)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test files
            test_file1 = project_root / "test1.yaml"
            test_file1.write_text("test content 1")
            
            test_file2 = project_root / "test2.yaml"
            test_file2.write_text("test content 2")
            
            # Calculate composite checksum in different orders
            file_paths_order1 = [
                str(test_file1.relative_to(project_root)),
                str(test_file2.relative_to(project_root))
            ]
            file_paths_order2 = [
                str(test_file2.relative_to(project_root)),
                str(test_file1.relative_to(project_root))
            ]
            
            checksum1 = resolver.calculate_composite_checksum(file_paths_order1)
            checksum2 = resolver.calculate_composite_checksum(file_paths_order2)
            
            # Should be the same (dependencies are sorted internally)
            assert checksum1 == checksum2
    
    def test_calculate_composite_checksum_content_sensitive(self):
        """Test that composite checksum changes when file content changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test file
            test_file = project_root / "test.yaml"
            test_file.write_text("original content")
            
            # Calculate initial checksum
            file_paths = [str(test_file.relative_to(project_root))]
            original_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Change file content
            test_file.write_text("modified content")
            
            # Calculate new checksum
            new_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should be different
            assert original_checksum != new_checksum
    
    def test_calculate_composite_checksum_missing_file(self):
        """Test calculating composite checksum with missing file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create one existing file and reference one missing file
            existing_file = project_root / "existing.yaml"
            existing_file.write_text("existing content")
            
            # Calculate composite checksum with missing file
            file_paths = [
                str(existing_file.relative_to(project_root)),
                "missing.yaml"
            ]
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should still return a checksum (with empty content for missing file)
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
    
    def test_calculate_composite_checksum_empty_list(self):
        """Test calculating composite checksum with empty file list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Calculate composite checksum with empty list
            composite_checksum = resolver.calculate_composite_checksum([])
            
            # Should return SHA256 of empty input (not empty string)
            import hashlib
            expected_checksum = hashlib.sha256().hexdigest()
            assert composite_checksum == expected_checksum
    
    def test_calculate_composite_checksum_with_dependencies(self):
        """Test calculating composite checksum in a real dependency scenario."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create YAML file that uses the preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test
flowgroup: test
presets:
  - bronze_layer
actions: []
""")
            
            # Resolve dependencies
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Extract dependency paths
            dependency_paths = [str(yaml_file.relative_to(project_root))] + list(dependencies.keys())
            
            # Calculate composite checksum
            composite_checksum = resolver.calculate_composite_checksum(dependency_paths)
            
            # Should return a valid checksum
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
            
            # Modify preset file and verify checksum changes
            preset_file.write_text("name: bronze_layer\nversion: '2.0'")
            new_checksum = resolver.calculate_composite_checksum(dependency_paths)
            
            # Should be different
            assert composite_checksum != new_checksum
    
    def test_calculate_composite_checksum_transitive_dependencies(self):
        """Test calculating composite checksum with transitive dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create preset files with inheritance
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            
            # Base preset
            base_preset = preset_dir / "base_layer.yaml"
            base_preset.write_text("name: base_layer\nversion: '1.0'")
            
            # Derived preset
            derived_preset = preset_dir / "bronze_layer.yaml"
            derived_preset.write_text("name: bronze_layer\nversion: '1.0'\nextends: base_layer")
            
            # Create YAML file that uses derived preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test
flowgroup: test
presets:
  - bronze_layer
actions: []
""")
            
            # Resolve dependencies 
            dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            
            # Verify that the direct preset is in dependencies
            assert "presets/bronze_layer.yaml" in dependencies
            
            # Check if transitive dependencies are resolved (may or may not be)
            has_transitive = "presets/base_layer.yaml" in dependencies
            
            # Extract dependency paths
            dependency_paths = [str(yaml_file.relative_to(project_root))] + list(dependencies.keys())
            
            # Calculate composite checksum
            composite_checksum = resolver.calculate_composite_checksum(dependency_paths)
            
            # Should return a valid checksum
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
            
            # Modify the direct preset and verify checksum changes
            derived_preset.write_text("name: bronze_layer\nversion: '2.0'\nextends: base_layer")
            
            # Re-resolve dependencies after change
            new_dependencies = resolver.resolve_file_dependencies(yaml_file, "dev")
            new_dependency_paths = [str(yaml_file.relative_to(project_root))] + list(new_dependencies.keys())
            
            # Calculate new checksum
            new_checksum = resolver.calculate_composite_checksum(new_dependency_paths)
            
            # Should be different (direct dependency changed)
            assert composite_checksum != new_checksum
    
    def test_calculate_composite_checksum_large_number_of_files(self):
        """Test calculating composite checksum with a large number of files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create many test files
            file_paths = []
            for i in range(100):
                test_file = project_root / f"test_{i}.yaml"
                test_file.write_text(f"test content {i}")
                file_paths.append(str(test_file.relative_to(project_root)))
            
            # Calculate composite checksum
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should return a valid checksum
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
    
    def test_calculate_composite_checksum_special_characters(self):
        """Test calculating composite checksum with files containing special characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test file with special characters
            test_file = project_root / "test.yaml"
            test_file.write_text("content with special chars: àáâãäåæçèéêë 中文 🚀")
            
            # Calculate composite checksum
            file_paths = [str(test_file.relative_to(project_root))]
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should return a valid checksum
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0
    
    def test_calculate_composite_checksum_binary_content(self):
        """Test calculating composite checksum with binary content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            resolver = StateDependencyResolver(project_root)
            
            # Create test file with binary content
            test_file = project_root / "test.bin"
            test_file.write_bytes(b'\x00\x01\x02\x03\x04\x05\xff\xfe\xfd')
            
            # Calculate composite checksum
            file_paths = [str(test_file.relative_to(project_root))]
            composite_checksum = resolver.calculate_composite_checksum(file_paths)
            
            # Should return a valid checksum
            assert composite_checksum
            assert isinstance(composite_checksum, str)
            assert len(composite_checksum) > 0 
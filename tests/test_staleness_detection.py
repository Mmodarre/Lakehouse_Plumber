"""Tests for enhanced staleness detection functionality."""

import tempfile
import pytest
from pathlib import Path
from datetime import datetime

from lhp.core.state_manager import StateManager, FileState, DependencyInfo, GlobalDependencies


class TestEnhancedStalenessDetection:
    """Test enhanced staleness detection functionality."""
    
    def test_find_stale_files_source_yaml_changed(self):
        """Test finding stale files when source YAML has changed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create a source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Create file state with old checksum
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum="old_checksum",
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find the file as stale (checksum changed)
            assert len(stale_files) == 1
            assert stale_files[0].generated_path == "test.py"
    
    def test_find_stale_files_source_yaml_unchanged(self):
        """Test finding stale files when source YAML is unchanged."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create a source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Calculate current checksum
            current_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state with current checksum
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find no stale files
            assert len(stale_files) == 0
    
    def test_find_stale_files_global_dependency_changed(self):
        """Test finding stale files when global dependencies have changed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("database: dev_db\nschema: dev_schema")
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Calculate current checksum
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state with current YAML checksum but old global dependencies
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Set old global dependencies in state
            old_sub_info = DependencyInfo(
                path="substitutions/dev.yaml",
                checksum="old_sub_checksum",
                type="substitution", 
                last_modified="2023-01-01T00:00:00"
            )
            
            state_manager._state.global_dependencies = {
                "dev": GlobalDependencies(
                    substitution_file=old_sub_info,
                    project_config=None
                )
            }
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find the file as stale (global dependency changed)
            assert len(stale_files) == 1
            assert stale_files[0].generated_path == "test.py"
    
    def test_find_stale_files_file_dependency_changed(self):
        """Test finding stale files when file-specific dependencies have changed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create source YAML file that uses the preset
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test
flowgroup: test
presets:
  - bronze_layer
actions: []
""")
            
            # Calculate current checksum
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state with current YAML checksum but old preset dependency
            old_preset_info = DependencyInfo(
                path="presets/bronze_layer.yaml",
                checksum="old_preset_checksum",
                type="preset",
                last_modified="2023-01-01T00:00:00"
            )
            
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                file_dependencies={"presets/bronze_layer.yaml": old_preset_info}
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find the file as stale (preset dependency changed)
            assert len(stale_files) == 1
            assert stale_files[0].generated_path == "test.py"
    
    def test_find_stale_files_no_changes(self):
        """Test finding stale files when nothing has changed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test
flowgroup: test
presets:
  - bronze_layer
actions: []
""")
            
            # Calculate current checksums
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            current_preset_checksum = state_manager._calculate_checksum(preset_file)
            
            # Create file state with current checksums
            current_preset_info = DependencyInfo(
                path="presets/bronze_layer.yaml",
                checksum=current_preset_checksum,
                type="preset",
                last_modified=datetime.now().isoformat()
            )
            
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                file_dependencies={"presets/bronze_layer.yaml": current_preset_info}
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find no stale files
            assert len(stale_files) == 0
    
    def test_get_detailed_staleness_info_global_changes(self):
        """Test getting detailed staleness information for global changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create substitution file
            substitution_dir = project_root / "substitutions"
            substitution_dir.mkdir()
            substitution_file = substitution_dir / "dev.yaml"
            substitution_file.write_text("database: dev_db\nschema: dev_schema")
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Calculate current checksum
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Set old global dependencies in state
            old_sub_info = DependencyInfo(
                path="substitutions/dev.yaml",
                checksum="old_sub_checksum",
                type="substitution",
                last_modified="2023-01-01T00:00:00"
            )
            
            state_manager._state.global_dependencies = {
                "dev": GlobalDependencies(
                    substitution_file=old_sub_info,
                    project_config=None
                )
            }
            
            # Get detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            
            # Should detect global changes
            assert len(staleness_info["global_changes"]) > 0
            assert "substitutions/dev.yaml" in staleness_info["global_changes"][0]
            assert "test.py" in staleness_info["files"]
            assert "Global dependencies changed" in staleness_info["files"]["test.py"]["reasons"]
    
    def test_get_detailed_staleness_info_file_changes(self):
        """Test getting detailed staleness information for file-specific changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '2.0'")  # Changed version
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("""
pipeline: test
flowgroup: test
presets:
  - bronze_layer
actions: []
""")
            
            # Calculate current YAML checksum
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state with old preset dependency
            old_preset_info = DependencyInfo(
                path="presets/bronze_layer.yaml",
                checksum="old_preset_checksum",
                type="preset",
                last_modified="2023-01-01T00:00:00"
            )
            
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                file_dependencies={"presets/bronze_layer.yaml": old_preset_info}
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Get detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            
            # Should detect file-specific changes
            assert len(staleness_info["global_changes"]) == 0
            assert "test.py" in staleness_info["files"]
            assert "File dependencies changed" in staleness_info["files"]["test.py"]["reasons"]
            assert any("presets/bronze_layer.yaml" in detail for detail in staleness_info["files"]["test.py"]["details"])
    
    def test_get_detailed_staleness_info_source_changes(self):
        """Test getting detailed staleness information for source YAML changes."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Create file state with old source checksum
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum="old_source_checksum",
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Get detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            
            # Should detect source changes
            assert len(staleness_info["global_changes"]) == 0
            assert "test.py" in staleness_info["files"]
            assert "Source YAML changed" in staleness_info["files"]["test.py"]["reasons"]
            assert any("test.yaml" in detail for detail in staleness_info["files"]["test.py"]["details"])
    
    def test_get_detailed_staleness_info_no_changes(self):
        """Test getting detailed staleness information when nothing has changed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create source YAML file
            yaml_file = project_root / "test.yaml"
            yaml_file.write_text("pipeline: test\nflowgroup: test\nactions: []")
            
            # Calculate current checksum
            current_yaml_checksum = state_manager._calculate_checksum(yaml_file)
            
            # Create file state with current checksum
            file_state = FileState(
                source_yaml="test.yaml",
                generated_path="test.py",
                checksum="gen_checksum",
                source_yaml_checksum=current_yaml_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test_flowgroup"
            )
            
            state_manager._state.environments["dev"] = {"test.py": file_state}
            
            # Get detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            
            # Should detect no changes
            assert len(staleness_info["global_changes"]) == 0
            assert len(staleness_info["files"]) == 0
    
    def test_staleness_detection_with_multiple_files(self):
        """Test staleness detection with multiple files having different statuses."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_manager = StateManager(project_root)
            
            # Create preset file
            preset_dir = project_root / "presets"
            preset_dir.mkdir()
            preset_file = preset_dir / "bronze_layer.yaml"
            preset_file.write_text("name: bronze_layer\nversion: '1.0'")
            
            # Create source YAML files
            yaml_file1 = project_root / "test1.yaml"
            yaml_file1.write_text("pipeline: test\nflowgroup: test1\nactions: []")
            
            yaml_file2 = project_root / "test2.yaml"
            yaml_file2.write_text("pipeline: test\nflowgroup: test2\npresets:\n  - bronze_layer\nactions: []")
            
            # Calculate current checksums
            current_yaml1_checksum = state_manager._calculate_checksum(yaml_file1)
            current_yaml2_checksum = state_manager._calculate_checksum(yaml_file2)
            current_preset_checksum = state_manager._calculate_checksum(preset_file)
            
            # Create file states
            # File 1: up-to-date
            file_state1 = FileState(
                source_yaml="test1.yaml",
                generated_path="test1.py",
                checksum="gen_checksum1",
                source_yaml_checksum=current_yaml1_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test1"
            )
            
            # File 2: stale due to preset change
            old_preset_info = DependencyInfo(
                path="presets/bronze_layer.yaml",
                checksum="old_preset_checksum",
                type="preset",
                last_modified="2023-01-01T00:00:00"
            )
            
            file_state2 = FileState(
                source_yaml="test2.yaml",
                generated_path="test2.py",
                checksum="gen_checksum2",
                source_yaml_checksum=current_yaml2_checksum,
                timestamp=datetime.now().isoformat(),
                environment="dev",
                pipeline="test_pipeline",
                flowgroup="test2",
                file_dependencies={"presets/bronze_layer.yaml": old_preset_info}
            )
            
            state_manager._state.environments["dev"] = {
                "test1.py": file_state1,
                "test2.py": file_state2
            }
            
            # Find stale files
            stale_files = state_manager.find_stale_files("dev")
            
            # Should find only file 2 as stale
            assert len(stale_files) == 1
            assert stale_files[0].generated_path == "test2.py"
            
            # Get detailed staleness info
            staleness_info = state_manager.get_detailed_staleness_info("dev")
            
            # Should show only file 2 as stale
            assert len(staleness_info["global_changes"]) == 0
            assert len(staleness_info["files"]) == 1
            assert "test2.py" in staleness_info["files"]
            assert "test1.py" not in staleness_info["files"] 
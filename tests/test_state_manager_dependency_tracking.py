"""Tests for StateManager enhanced dependency tracking functionality."""

import pytest
import tempfile
import json
import hashlib
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime
import logging

from lhp.core.state_manager import StateManager, FileState, ProjectState
from lhp.core.state_dependency_resolver import StateDependencyResolver


class TestDependencyTrackingDataStructures:
    """Test new data structures for dependency tracking."""

    def test_dependency_info_creation(self):
        """Test DependencyInfo dataclass creation."""
        from lhp.core.state_manager import DependencyInfo
        
        dep_info = DependencyInfo(
            path="presets/bronze_layer.yaml",
            checksum="abc123",
            type="preset",
            last_modified="2023-01-01T00:00:00"
        )
        
        assert dep_info.path == "presets/bronze_layer.yaml"
        assert dep_info.checksum == "abc123"
        assert dep_info.type == "preset"
        assert dep_info.last_modified == "2023-01-01T00:00:00"

    def test_global_dependencies_creation(self):
        """Test GlobalDependencies dataclass creation."""
        from lhp.core.state_manager import GlobalDependencies, DependencyInfo
        
        substitution_dep = DependencyInfo(
            path="substitutions/dev.yaml",
            checksum="sub123",
            type="substitution",
            last_modified="2023-01-01T00:00:00"
        )
        
        project_dep = DependencyInfo(
            path="lhp.yaml",
            checksum="proj123",
            type="project_config",
            last_modified="2023-01-01T00:00:00"
        )
        
        global_deps = GlobalDependencies(
            substitution_file=substitution_dep,
            project_config=project_dep
        )
        
        assert global_deps.substitution_file == substitution_dep
        assert global_deps.project_config == project_dep

    def test_enhanced_file_state_creation(self):
        """Test FileState with new dependency fields."""
        from lhp.core.state_manager import DependencyInfo
        
        preset_dep = DependencyInfo(
            path="presets/bronze_layer.yaml",
            checksum="preset123",
            type="preset",
            last_modified="2023-01-01T00:00:00"
        )
        
        template_dep = DependencyInfo(
            path="templates/standard_ingestion.yaml",
            checksum="template123",
            type="template",
            last_modified="2023-01-01T00:00:00"
        )
        
        file_state = FileState(
            source_yaml="pipelines/customer_bronze.yaml",
            generated_path="generated/customer_bronze.py",
            checksum="file123",
            source_yaml_checksum="yaml123",
            timestamp="2023-01-01T00:00:00",
            environment="dev",
            pipeline="bronze_load",
            flowgroup="customer_bronze",
            file_dependencies={
                "presets/bronze_layer.yaml": preset_dep,
                "templates/standard_ingestion.yaml": template_dep
            },
            file_composite_checksum="composite123"
        )
        
        assert file_state.file_dependencies is not None
        assert len(file_state.file_dependencies) == 2
        assert "presets/bronze_layer.yaml" in file_state.file_dependencies
        assert file_state.file_composite_checksum == "composite123"

    def test_enhanced_project_state_creation(self):
        """Test ProjectState with global dependencies."""
        from lhp.core.state_manager import GlobalDependencies, DependencyInfo
        
        global_deps = GlobalDependencies(
            substitution_file=DependencyInfo(
                path="substitutions/dev.yaml",
                checksum="sub123",
                type="substitution",
                last_modified="2023-01-01T00:00:00"
            )
        )
        
        project_state = ProjectState(
            version="1.0",
            global_dependencies={"dev": global_deps},
            environments={"dev": {}}
        )
        
        assert project_state.global_dependencies is not None
        assert "dev" in project_state.global_dependencies
        assert project_state.global_dependencies["dev"] == global_deps


class TestBasicStateManagementWithDependencies:
    """Test basic state management with enhanced dependency tracking."""

    def test_load_existing_state_with_new_dependency_fields(self):
        """Test loading state files with new dependency fields."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_file = project_root / ".lhp_state.json"
            
            # Create state file with new dependency fields
            state_data = {
                "version": "1.0",
                "last_updated": "2023-01-01T00:00:00",
                "global_dependencies": {
                    "dev": {
                        "substitution_file": {
                            "path": "substitutions/dev.yaml",
                            "checksum": "sub123",
                            "type": "substitution",
                            "last_modified": "2023-01-01T00:00:00"
                        },
                        "project_config": {
                            "path": "lhp.yaml",
                            "checksum": "proj123",
                            "type": "project_config",
                            "last_modified": "2023-01-01T00:00:00"
                        }
                    }
                },
                "environments": {
                    "dev": {
                        "generated/test.py": {
                            "source_yaml": "pipelines/test.yaml",
                            "generated_path": "generated/test.py",
                            "checksum": "abc123",
                            "source_yaml_checksum": "def456",
                            "timestamp": "2023-01-01T00:00:00",
                            "environment": "dev",
                            "pipeline": "test_pipeline",
                            "flowgroup": "test_flowgroup",
                            "file_dependencies": {
                                "presets/bronze_layer.yaml": {
                                    "path": "presets/bronze_layer.yaml",
                                    "checksum": "preset123",
                                    "type": "preset",
                                    "last_modified": "2023-01-01T00:00:00"
                                }
                            },
                            "file_composite_checksum": "composite123"
                        }
                    }
                }
            }
            
            with open(state_file, 'w') as f:
                json.dump(state_data, f)
            
            # Load state - should handle new dependency fields
            # NOTE: This will fail until we implement the enhanced StateManager
            # For now, we're just testing that the test structure is correct
            
            # state_manager = StateManager(project_root)
            # 
            # # Verify global dependencies loaded
            # assert state_manager._state.global_dependencies is not None
            # assert "dev" in state_manager._state.global_dependencies
            # 
            # dev_global_deps = state_manager._state.global_dependencies["dev"]
            # assert dev_global_deps.substitution_file.path == "substitutions/dev.yaml"
            # assert dev_global_deps.project_config.path == "lhp.yaml"
            # 
            # # Verify file dependencies loaded
            # dev_files = state_manager._state.environments["dev"]
            # file_state = dev_files["generated/test.py"]
            # assert file_state.file_dependencies is not None
            # assert "presets/bronze_layer.yaml" in file_state.file_dependencies
            # assert file_state.file_composite_checksum == "composite123"

    def test_save_state_with_dependency_tracking(self):
        """Test saving state with global and file dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # NOTE: This test will be implemented once we enhance StateManager
            # For now, we're defining the expected behavior
            
            # Create StateManager and add dependency data
            # state_manager = StateManager(project_root)
            # 
            # # Add global dependencies
            # from lhp.core.state_manager import GlobalDependencies, DependencyInfo
            # global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/dev.yaml",
            #         checksum="sub123",
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # state_manager._state.global_dependencies = {"dev": global_deps}
            # 
            # # Add file with dependencies
            # preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum="preset123",
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_state = FileState(
            #     source_yaml="pipelines/test.yaml",
            #     generated_path="generated/test.py",
            #     checksum="file123",
            #     source_yaml_checksum="yaml123",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="test_pipeline",
            #     flowgroup="test_flowgroup",
            #     file_dependencies={"presets/bronze_layer.yaml": preset_dep},
            #     file_composite_checksum="composite123"
            # )
            # 
            # state_manager._state.environments["dev"] = {"generated/test.py": file_state}
            # 
            # # Save state
            # state_manager.save()
            # 
            # # Verify state file contains dependency data
            # assert state_manager.state_file.exists()
            # 
            # with open(state_manager.state_file, 'r') as f:
            #     saved_data = json.load(f)
            # 
            # # Check global dependencies
            # assert "global_dependencies" in saved_data
            # assert "dev" in saved_data["global_dependencies"]
            # 
            # # Check file dependencies
            # file_data = saved_data["environments"]["dev"]["generated/test.py"]
            # assert "file_dependencies" in file_data
            # assert "file_composite_checksum" in file_data
            pass

    def test_backward_compatibility_with_old_state_files(self):
        """Test that old state files without dependencies still work."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_file = project_root / ".lhp_state.json"
            
            # Create old-format state file without dependency fields
            old_state_data = {
                "version": "1.0",
                "last_updated": "2023-01-01T00:00:00",
                "environments": {
                    "dev": {
                        "generated/test.py": {
                            "source_yaml": "pipelines/test.yaml",
                            "generated_path": "generated/test.py",
                            "checksum": "abc123",
                            "source_yaml_checksum": "def456",
                            "timestamp": "2023-01-01T00:00:00",
                            "environment": "dev",
                            "pipeline": "test_pipeline",
                            "flowgroup": "test_flowgroup"
                            # No file_dependencies or file_composite_checksum
                        }
                    }
                    # No global_dependencies
                }
            }
            
            with open(state_file, 'w') as f:
                json.dump(old_state_data, f)
            
            # Load state - should handle missing dependency fields gracefully
            # NOTE: This will be implemented with backward compatibility handling
            
            # state_manager = StateManager(project_root)
            # 
            # # Should load successfully with defaults for missing fields
            # assert state_manager._state.version == "1.0"
            # assert "dev" in state_manager._state.environments
            # 
            # # Global dependencies should be empty/None
            # assert state_manager._state.global_dependencies is None or len(state_manager._state.global_dependencies) == 0
            # 
            # # File should load with empty dependencies
            # file_state = state_manager._state.environments["dev"]["generated/test.py"]
            # assert file_state.file_dependencies is None or len(file_state.file_dependencies) == 0
            # assert file_state.file_composite_checksum == ""

    def test_migration_from_old_state_to_new_state(self):
        """Test automatic migration of state structure during save."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            state_file = project_root / ".lhp_state.json"
            
            # Create old-format state file
            old_state_data = {
                "version": "1.0",
                "last_updated": "2023-01-01T00:00:00",
                "environments": {
                    "dev": {
                        "generated/test.py": {
                            "source_yaml": "pipelines/test.yaml",
                            "generated_path": "generated/test.py",
                            "checksum": "abc123",
                            "source_yaml_checksum": "def456",
                            "timestamp": "2023-01-01T00:00:00",
                            "environment": "dev",
                            "pipeline": "test_pipeline",
                            "flowgroup": "test_flowgroup"
                        }
                    }
                }
            }
            
            with open(state_file, 'w') as f:
                json.dump(old_state_data, f)
            
            # Load and save - should migrate to new format
            # NOTE: This will be implemented with migration logic
            
            # state_manager = StateManager(project_root)
            # state_manager.save()
            # 
            # # Verify migrated state file includes new fields
            # with open(state_file, 'r') as f:
            #     migrated_data = json.load(f)
            # 
            # # Should have global_dependencies field (even if empty)
            # assert "global_dependencies" in migrated_data
            # 
            # # File should have new dependency fields
            # file_data = migrated_data["environments"]["dev"]["generated/test.py"]
            # assert "file_dependencies" in file_data
            # assert "file_composite_checksum" in file_data


class TestGlobalDependencyTracking:
    """Test global dependency tracking functionality."""

    def test_substitution_file_change_marks_all_environment_files_stale(self):
        """Test that when substitution file changes, all files in that environment are marked stale."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create substitution file
            substitution_file = project_root / "substitutions" / "dev.yaml"
            substitution_file.parent.mkdir(parents=True)
            substitution_file.write_text("catalog: dev_catalog\nschema: dev_schema")
            
            # Create tracked files
            state_manager = StateManager(project_root)
            
            # Track some files with current substitution checksum
            original_checksum = state_manager._calculate_checksum(substitution_file)
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # # Add global dependencies to state
            # from lhp.core.state_manager import GlobalDependencies, DependencyInfo
            # global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/dev.yaml",
            #         checksum=original_checksum,
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # state_manager._state.global_dependencies = {"dev": global_deps}
            # 
            # # Add some tracked files
            # file_state1 = FileState(
            #     source_yaml="pipelines/test1.yaml",
            #     generated_path="generated/test1.py",
            #     checksum="file1",
            #     source_yaml_checksum="yaml1",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="test_pipeline",
            #     flowgroup="test1"
            # )
            # 
            # file_state2 = FileState(
            #     source_yaml="pipelines/test2.yaml",
            #     generated_path="generated/test2.py",
            #     checksum="file2",
            #     source_yaml_checksum="yaml2",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="test_pipeline",
            #     flowgroup="test2"
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/test1.py": file_state1,
            #     "generated/test2.py": file_state2
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change substitution file
            # substitution_file.write_text("catalog: dev_catalog\nschema: updated_schema")
            # 
            # # Now ALL files in dev environment should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 2
            # 
            # # Verify both files are marked stale
            # stale_flowgroups = {f.flowgroup for f in stale_files}
            # assert "test1" in stale_flowgroups
            # assert "test2" in stale_flowgroups

    def test_project_config_change_marks_all_environments_stale(self):
        """Test that when project config changes, all environments are affected."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create project config file
            project_config_file = project_root / "lhp.yaml"
            project_config_file.write_text("name: test_project\nversion: 1.0")
            
            # Create substitution files for different environments
            dev_sub = project_root / "substitutions" / "dev.yaml"
            prod_sub = project_root / "substitutions" / "prod.yaml"
            dev_sub.parent.mkdir(parents=True)
            dev_sub.write_text("catalog: dev_catalog")
            prod_sub.write_text("catalog: prod_catalog")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track files in both environments
            # from lhp.core.state_manager import GlobalDependencies, DependencyInfo
            # 
            # # Add global dependencies for both environments
            # project_checksum = state_manager._calculate_checksum(project_config_file)
            # dev_sub_checksum = state_manager._calculate_checksum(dev_sub)
            # prod_sub_checksum = state_manager._calculate_checksum(prod_sub)
            # 
            # dev_global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/dev.yaml",
            #         checksum=dev_sub_checksum,
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     ),
            #     project_config=DependencyInfo(
            #         path="lhp.yaml",
            #         checksum=project_checksum,
            #         type="project_config",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # 
            # prod_global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/prod.yaml",
            #         checksum=prod_sub_checksum,
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     ),
            #     project_config=DependencyInfo(
            #         path="lhp.yaml",
            #         checksum=project_checksum,
            #         type="project_config",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # 
            # state_manager._state.global_dependencies = {
            #     "dev": dev_global_deps,
            #     "prod": prod_global_deps
            # }
            # 
            # # Add files to both environments
            # state_manager._state.environments["dev"] = {
            #     "generated/dev_file.py": FileState(
            #         source_yaml="pipelines/dev_file.yaml",
            #         generated_path="generated/dev_file.py",
            #         checksum="dev_file",
            #         source_yaml_checksum="dev_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="dev",
            #         pipeline="test_pipeline",
            #         flowgroup="dev_flowgroup"
            #     )
            # }
            # 
            # state_manager._state.environments["prod"] = {
            #     "generated/prod_file.py": FileState(
            #         source_yaml="pipelines/prod_file.yaml",
            #         generated_path="generated/prod_file.py",
            #         checksum="prod_file",
            #         source_yaml_checksum="prod_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="prod",
            #         pipeline="test_pipeline",
            #         flowgroup="prod_flowgroup"
            #     )
            # }
            # 
            # # Initially no files should be stale
            # dev_stale = state_manager.find_stale_files("dev")
            # prod_stale = state_manager.find_stale_files("prod")
            # assert len(dev_stale) == 0
            # assert len(prod_stale) == 0
            # 
            # # Change project config
            # project_config_file.write_text("name: test_project\nversion: 2.0")
            # 
            # # Now ALL files in ALL environments should be stale
            # dev_stale = state_manager.find_stale_files("dev")
            # prod_stale = state_manager.find_stale_files("prod")
            # assert len(dev_stale) == 1
            # assert len(prod_stale) == 1
            # 
            # # Verify correct files are stale
            # assert dev_stale[0].flowgroup == "dev_flowgroup"
            # assert prod_stale[0].flowgroup == "prod_flowgroup"

    def test_substitution_file_change_only_affects_specific_environment(self):
        """Test that substitution file changes only affect the specific environment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create substitution files for different environments
            dev_sub = project_root / "substitutions" / "dev.yaml"
            prod_sub = project_root / "substitutions" / "prod.yaml"
            dev_sub.parent.mkdir(parents=True)
            dev_sub.write_text("catalog: dev_catalog")
            prod_sub.write_text("catalog: prod_catalog")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track files in both environments
            # from lhp.core.state_manager import GlobalDependencies, DependencyInfo
            # 
            # dev_sub_checksum = state_manager._calculate_checksum(dev_sub)
            # prod_sub_checksum = state_manager._calculate_checksum(prod_sub)
            # 
            # dev_global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/dev.yaml",
            #         checksum=dev_sub_checksum,
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # 
            # prod_global_deps = GlobalDependencies(
            #     substitution_file=DependencyInfo(
            #         path="substitutions/prod.yaml",
            #         checksum=prod_sub_checksum,
            #         type="substitution",
            #         last_modified="2023-01-01T00:00:00"
            #     )
            # )
            # 
            # state_manager._state.global_dependencies = {
            #     "dev": dev_global_deps,
            #     "prod": prod_global_deps
            # }
            # 
            # # Add files to both environments
            # state_manager._state.environments["dev"] = {
            #     "generated/dev_file.py": FileState(
            #         source_yaml="pipelines/dev_file.yaml",
            #         generated_path="generated/dev_file.py",
            #         checksum="dev_file",
            #         source_yaml_checksum="dev_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="dev",
            #         pipeline="test_pipeline",
            #         flowgroup="dev_flowgroup"
            #     )
            # }
            # 
            # state_manager._state.environments["prod"] = {
            #     "generated/prod_file.py": FileState(
            #         source_yaml="pipelines/prod_file.yaml",
            #         generated_path="generated/prod_file.py",
            #         checksum="prod_file",
            #         source_yaml_checksum="prod_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="prod",
            #         pipeline="test_pipeline",
            #         flowgroup="prod_flowgroup"
            #     )
            # }
            # 
            # # Initially no files should be stale
            # dev_stale = state_manager.find_stale_files("dev")
            # prod_stale = state_manager.find_stale_files("prod")
            # assert len(dev_stale) == 0
            # assert len(prod_stale) == 0
            # 
            # # Change ONLY dev substitution file
            # dev_sub.write_text("catalog: dev_catalog\nschema: updated_dev_schema")
            # 
            # # Only dev files should be stale, prod should remain unaffected
            # dev_stale = state_manager.find_stale_files("dev")
            # prod_stale = state_manager.find_stale_files("prod")
            # assert len(dev_stale) == 1
            # assert len(prod_stale) == 0
            # 
            # # Verify correct file is stale
            # assert dev_stale[0].flowgroup == "dev_flowgroup"

    def test_missing_substitution_file_handling(self):
        """Test handling of missing substitution files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track a file but don't create substitution file
            # state_manager._state.environments["dev"] = {
            #     "generated/test.py": FileState(
            #         source_yaml="pipelines/test.yaml",
            #         generated_path="generated/test.py",
            #         checksum="test_file",
            #         source_yaml_checksum="test_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="dev",
            #         pipeline="test_pipeline",
            #         flowgroup="test_flowgroup"
            #     )
            # }
            # 
            # # Should handle missing substitution file gracefully
            # # (files might be stale or not, depending on implementation)
            # stale_files = state_manager.find_stale_files("dev")
            # 
            # # Should not crash and should handle gracefully
            # assert isinstance(stale_files, list)

    def test_missing_project_config_handling(self):
        """Test handling of missing project config."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create substitution file but no project config
            substitution_file = project_root / "substitutions" / "dev.yaml"
            substitution_file.parent.mkdir(parents=True)
            substitution_file.write_text("catalog: dev_catalog")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track a file
            # state_manager._state.environments["dev"] = {
            #     "generated/test.py": FileState(
            #         source_yaml="pipelines/test.yaml",
            #         generated_path="generated/test.py",
            #         checksum="test_file",
            #         source_yaml_checksum="test_yaml",
            #         timestamp="2023-01-01T00:00:00",
            #         environment="dev",
            #         pipeline="test_pipeline",
            #         flowgroup="test_flowgroup"
            #     )
            # }
            # 
            # # Should handle missing project config gracefully
            # stale_files = state_manager.find_stale_files("dev")
            # 
            # # Should not crash and should handle gracefully
            # assert isinstance(stale_files, list)

    def test_global_dependency_checksum_calculation(self):
        """Test global dependency checksum calculation."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create global dependency files
            substitution_file = project_root / "substitutions" / "dev.yaml"
            project_config_file = project_root / "lhp.yaml"
            
            substitution_file.parent.mkdir(parents=True)
            substitution_file.write_text("catalog: dev_catalog\nschema: dev_schema")
            project_config_file.write_text("name: test_project\nversion: 1.0")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Calculate checksums
            # sub_checksum = state_manager._calculate_checksum(substitution_file)
            # proj_checksum = state_manager._calculate_checksum(project_config_file)
            # 
            # # Both should be valid SHA256 checksums
            # assert sub_checksum != ""
            # assert len(sub_checksum) == 64
            # assert proj_checksum != ""
            # assert len(proj_checksum) == 64
            # 
            # # Should be deterministic
            # sub_checksum2 = state_manager._calculate_checksum(substitution_file)
            # proj_checksum2 = state_manager._calculate_checksum(project_config_file)
            # assert sub_checksum == sub_checksum2
            # assert proj_checksum == proj_checksum2
            # 
            # # Should change when file changes
            # substitution_file.write_text("catalog: dev_catalog\nschema: updated_schema")
            # new_sub_checksum = state_manager._calculate_checksum(substitution_file)
            # assert new_sub_checksum != sub_checksum


class TestFileSpecificDependencyTracking:
    """Test file-specific dependency tracking functionality."""

    def test_preset_change_marks_only_dependent_files_stale(self):
        """Test that preset changes only mark files that use the preset as stale."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset file
            preset_file = project_root / "presets" / "bronze_layer.yaml"
            preset_file.parent.mkdir(parents=True)
            preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            
            # Create another preset file (not used by test files)
            other_preset_file = project_root / "presets" / "silver_layer.yaml"
            other_preset_file.write_text("name: silver_layer\ndefaults:\n  quality: silver")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track files with different dependencies
            # from lhp.core.state_manager import DependencyInfo
            # 
            # preset_checksum = state_manager._calculate_checksum(preset_file)
            # other_preset_checksum = state_manager._calculate_checksum(other_preset_file)
            # 
            # # File that uses bronze_layer preset
            # bronze_preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=preset_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_bronze_preset = FileState(
            #     source_yaml="pipelines/customer_bronze.yaml",
            #     generated_path="generated/customer_bronze.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="bronze_load",
            #     flowgroup="customer_bronze",
            #     file_dependencies={"presets/bronze_layer.yaml": bronze_preset_dep}
            # )
            # 
            # # File that uses silver_layer preset
            # silver_preset_dep = DependencyInfo(
            #     path="presets/silver_layer.yaml",
            #     checksum=other_preset_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_silver_preset = FileState(
            #     source_yaml="pipelines/customer_silver.yaml",
            #     generated_path="generated/customer_silver.py",
            #     checksum="silver_file",
            #     source_yaml_checksum="silver_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="silver_load",
            #     flowgroup="customer_silver",
            #     file_dependencies={"presets/silver_layer.yaml": silver_preset_dep}
            # )
            # 
            # # File with no preset dependencies
            # file_no_preset = FileState(
            #     source_yaml="pipelines/custom_logic.yaml",
            #     generated_path="generated/custom_logic.py",
            #     checksum="custom_file",
            #     source_yaml_checksum="custom_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="custom_pipeline",
            #     flowgroup="custom_logic"
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_bronze.py": file_with_bronze_preset,
            #     "generated/customer_silver.py": file_with_silver_preset,
            #     "generated/custom_logic.py": file_no_preset
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change only the bronze_layer preset
            # preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze\n  new_setting: true")
            # 
            # # Only the file using bronze_layer preset should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"
            # 
            # # Verify other files are not stale
            # stale_flowgroups = {f.flowgroup for f in stale_files}
            # assert "customer_silver" not in stale_flowgroups
            # assert "custom_logic" not in stale_flowgroups

    def test_template_change_marks_only_dependent_files_stale(self):
        """Test that template changes only mark files that use the template as stale."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create template file
            template_file = project_root / "templates" / "standard_ingestion.yaml"
            template_file.parent.mkdir(parents=True)
            template_file.write_text("name: standard_ingestion\nactions:\n  - name: load_data\n    type: load")
            
            # Create another template file (not used by test files)
            other_template_file = project_root / "templates" / "cdc_ingestion.yaml"
            other_template_file.write_text("name: cdc_ingestion\nactions:\n  - name: load_cdc\n    type: load")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track files with different dependencies
            # from lhp.core.state_manager import DependencyInfo
            # 
            # template_checksum = state_manager._calculate_checksum(template_file)
            # other_template_checksum = state_manager._calculate_checksum(other_template_file)
            # 
            # # File that uses standard_ingestion template
            # standard_template_dep = DependencyInfo(
            #     path="templates/standard_ingestion.yaml",
            #     checksum=template_checksum,
            #     type="template",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_standard_template = FileState(
            #     source_yaml="pipelines/customer_ingestion.yaml",
            #     generated_path="generated/customer_ingestion.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="ingestion",
            #     flowgroup="customer_ingestion",
            #     file_dependencies={"templates/standard_ingestion.yaml": standard_template_dep}
            # )
            # 
            # # File that uses cdc_ingestion template
            # cdc_template_dep = DependencyInfo(
            #     path="templates/cdc_ingestion.yaml",
            #     checksum=other_template_checksum,
            #     type="template",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_cdc_template = FileState(
            #     source_yaml="pipelines/order_cdc.yaml",
            #     generated_path="generated/order_cdc.py",
            #     checksum="order_file",
            #     source_yaml_checksum="order_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="cdc_pipeline",
            #     flowgroup="order_cdc",
            #     file_dependencies={"templates/cdc_ingestion.yaml": cdc_template_dep}
            # )
            # 
            # # File with no template dependencies
            # file_no_template = FileState(
            #     source_yaml="pipelines/custom_logic.yaml",
            #     generated_path="generated/custom_logic.py",
            #     checksum="custom_file",
            #     source_yaml_checksum="custom_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="custom_pipeline",
            #     flowgroup="custom_logic"
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_ingestion.py": file_with_standard_template,
            #     "generated/order_cdc.py": file_with_cdc_template,
            #     "generated/custom_logic.py": file_no_template
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change only the standard_ingestion template
            # template_file.write_text("name: standard_ingestion\nactions:\n  - name: load_data\n    type: load\n  - name: validate\n    type: transform")
            # 
            # # Only the file using standard_ingestion template should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion"
            # 
            # # Verify other files are not stale
            # stale_flowgroups = {f.flowgroup for f in stale_files}
            # assert "order_cdc" not in stale_flowgroups
            # assert "custom_logic" not in stale_flowgroups

    def test_multiple_presets_in_single_file(self):
        """Test files with multiple preset dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create multiple preset files
            preset1_file = project_root / "presets" / "bronze_layer.yaml"
            preset2_file = project_root / "presets" / "data_quality.yaml"
            preset1_file.parent.mkdir(parents=True)
            preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            preset2_file.write_text("name: data_quality\ndefaults:\n  validation: strict")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file with multiple preset dependencies
            # from lhp.core.state_manager import DependencyInfo
            # 
            # preset1_checksum = state_manager._calculate_checksum(preset1_file)
            # preset2_checksum = state_manager._calculate_checksum(preset2_file)
            # 
            # bronze_preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=preset1_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # dq_preset_dep = DependencyInfo(
            #     path="presets/data_quality.yaml",
            #     checksum=preset2_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_multiple_presets = FileState(
            #     source_yaml="pipelines/customer_bronze.yaml",
            #     generated_path="generated/customer_bronze.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="bronze_load",
            #     flowgroup="customer_bronze",
            #     file_dependencies={
            #         "presets/bronze_layer.yaml": bronze_preset_dep,
            #         "presets/data_quality.yaml": dq_preset_dep
            #     }
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_bronze.py": file_with_multiple_presets
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change first preset
            # preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze\n  new_setting: true")
            # 
            # # File should be stale due to first preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"
            # 
            # # Reset to original and change second preset
            # preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            # preset2_file.write_text("name: data_quality\ndefaults:\n  validation: strict\n  new_rule: enabled")
            # 
            # # File should be stale due to second preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"

    def test_file_with_no_dependencies(self):
        """Test files with no preset or template dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create some preset/template files (but file won't use them)
            preset_file = project_root / "presets" / "bronze_layer.yaml"
            preset_file.parent.mkdir(parents=True)
            preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file with no dependencies
            # file_no_deps = FileState(
            #     source_yaml="pipelines/custom_logic.yaml",
            #     generated_path="generated/custom_logic.py",
            #     checksum="custom_file",
            #     source_yaml_checksum="custom_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="custom_pipeline",
            #     flowgroup="custom_logic"
            #     # No file_dependencies
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/custom_logic.py": file_no_deps
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change preset file (shouldn't affect our file)
            # preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze\n  new_setting: true")
            # 
            # # File should NOT be stale since it doesn't depend on the preset
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0

    def test_nonexistent_preset_reference(self):
        """Test handling of references to missing presets."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file with reference to nonexistent preset
            # from lhp.core.state_manager import DependencyInfo
            # 
            # nonexistent_preset_dep = DependencyInfo(
            #     path="presets/nonexistent.yaml",
            #     checksum="",  # Empty checksum for missing file
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_missing_preset = FileState(
            #     source_yaml="pipelines/customer_bronze.yaml",
            #     generated_path="generated/customer_bronze.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="bronze_load",
            #     flowgroup="customer_bronze",
            #     file_dependencies={"presets/nonexistent.yaml": nonexistent_preset_dep}
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_bronze.py": file_with_missing_preset
            # }
            # 
            # # Should handle missing preset gracefully
            # stale_files = state_manager.find_stale_files("dev")
            # 
            # # Should not crash and should handle gracefully
            # assert isinstance(stale_files, list)
            # # File might be considered stale or not, depending on implementation

    def test_nonexistent_template_reference(self):
        """Test handling of references to missing templates."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file with reference to nonexistent template
            # from lhp.core.state_manager import DependencyInfo
            # 
            # nonexistent_template_dep = DependencyInfo(
            #     path="templates/nonexistent.yaml",
            #     checksum="",  # Empty checksum for missing file
            #     type="template",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_missing_template = FileState(
            #     source_yaml="pipelines/customer_ingestion.yaml",
            #     generated_path="generated/customer_ingestion.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="ingestion",
            #     flowgroup="customer_ingestion",
            #     file_dependencies={"templates/nonexistent.yaml": nonexistent_template_dep}
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_ingestion.py": file_with_missing_template
            # }
            # 
            # # Should handle missing template gracefully
            # stale_files = state_manager.find_stale_files("dev")
            # 
            # # Should not crash and should handle gracefully
            # assert isinstance(stale_files, list)
            # # File might be considered stale or not, depending on implementation


class TestTransitiveDependencyTracking:
    """Test transitive dependency tracking functionality."""

    def test_preset_extends_preset_transitive_tracking(self):
        """Test that when preset A extends preset B, files using A depend on both A and B."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create base preset
            base_preset_file = project_root / "presets" / "base_layer.yaml"
            base_preset_file.parent.mkdir(parents=True)
            base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true\n  quality: base")
            
            # Create child preset that extends base
            child_preset_file = project_root / "presets" / "bronze_layer.yaml"
            child_preset_file.write_text("name: bronze_layer\nextends: base_layer\ndefaults:\n  quality: bronze")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file that uses child preset (should transitively depend on both)
            # from lhp.core.state_manager import DependencyInfo
            # 
            # base_checksum = state_manager._calculate_checksum(base_preset_file)
            # child_checksum = state_manager._calculate_checksum(child_preset_file)
            # 
            # # File using child preset should depend on both child and base
            # base_preset_dep = DependencyInfo(
            #     path="presets/base_layer.yaml",
            #     checksum=base_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # child_preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=child_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_child_preset = FileState(
            #     source_yaml="pipelines/customer_bronze.yaml",
            #     generated_path="generated/customer_bronze.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="bronze_load",
            #     flowgroup="customer_bronze",
            #     file_dependencies={
            #         "presets/bronze_layer.yaml": child_preset_dep,
            #         "presets/base_layer.yaml": base_preset_dep  # Transitive dependency
            #     }
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_bronze.py": file_with_child_preset
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change base preset - should make file stale due to transitive dependency
            # base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true\n  new_setting: true")
            # 
            # # File should be stale due to base preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"
            # 
            # # Reset base and change child preset - should also make file stale
            # base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true")
            # child_preset_file.write_text("name: bronze_layer\nextends: base_layer\ndefaults:\n  quality: bronze\n  bronze_setting: true")
            # 
            # # File should be stale due to child preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"

    def test_template_uses_preset_transitive_tracking(self):
        """Test that when template uses preset, files using template depend on both template and preset."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset
            preset_file = project_root / "presets" / "bronze_layer.yaml"
            preset_file.parent.mkdir(parents=True)
            preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            
            # Create template that uses preset
            template_file = project_root / "templates" / "bronze_ingestion.yaml"
            template_file.parent.mkdir(parents=True)
            template_file.write_text("name: bronze_ingestion\npresets:\n  - bronze_layer\nactions:\n  - name: load_data\n    type: load")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file that uses template (should transitively depend on both template and preset)
            # from lhp.core.state_manager import DependencyInfo
            # 
            # preset_checksum = state_manager._calculate_checksum(preset_file)
            # template_checksum = state_manager._calculate_checksum(template_file)
            # 
            # # File using template should depend on both template and preset
            # preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=preset_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # template_dep = DependencyInfo(
            #     path="templates/bronze_ingestion.yaml",
            #     checksum=template_checksum,
            #     type="template",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_template = FileState(
            #     source_yaml="pipelines/customer_ingestion.yaml",
            #     generated_path="generated/customer_ingestion.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="ingestion",
            #     flowgroup="customer_ingestion",
            #     file_dependencies={
            #         "templates/bronze_ingestion.yaml": template_dep,
            #         "presets/bronze_layer.yaml": preset_dep  # Transitive dependency
            #     }
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_ingestion.py": file_with_template
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change preset - should make file stale due to transitive dependency
            # preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze\n  new_setting: true")
            # 
            # # File should be stale due to preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion"
            # 
            # # Reset preset and change template - should also make file stale
            # preset_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            # template_file.write_text("name: bronze_ingestion\npresets:\n  - bronze_layer\nactions:\n  - name: load_data\n    type: load\n  - name: validate\n    type: transform")
            # 
            # # File should be stale due to template change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion"

    def test_complex_transitive_chain_preset_extends_preset_extends_preset(self):
        """Test complex transitive chain: A extends B extends C."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create base preset (C)
            base_preset_file = project_root / "presets" / "base_layer.yaml"
            base_preset_file.parent.mkdir(parents=True)
            base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true")
            
            # Create middle preset (B) that extends base
            middle_preset_file = project_root / "presets" / "ingestion_layer.yaml"
            middle_preset_file.write_text("name: ingestion_layer\nextends: base_layer\ndefaults:\n  schema_evolution: true")
            
            # Create child preset (A) that extends middle
            child_preset_file = project_root / "presets" / "bronze_layer.yaml"
            child_preset_file.write_text("name: bronze_layer\nextends: ingestion_layer\ndefaults:\n  quality: bronze")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file that uses child preset (should transitively depend on all three)
            # from lhp.core.state_manager import DependencyInfo
            # 
            # base_checksum = state_manager._calculate_checksum(base_preset_file)
            # middle_checksum = state_manager._calculate_checksum(middle_preset_file)
            # child_checksum = state_manager._calculate_checksum(child_preset_file)
            # 
            # # File using child preset should depend on all three presets
            # base_preset_dep = DependencyInfo(
            #     path="presets/base_layer.yaml",
            #     checksum=base_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # middle_preset_dep = DependencyInfo(
            #     path="presets/ingestion_layer.yaml",
            #     checksum=middle_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # child_preset_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=child_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_child_preset = FileState(
            #     source_yaml="pipelines/customer_bronze.yaml",
            #     generated_path="generated/customer_bronze.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="bronze_load",
            #     flowgroup="customer_bronze",
            #     file_dependencies={
            #         "presets/bronze_layer.yaml": child_preset_dep,
            #         "presets/ingestion_layer.yaml": middle_preset_dep,  # Transitive
            #         "presets/base_layer.yaml": base_preset_dep  # Transitive
            #     }
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_bronze.py": file_with_child_preset
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change base preset (C) - should make file stale
            # base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true\n  new_base_setting: true")
            # 
            # # File should be stale due to base preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"
            # 
            # # Reset base and change middle preset (B) - should also make file stale
            # base_preset_file.write_text("name: base_layer\ndefaults:\n  checkpoint: true")
            # middle_preset_file.write_text("name: ingestion_layer\nextends: base_layer\ndefaults:\n  schema_evolution: true\n  new_middle_setting: true")
            # 
            # # File should be stale due to middle preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"
            # 
            # # Reset middle and change child preset (A) - should also make file stale
            # middle_preset_file.write_text("name: ingestion_layer\nextends: base_layer\ndefaults:\n  schema_evolution: true")
            # child_preset_file.write_text("name: bronze_layer\nextends: ingestion_layer\ndefaults:\n  quality: bronze\n  new_child_setting: true")
            # 
            # # File should be stale due to child preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_bronze"

    def test_circular_preset_dependency_detection(self):
        """Test detection and handling of circular preset dependencies."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create preset A that extends B
            preset_a_file = project_root / "presets" / "preset_a.yaml"
            preset_a_file.parent.mkdir(parents=True)
            preset_a_file.write_text("name: preset_a\nextends: preset_b\ndefaults:\n  setting_a: true")
            
            # Create preset B that extends A (creates cycle)
            preset_b_file = project_root / "presets" / "preset_b.yaml"
            preset_b_file.write_text("name: preset_b\nextends: preset_a\ndefaults:\n  setting_b: true")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file that uses preset A (should detect circular dependency)
            # from lhp.core.state_manager import DependencyInfo
            # 
            # # Dependency resolution should detect circular reference and handle gracefully
            # # This might involve:
            # # 1. Throwing an error during dependency resolution
            # # 2. Breaking the cycle and logging a warning
            # # 3. Treating as stale always
            # 
            # # For now, we just ensure it doesn't crash
            # try:
            #     # This would be called during dependency resolution
            #     # dependencies = state_manager._resolve_transitive_dependencies("presets/preset_a.yaml")
            #     # Should either succeed with limited deps or raise informative error
            #     pass
            # except Exception as e:
            #     # Should be a clear error message about circular dependency
            #     assert "circular" in str(e).lower() or "cycle" in str(e).lower()

    def test_template_with_multiple_presets_transitive_tracking(self):
        """Test template that uses multiple presets and transitive dependency tracking."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create multiple presets
            preset1_file = project_root / "presets" / "bronze_layer.yaml"
            preset1_file.parent.mkdir(parents=True)
            preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            
            preset2_file = project_root / "presets" / "data_quality.yaml"
            preset2_file.write_text("name: data_quality\ndefaults:\n  validation: strict")
            
            # Create template that uses both presets
            template_file = project_root / "templates" / "bronze_ingestion.yaml"
            template_file.parent.mkdir(parents=True)
            template_file.write_text("""
name: bronze_ingestion
version: "1.0"
presets:
  - bronze_layer
  - data_quality
actions:
  - name: load_data
    type: load
""")
            
            # NOTE: This test will be implemented with enhanced StateManager
            # For now, we're defining the expected behavior
            
            # state_manager = StateManager(project_root)
            # 
            # # Track file that uses template (should transitively depend on template and both presets)
            # from lhp.core.state_manager import DependencyInfo
            # 
            # preset1_checksum = state_manager._calculate_checksum(preset1_file)
            # preset2_checksum = state_manager._calculate_checksum(preset2_file)
            # template_checksum = state_manager._calculate_checksum(template_file)
            # 
            # # File using template should depend on template and both presets
            # preset1_dep = DependencyInfo(
            #     path="presets/bronze_layer.yaml",
            #     checksum=preset1_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # preset2_dep = DependencyInfo(
            #     path="presets/data_quality.yaml",
            #     checksum=preset2_checksum,
            #     type="preset",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # template_dep = DependencyInfo(
            #     path="templates/bronze_ingestion.yaml",
            #     checksum=template_checksum,
            #     type="template",
            #     last_modified="2023-01-01T00:00:00"
            # )
            # 
            # file_with_template = FileState(
            #     source_yaml="pipelines/customer_ingestion.yaml",
            #     generated_path="generated/customer_ingestion.py",
            #     checksum="customer_file",
            #     source_yaml_checksum="customer_yaml",
            #     timestamp="2023-01-01T00:00:00",
            #     environment="dev",
            #     pipeline="ingestion",
            #     flowgroup="customer_ingestion",
            #     file_dependencies={
            #         "templates/bronze_ingestion.yaml": template_dep,
            #         "presets/bronze_layer.yaml": preset1_dep,  # Transitive
            #         "presets/data_quality.yaml": preset2_dep  # Transitive
            #     }
            # )
            # 
            # state_manager._state.environments["dev"] = {
            #     "generated/customer_ingestion.py": file_with_template
            # }
            # 
            # # Initially no files should be stale
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 0
            # 
            # # Change first preset - should make file stale
            # preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze\n  new_setting: true")
            # 
            # # File should be stale due to first preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion"
            # 
            # # Reset first preset and change second preset - should also make file stale
            # preset1_file.write_text("name: bronze_layer\ndefaults:\n  quality: bronze")
            # preset2_file.write_text("name: data_quality\ndefaults:\n  validation: strict\n  new_rule: enabled")
            # 
            # # File should be stale due to second preset change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion"
            # 
            # # Reset second preset and change template - should also make file stale
            # preset2_file.write_text("name: data_quality\ndefaults:\n  validation: strict")
            # template_file.write_text("name: bronze_ingestion\npresets:\n  - bronze_layer\n  - data_quality\nactions:\n  - name: load_data\n    type: load\n  - name: validate\n    type: transform")
            # 
            # # File should be stale due to template change
            # stale_files = state_manager.find_stale_files("dev")
            # assert len(stale_files) == 1
            # assert stale_files[0].flowgroup == "customer_ingestion" 
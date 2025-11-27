"""
Tests for BundleManager force regeneration functionality with pipeline config.

This test module verifies that the --force flag combined with -pc correctly
regenerates LHP-generated pipeline YAML files.
"""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from lhp.bundle.manager import BundleManager
from lhp.bundle.exceptions import BundleResourceError


@pytest.fixture
def mock_project_root(tmp_path):
    """Create a mock project root directory structure."""
    project_root = tmp_path / "test_project"
    project_root.mkdir()
    
    # Create resources/lhp directory
    resources_dir = project_root / "resources" / "lhp"
    resources_dir.mkdir(parents=True)
    
    # Create generated output directory
    output_dir = project_root / "generated" / "dev"
    output_dir.mkdir(parents=True)
    
    return project_root


@pytest.fixture
def mock_output_dir(mock_project_root):
    """Create mock output directory with pipeline directories."""
    output_dir = mock_project_root / "generated" / "dev"
    
    # Create pipeline directories
    (output_dir / "test_pipeline_1").mkdir()
    (output_dir / "test_pipeline_2").mkdir()
    
    return output_dir


@pytest.fixture
def bundle_manager(mock_project_root):
    """Create a BundleManager instance with mocked dependencies."""
    with patch('lhp.core.services.pipeline_config_loader.PipelineConfigLoader'):
        manager = BundleManager(mock_project_root)
        return manager


class TestForceRegenerationWithPipelineConfig:
    """Test force regeneration behavior when both --force and -pc are used."""
    
    def test_force_with_pipeline_config_regenerates_lhp_files(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that LHP files are regenerated when force=True and has_pipeline_config=True."""
        # Setup: Create an LHP-generated file
        resources_dir = mock_project_root / "resources" / "lhp"
        lhp_file = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file.write_text("# Managed by LHP\nresources:\n  pipelines:\n    old_config: true\n")
        
        # Mock the helper methods
        bundle_manager._is_lhp_generated_file = Mock(return_value=True)
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[lhp_file])
        
        # Execute with force=True and has_pipeline_config=True
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1", 
            mock_output_dir / "test_pipeline_1",
            "dev",
            force=True,
            has_pipeline_config=True
        )
        
        # Assert: File should be regenerated
        assert result is True
        bundle_manager._create_new_resource_file.assert_called_once()
    
    def test_force_only_preserves_lhp_files(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that LHP files are preserved when force=True but has_pipeline_config=False."""
        # Setup: Create an LHP-generated file
        resources_dir = mock_project_root / "resources" / "lhp"
        lhp_file = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file.write_text("# Managed by LHP\nresources:\n  pipelines:\n    old_config: true\n")
        
        # Mock the helper methods
        bundle_manager._is_lhp_generated_file = Mock(return_value=True)
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[lhp_file])
        
        # Execute with force=True but has_pipeline_config=False
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1",
            mock_output_dir / "test_pipeline_1",
            "dev",
            force=True,
            has_pipeline_config=False
        )
        
        # Assert: File should NOT be regenerated
        assert result is False
        bundle_manager._create_new_resource_file.assert_not_called()
    
    def test_no_force_preserves_lhp_files(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that LHP files are preserved when force=False (existing behavior)."""
        # Setup: Create an LHP-generated file
        resources_dir = mock_project_root / "resources" / "lhp"
        lhp_file = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file.write_text("# Managed by LHP\nresources:\n  pipelines:\n    old_config: true\n")
        
        # Mock the helper methods
        bundle_manager._is_lhp_generated_file = Mock(return_value=True)
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[lhp_file])
        
        # Execute with force=False
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1",
            mock_output_dir / "test_pipeline_1",
            "dev",
            force=False,
            has_pipeline_config=True
        )
        
        # Assert: File should NOT be regenerated
        assert result is False
        bundle_manager._create_new_resource_file.assert_not_called()
    
    def test_force_with_pipeline_config_backs_up_user_files(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that user files are still backed up and replaced (existing behavior maintained)."""
        # Setup: Create a user-created file (not LHP-generated)
        resources_dir = mock_project_root / "resources" / "lhp"
        user_file = resources_dir / "test_pipeline_1.pipeline.yml"
        user_file.write_text("resources:\n  pipelines:\n    user_config: true\n")
        
        # Mock the helper methods
        bundle_manager._is_lhp_generated_file = Mock(return_value=False)
        bundle_manager._backup_single_file = Mock()
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[user_file])
        
        # Execute with force=True and has_pipeline_config=True
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1",
            mock_output_dir / "test_pipeline_1",
            "dev",
            force=True,
            has_pipeline_config=True
        )
        
        # Assert: User file should be backed up and replaced
        assert result is True
        bundle_manager._backup_single_file.assert_called_once_with(user_file, "test_pipeline_1")
        bundle_manager._create_new_resource_file.assert_called_once()
    
    def test_sync_resources_passes_force_flags(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that sync_resources_with_generated_files passes force flags correctly."""
        # Mock all the helper methods
        bundle_manager._setup_sync_environment = Mock(return_value=(
            [mock_output_dir / "test_pipeline_1"],
            {"test_pipeline_1"},
            []
        ))
        bundle_manager._process_current_pipelines = Mock(return_value=1)
        bundle_manager._cleanup_orphaned_resources = Mock(return_value=0)
        bundle_manager._update_configuration_files = Mock()
        bundle_manager._log_sync_summary = Mock()
        
        # Execute
        bundle_manager.sync_resources_with_generated_files(
            mock_output_dir,
            "dev",
            force=True,
            has_pipeline_config=True
        )
        
        # Assert: _process_current_pipelines was called with correct flags
        bundle_manager._process_current_pipelines.assert_called_once_with(
            [mock_output_dir / "test_pipeline_1"],
            "dev",
            True,  # force
            True   # has_pipeline_config
        )


class TestNoBackupForLHPFiles:
    """Test that LHP files are overwritten directly without backup when force+pc is used."""
    
    def test_lhp_file_overwritten_directly_no_backup(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that LHP files are overwritten directly without calling backup method."""
        # Setup: Create an LHP-generated file
        resources_dir = mock_project_root / "resources" / "lhp"
        lhp_file = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file.write_text("# Managed by LHP\nresources:\n  pipelines:\n    old_config: true\n")
        
        # Mock the helper methods
        bundle_manager._is_lhp_generated_file = Mock(return_value=True)
        bundle_manager._backup_single_file = Mock()
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[lhp_file])
        
        # Execute with force=True and has_pipeline_config=True
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1",
            mock_output_dir / "test_pipeline_1",
            "dev",
            force=True,
            has_pipeline_config=True
        )
        
        # Assert: No backup should be created for LHP files
        assert result is True
        bundle_manager._backup_single_file.assert_not_called()
        bundle_manager._create_new_resource_file.assert_called_once()


class TestMultiplePipelinesForceRegeneration:
    """Test force regeneration behavior across multiple pipelines."""
    
    def test_multiple_pipelines_with_mixed_file_types(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test force regeneration with multiple pipelines having different file types."""
        resources_dir = mock_project_root / "resources" / "lhp"
        
        # Create multiple pipeline files
        lhp_file_1 = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file_1.write_text("# Managed by LHP\n")
        
        user_file_2 = resources_dir / "test_pipeline_2.pipeline.yml"
        user_file_2.write_text("# User created\n")
        
        # Mock methods
        def is_lhp_generated(file_path):
            return file_path.name == "test_pipeline_1.pipeline.yml"
        
        bundle_manager._is_lhp_generated_file = Mock(side_effect=is_lhp_generated)
        bundle_manager._backup_single_file = Mock()
        bundle_manager._create_new_resource_file = Mock()
        
        def find_files(pipeline_name):
            if pipeline_name == "test_pipeline_1":
                return [lhp_file_1]
            elif pipeline_name == "test_pipeline_2":
                return [user_file_2]
            return []
        
        bundle_manager._find_all_resource_files_for_pipeline = Mock(side_effect=find_files)
        
        # Process both pipelines with force=True and has_pipeline_config=True
        result_1 = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1", mock_output_dir / "test_pipeline_1", "dev",
            force=True, has_pipeline_config=True
        )
        result_2 = bundle_manager._sync_pipeline_resource(
            "test_pipeline_2", mock_output_dir / "test_pipeline_2", "dev",
            force=True, has_pipeline_config=True
        )
        
        # Assert: Both should be regenerated
        assert result_1 is True
        assert result_2 is True
        
        # LHP file: no backup, direct regeneration
        # User file: backup + regeneration
        assert bundle_manager._backup_single_file.call_count == 1
        assert bundle_manager._create_new_resource_file.call_count == 2


class TestBackwardCompatibility:
    """Test that changes maintain backward compatibility."""
    
    def test_sync_resources_without_force_flags_works(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that calling sync_resources without force flags still works (backward compatibility)."""
        # Mock all the helper methods
        bundle_manager._setup_sync_environment = Mock(return_value=([], set(), []))
        bundle_manager._process_current_pipelines = Mock(return_value=0)
        bundle_manager._cleanup_orphaned_resources = Mock(return_value=0)
        bundle_manager._update_configuration_files = Mock()
        bundle_manager._log_sync_summary = Mock()
        
        # Execute without force flags (using defaults)
        result = bundle_manager.sync_resources_with_generated_files(mock_output_dir, "dev")
        
        # Assert: Should work with default False values
        assert result == 0
        bundle_manager._process_current_pipelines.assert_called_once_with(
            [], "dev", False, False
        )
    
    def test_sync_pipeline_resource_without_force_flags_works(self, bundle_manager, mock_output_dir, mock_project_root):
        """Test that _sync_pipeline_resource without force flags maintains existing behavior."""
        resources_dir = mock_project_root / "resources" / "lhp"
        lhp_file = resources_dir / "test_pipeline_1.pipeline.yml"
        lhp_file.write_text("# Managed by LHP\n")
        
        bundle_manager._is_lhp_generated_file = Mock(return_value=True)
        bundle_manager._create_new_resource_file = Mock()
        bundle_manager._find_all_resource_files_for_pipeline = Mock(return_value=[lhp_file])
        
        # Execute without force flags (using defaults)
        result = bundle_manager._sync_pipeline_resource(
            "test_pipeline_1",
            mock_output_dir / "test_pipeline_1",
            "dev"
        )
        
        # Assert: LHP file should be preserved (conservative approach)
        assert result is False
        bundle_manager._create_new_resource_file.assert_not_called()


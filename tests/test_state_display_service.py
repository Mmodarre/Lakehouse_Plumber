"""Tests for StateDisplayService - business logic layer for state operations."""

import pytest
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from collections import defaultdict

from lhp.services.state_display_service import StateDisplayService
from lhp.core.state_manager import StateManager


class TestStateDisplayService:
    """Test the StateDisplayService business logic layer."""
    
    @pytest.fixture
    def mock_state_manager(self):
        """Create a mock StateManager for testing."""
        manager = Mock(spec=StateManager)
        manager._calculate_checksum = Mock(return_value="mock_checksum")
        return manager
    
    @pytest.fixture
    def service(self, mock_state_manager):
        """Create a StateDisplayService instance for testing."""
        project_root = Path("/mock/project")
        return StateDisplayService(mock_state_manager, project_root, verbose=False)
    
    def test_get_overall_stats_no_files(self, service, mock_state_manager):
        """Test get_overall_stats when no tracked files exist."""
        mock_state_manager.get_statistics.return_value = {"total_environments": 0}
        
        result = service.get_overall_stats()
        
        assert result is None
        mock_state_manager.get_statistics.assert_called_once()
    
    def test_get_overall_stats_with_files(self, service, mock_state_manager):
        """Test get_overall_stats with tracked files."""
        stats = {
            "total_environments": 2,
            "environments": {
                "dev": {"total_files": 5, "pipelines": {"bronze": 3}, "flowgroups": {"customers": 1}},
                "prod": {"total_files": 3, "pipelines": {"silver": 2}, "flowgroups": {"orders": 1}}
            }
        }
        mock_state_manager.get_statistics.return_value = stats
        
        result = service.get_overall_stats()
        
        assert result == stats
        mock_state_manager.get_statistics.assert_called_once()
    
    def test_get_tracked_files_none_found(self, service, mock_state_manager):
        """Test get_tracked_files when no files are tracked."""
        mock_state_manager.get_generated_files.return_value = {}
        
        result = service.get_tracked_files("dev")
        
        assert result is None
        mock_state_manager.get_generated_files.assert_called_once_with("dev")
    
    def test_get_tracked_files_with_pipeline_filter(self, service, mock_state_manager):
        """Test get_tracked_files with pipeline filtering."""
        # Mock file states
        file1 = Mock()
        file1.pipeline = "bronze"
        file2 = Mock()
        file2.pipeline = "silver"
        
        mock_state_manager.get_generated_files.return_value = {
            "file1.py": file1,
            "file2.py": file2
        }
        
        result = service.get_tracked_files("dev", pipeline="bronze")
        
        assert result == {"file1.py": file1}
        mock_state_manager.get_generated_files.assert_called_once_with("dev")
    
    def test_get_orphaned_files(self, service, mock_state_manager):
        """Test get_orphaned_files functionality."""
        orphaned1 = Mock()
        orphaned1.pipeline = "bronze"
        orphaned2 = Mock()
        orphaned2.pipeline = "silver"
        
        mock_state_manager.find_orphaned_files.return_value = [orphaned1, orphaned2]
        
        result = service.get_orphaned_files("dev", pipeline="bronze")
        
        assert result == [orphaned1]
        mock_state_manager.find_orphaned_files.assert_called_once_with("dev")
    
    def test_get_stale_files(self, service, mock_state_manager):
        """Test get_stale_files functionality."""
        stale1 = Mock()
        stale1.pipeline = "bronze"
        stale2 = Mock()
        stale2.pipeline = "silver"
        
        staleness_info = {"global_changes": [], "files": {}}
        
        mock_state_manager.find_stale_files.return_value = [stale1, stale2]
        mock_state_manager.get_detailed_staleness_info.return_value = staleness_info
        
        stale_files, info = service.get_stale_files("dev", pipeline="bronze")
        
        assert stale_files == [stale1]
        assert info == staleness_info
        mock_state_manager.find_stale_files.assert_called_once_with("dev")
        mock_state_manager.get_detailed_staleness_info.assert_called_once_with("dev")
    
    def test_get_new_files_with_pipeline_filter(self, service, mock_state_manager):
        """Test get_new_files with pipeline filtering."""
        project_root = Path("/mock/project")
        service.project_root = project_root
        
        # Mock new files
        new_file1 = project_root / "pipelines/bronze/file1.yaml"
        new_file2 = project_root / "pipelines/silver/file2.yaml"
        
        mock_state_manager.find_new_yaml_files.return_value = [new_file1, new_file2]
        
        result = service.get_new_files("dev", pipeline="bronze")
        
        assert result == [new_file1]
        mock_state_manager.find_new_yaml_files.assert_called_once_with("dev")
    
    def test_cleanup_orphaned_files_success(self, service, mock_state_manager):
        """Test successful cleanup of orphaned files."""
        deleted_files = ["file1.py", "file2.py"]
        mock_state_manager.cleanup_orphaned_files.return_value = deleted_files
        
        result = service.cleanup_orphaned_files("dev")
        
        assert result == deleted_files
        mock_state_manager.cleanup_orphaned_files.assert_called_once_with("dev", dry_run=False)
    
    def test_cleanup_orphaned_files_error(self, service, mock_state_manager):
        """Test cleanup error handling."""
        mock_state_manager.cleanup_orphaned_files.side_effect = Exception("Cleanup failed")
        
        with pytest.raises(Exception):
            service.cleanup_orphaned_files("dev")
    
    def test_regenerate_stale_files_dry_run(self, service, mock_state_manager):
        """Test regenerate_stale_files in dry run mode."""
        stale_files = [Mock(), Mock()]
        
        result = service.regenerate_stale_files("dev", stale_files, dry_run=True)
        
        assert result == 0
    
    @patch('lhp.core.orchestrator.ActionOrchestrator')
    def test_regenerate_stale_files_success(self, mock_orchestrator_class, service, mock_state_manager):
        """Test successful regeneration of stale files."""
        # Mock stale files
        stale1 = Mock()
        stale1.pipeline = "bronze"
        stale2 = Mock()
        stale2.pipeline = "bronze"
        stale3 = Mock()
        stale3.pipeline = "silver"
        
        stale_files = [stale1, stale2, stale3]
        
        # Mock orchestrator
        mock_orchestrator = Mock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.generate_pipeline.side_effect = [
            ["file1.py", "file2.py"],  # bronze pipeline - 2 files
            ["file3.py"]               # silver pipeline - 1 file
        ]
        
        result = service.regenerate_stale_files("dev", stale_files, dry_run=False)
        
        assert result == 3  # Total files regenerated
        assert mock_orchestrator.generate_pipeline.call_count == 2
    
    @patch('lhp.core.orchestrator.ActionOrchestrator')
    def test_regenerate_stale_files_error(self, mock_orchestrator_class, service, mock_state_manager):
        """Test regeneration error handling."""
        stale1 = Mock()
        stale1.pipeline = "bronze"
        
        mock_orchestrator = Mock()
        mock_orchestrator_class.return_value = mock_orchestrator
        mock_orchestrator.generate_pipeline.side_effect = Exception("Generation failed")
        
        with pytest.raises(Exception):
            service.regenerate_stale_files("dev", [stale1], dry_run=False)
    
    def test_group_files_by_pipeline(self, service):
        """Test grouping files by pipeline."""
        project_root = Path("/mock/project")
        service.project_root = project_root
        
        files = [
            project_root / "pipelines/bronze/file1.yaml",
            project_root / "pipelines/bronze/file2.yaml",
            project_root / "pipelines/silver/file3.yaml",
            Path("/external/file.yaml")  # External file
        ]
        
        result = service.group_files_by_pipeline(files)
        
        expected = {
            "bronze": [files[0], files[1]],
            "silver": [files[2]],
            "unknown": [files[3]]
        }
        
        assert result == expected
    
    def test_calculate_file_status_up_to_date(self, service, mock_state_manager):
        """Test calculate_file_status for up-to-date file."""
        project_root = Path("/mock/project")
        service.project_root = project_root
        
        # Mock file state
        file_state = Mock()
        file_state.source_yaml = "pipelines/bronze/file.yaml"
        file_state.generated_path = "generated/bronze/file.py"
        file_state.source_yaml_checksum = "existing_checksum"
        
        # Mock file existence and checksum
        with patch('pathlib.Path.exists') as mock_exists:
            mock_exists.return_value = True
            mock_state_manager._calculate_checksum.return_value = "existing_checksum"
            
            source_exists, generated_exists, change_status = service.calculate_file_status(file_state)
            
            assert source_exists is True
            assert generated_exists is True
            assert change_status == " 🟢 (up-to-date)"
    
    def test_calculate_file_status_stale(self, service, mock_state_manager):
        """Test calculate_file_status for stale file."""
        project_root = Path("/mock/project")
        service.project_root = project_root
        
        file_state = Mock()
        file_state.source_yaml = "pipelines/bronze/file.yaml"
        file_state.generated_path = "generated/bronze/file.py"
        file_state.source_yaml_checksum = "old_checksum"
        
        with patch('pathlib.Path.exists') as mock_exists:
            mock_exists.return_value = True
            mock_state_manager._calculate_checksum.return_value = "new_checksum"
            
            source_exists, generated_exists, change_status = service.calculate_file_status(file_state)
            
            assert source_exists is True
            assert generated_exists is True
            assert change_status == " 🟡 (stale)"
    
    def test_calculate_summary_counts(self, service, mock_state_manager):
        """Test calculate_summary_counts functionality."""
        # Mock tracked files
        tracked_files = {"file1.py": Mock(), "file2.py": Mock(), "file3.py": Mock()}
        
        # Mock orphaned files
        orphaned_files = [Mock(), Mock()]
        
        # Mock stale files
        stale_files = [Mock()]
        
        # Mock new files
        new_files = [Path("new1.yaml"), Path("new2.yaml")]
        
        # Configure mocks
        mock_state_manager.get_generated_files.return_value = tracked_files
        mock_state_manager.find_orphaned_files.return_value = orphaned_files
        mock_state_manager.find_stale_files.return_value = stale_files
        mock_state_manager.find_new_yaml_files.return_value = new_files
        mock_state_manager.get_detailed_staleness_info.return_value = {"global_changes": [], "files": {}}
        
        result = service.calculate_summary_counts("dev")
        
        expected = {
            "total_tracked": 3,
            "orphaned_count": 2,
            "stale_count": 1,
            "new_count": 2,
            "up_to_date_count": 2  # 3 total - 1 stale
        }
        
        assert result == expected


class TestStateDisplayServiceEdgeCases:
    """Test edge cases and error scenarios for StateDisplayService."""
    
    @pytest.fixture
    def service(self):
        """Create a StateDisplayService with mocked dependencies."""
        mock_state_manager = Mock(spec=StateManager)
        project_root = Path("/mock/project")
        return StateDisplayService(mock_state_manager, project_root, verbose=True)
    
    def test_get_new_files_path_error(self, service):
        """Test get_new_files with path resolution errors."""
        project_root = Path("/mock/project")
        service.project_root = project_root
        
        # Create a file path that can't be resolved relative to project root
        external_file = Path("/external/pipelines/bronze/file.yaml")
        
        service.state_manager.find_new_yaml_files.return_value = [external_file]
        
        result = service.get_new_files("dev", pipeline="bronze")
        
        # Should handle the error gracefully and return empty list
        assert result == []
    
    def test_calculate_file_status_missing_files(self, service):
        """Test calculate_file_status when files are missing."""
        file_state = Mock()
        file_state.source_yaml = "missing/file.yaml"
        file_state.generated_path = "missing/generated.py"
        file_state.source_yaml_checksum = "checksum"
        
        with patch('pathlib.Path.exists') as mock_exists:
            mock_exists.return_value = False
            
            source_exists, generated_exists, change_status = service.calculate_file_status(file_state)
            
            assert source_exists is False
            assert generated_exists is False
            assert change_status == ""
    
    def test_verbose_logging(self, service):
        """Test that verbose logging is properly configured."""
        assert service.verbose is True
        assert service.error_handler.verbose is True 
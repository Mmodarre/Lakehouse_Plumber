"""Integration tests for multi-job orchestration workflow."""

import pytest
from pathlib import Path
from lhp.core.services.dependency_analyzer import DependencyAnalyzer
from lhp.core.services.job_generator import JobGenerator
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.utils.error_formatter import LHPError
from unittest.mock import Mock, patch
import tempfile
import yaml


class TestMultiJobWorkflow:
    """Test complete multi-job orchestration workflow."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.mock_config_loader = Mock()
    
    def teardown_method(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    # NOTE: These integration tests were removed due to logic issues:
    # 1. test_complete_workflow_two_jobs - Uses mocked integration which bypasses real discovery
    # 2. test_cross_job_dependencies_in_master - Fake dependencies don't establish real references
    # 3. test_backward_compat_single_job_no_master - Tests YAML validity, not actual behavior
    # 
    # TODO: Rewrite these tests using real project structures and YAML files instead of mocks
    @patch('lhp.core.services.dependency_analyzer.DependencyAnalyzer._get_flowgroups')
    def test_validation_failure_stops_workflow(self, mock_get_flowgroups,
                                               sample_flowgroups_mixed_job_name):
        """Test that validation failure stops the workflow early."""
        mock_get_flowgroups.return_value = sample_flowgroups_mixed_job_name
        
        analyzer = DependencyAnalyzer(self.temp_dir, self.mock_config_loader)
        
        # Should fail at validation step
        with pytest.raises(LHPError) as exc_info:
            analyzer.analyze_dependencies_by_job()
        
        assert exc_info.value.code == "LHP-VAL-002"
        assert "Inconsistent job_name usage" in exc_info.value.title


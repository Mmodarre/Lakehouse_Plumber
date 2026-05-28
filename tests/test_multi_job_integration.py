"""Integration tests for multi-job orchestration workflow."""

import pytest
from pathlib import Path
from lhp.core.coordination.validation_service import ValidationService
from lhp.core.dependencies.service import DependencyAnalysisService
from lhp.core.jobs.job_generator import JobGenerator
from lhp.models import FlowGroup, Action, ActionType, ProjectConfig
from lhp.errors import LHPError
from unittest.mock import Mock, patch
import tempfile
import yaml


def _make_service(project_root):
    """Construct a DependencyAnalysisService with a real ValidationService.

    Replaces the legacy 2-arg ``(project_root, config_loader)`` form that
    pre-dated the v0.0.9 refactor; the service now takes an already-loaded
    ``ProjectConfig`` and a ``ValidationService``.
    """
    project_config = ProjectConfig(name="test", version="1.0")
    validation_service = ValidationService(project_root, project_config)
    return DependencyAnalysisService(project_root, project_config, validation_service)


class TestMultiJobWorkflow:
    """Test complete multi-job orchestration workflow."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

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
    @patch('lhp.core.dependencies.builder.DependencyGraphBuilder.get_flowgroups')
    def test_validation_failure_stops_workflow(self, mockget_flowgroups,
                                               sample_flowgroups_mixed_job_name):
        """Test that validation failure stops the workflow early."""
        mockget_flowgroups.return_value = sample_flowgroups_mixed_job_name

        analyzer = _make_service(self.temp_dir)

        # Should fail at validation step
        with pytest.raises(LHPError) as exc_info:
            analyzer.analyze_dependencies_by_job()

        assert exc_info.value.code == "LHP-VAL-002"
        assert "Inconsistent job_name usage" in exc_info.value.title


"""Integration tests for multi-job orchestration workflow."""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
import yaml

from lhp.core.coordination.validation_service import ValidationService
from lhp.core.dependencies.service import DependencyAnalysisService
from lhp.core.jobs.job_generator import JobGenerator
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, ProjectConfig


def _make_service(project_root):
    """Construct a DependencyAnalysisService with a real ValidationService."""
    project_config = ProjectConfig(name="test", version="1.0")
    validation_service = ValidationService(project_root, project_config)
    return DependencyAnalysisService(project_root, project_config, validation_service)


class TestMultiJobWorkflow:
    """Test complete multi-job orchestration workflow."""

    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @patch("lhp.core.dependencies.service.DependencyAnalysisService.get_flowgroups")
    def test_validation_failure_stops_workflow(
        self, mockget_flowgroups, sample_flowgroups_mixed_job_name
    ):
        mockget_flowgroups.return_value = sample_flowgroups_mixed_job_name

        analyzer = _make_service(self.temp_dir)

        with pytest.raises(LHPError) as exc_info:
            analyzer.analyze_dependencies_by_job()

        assert exc_info.value.code == "LHP-VAL-002"
        assert "Inconsistent job_name usage" in exc_info.value.title

    @patch("lhp.core.dependencies.service.DependencyAnalysisService.get_flowgroups")
    def test_live_job_write_path_rejects_inconsistent_job_names(
        self, mockget_flowgroups, sample_flowgroups_mixed_job_name
    ):
        """The live ``dag --format job`` write path runs ``validate_job_names``
        via the consolidated service method.

        A job-name-inconsistent config (mixed usage) must raise
        ``LHP-VAL-002`` before any job file is written.
        """
        from lhp.core.dependencies.output_writer import DependencyOutputWriter

        mockget_flowgroups.return_value = sample_flowgroups_mixed_job_name

        analyzer = _make_service(self.temp_dir)
        # Build the global analysis the writer is handed; the writer re-validates
        # and partitions via analyze_dependencies_by_job(), which is where the
        # inconsistent config is now rejected.
        graphs = analyzer.build_graphs(sample_flowgroups_mixed_job_name)
        result = analyzer.analyze(graphs)

        output_dir = self.temp_dir / ".lhp" / "dependencies"
        writer = DependencyOutputWriter()

        with pytest.raises(LHPError) as exc_info:
            writer.save_outputs(analyzer, result, ["job"], output_dir)

        assert exc_info.value.code == "LHP-VAL-002"
        # No job file should have been written before the validation fired.
        assert not list(output_dir.glob("*.job.yml"))

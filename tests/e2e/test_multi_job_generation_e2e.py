"""End-to-end tests for multi-job generation feature."""

from pathlib import Path

import pytest
import yaml


@pytest.mark.e2e
class TestMultiJobE2E:
    """End-to-end tests for complete multi-job workflow."""

    def test_complete_multi_job_workflow(self, tmp_path):
        """Test complete workflow: flowgroups with job_name → job generation."""

    def test_single_job_backward_compatibility(self, tmp_path):
        """Test that existing projects without job_name still work."""

    def test_validation_prevents_mixed_usage(self, tmp_path):
        """Test that validation prevents mixed job_name usage."""


@pytest.mark.e2e
class TestCLIIntegration:
    """Test CLI integration with multi-job generation."""

    def test_cli_output_shows_multiple_jobs(self, capsys, tmp_path):
        """Test that CLI displays multiple generated job files."""

    def test_pipeline_filter_blocked_with_job_name(self, tmp_path):
        """Test that --pipeline flag is blocked when job_name is used."""

    def test_bundle_output_directory_structure(self, tmp_path):
        """Test that --bundle-output creates correct structure."""


class TestJobConfigMerging:
    """Test job config merging in E2E scenarios."""

    def test_project_defaults_applied_to_all_jobs(self, tmp_path):
        """Test that project_defaults apply to all generated jobs."""

    def test_job_specific_overrides_work(self, tmp_path):
        """Test that job-specific configs override project_defaults."""

    def test_tags_deep_merge(self, tmp_path):
        """Test that tags are deep-merged correctly."""


class TestMasterJobGeneration:
    """Test master orchestration job generation."""

    def test_master_job_contains_all_jobs(self, tmp_path):
        """Test that master job references all individual jobs."""

    def test_master_job_respects_dependencies(self, tmp_path):
        """Test that master job creates correct task dependencies."""

    def test_master_job_naming(self, tmp_path):
        """Test master job naming convention."""


class TestErrorHandling:
    """Test error handling in E2E scenarios."""

    def test_invalid_job_name_format_caught(self, tmp_path):
        """Test that invalid job_name format is caught."""

    def test_malformed_job_config_caught(self, tmp_path):
        """Test that malformed job_config.yaml is caught."""


class TestOutputFormats:
    """Test multi-job generation with different output formats."""

    def test_job_format_only(self, tmp_path):
        """Test generating only job format with multi-job."""

    def test_all_formats_with_multi_job(self, tmp_path):
        """Test generating all formats with multi-job."""

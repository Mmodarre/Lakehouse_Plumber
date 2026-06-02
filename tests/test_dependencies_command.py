"""Tests for dependencies command implementation."""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import click
import pytest

from lhp.api.responses import (
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
)
from lhp.api.views import FlowgroupView
from lhp.cli.commands.dependencies_command import DependenciesCommand
from lhp.errors import ErrorCategory, LHPError


def _make_flowgroup_view(
    *,
    name: str = "fg1",
    pipeline: str = "pipeline1",
    job_name=None,
) -> FlowgroupView:
    """Build a minimal :class:`FlowgroupView` for tests.

    Mirrors the public DTO shape declared in ``lhp.api.views``.
    """

    return FlowgroupView(
        name=name,
        pipeline=pipeline,
        file_path=None,
        presets=(),
        template=None,
        load_action_count=0,
        transform_action_count=0,
        write_action_count=0,
        test_action_count=0,
        job_name=job_name,
    )


class TestDependenciesCommand:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self) -> DependencyAnalysisResult:
        """Build a frozen :class:`DependencyAnalysisResult` test fixture.

        Mirrors the public-API DTO shape (tuple-based, no graph object).
        """
        return DependencyAnalysisResult(
            pipeline_dependencies={
                "pipeline1": (),
                "pipeline2": ("pipeline1",),
            },
            execution_stages=(("pipeline1",), ("pipeline2",)),
            circular_dependencies=(),
            external_sources=("external.table1",),
            total_pipelines=2,
            total_external_sources=1,
        )

    def _make_outputs_result(self) -> DependencyOutputsResult:
        return DependencyOutputsResult(
            success=True,
            entries=(),
            output_dir=self.temp_dir,
        )

    @patch("lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_execute_with_pipeline_filter(
        self, mock_ensure_root, mock_setup, mock_facade_cls
    ):
        mock_ensure_root.return_value = self.temp_dir

        mock_facade = Mock()
        mock_facade_cls.for_project.return_value = mock_facade

        result = self.create_mock_analysis_result()
        mock_facade.inspection.analyze_dependencies.return_value = result
        mock_facade.inspection.save_dependency_outputs.return_value = (
            self._make_outputs_result()
        )

        # The CLI calls list_flowgroups() for the pipeline existence check.
        mock_facade.inspection.list_flowgroups.return_value = (
            _make_flowgroup_view(name="fg1", pipeline="target_pipeline"),
            _make_flowgroup_view(name="fg2", pipeline="other_pipeline"),
        )

        with patch("click.echo"):
            self.command.execute(pipeline="target_pipeline")

        mock_facade_cls.for_project.assert_called_with(self.temp_dir)
        mock_facade.inspection.analyze_dependencies.assert_called_once_with(
            pipeline_filter="target_pipeline",
            expand_blueprints=False,
            blueprint_filter=None,
        )

    @patch("lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_pipeline_validation_failure(
        self, mock_ensure_root, mock_setup, mock_facade_cls
    ):
        mock_ensure_root.return_value = self.temp_dir

        mock_facade = Mock()
        mock_facade_cls.for_project.return_value = mock_facade

        mock_facade.inspection.list_flowgroups.return_value = (
            _make_flowgroup_view(name="fg1", pipeline="existing_pipeline1"),
            _make_flowgroup_view(name="fg2", pipeline="existing_pipeline2"),
        )

        with pytest.raises(LHPError) as exc_info:
            with patch("click.echo"):
                self.command.execute(pipeline="nonexistent_pipeline")

        assert "LHP-CFG-" in exc_info.value.code
        assert "Pipeline 'nonexistent_pipeline' not found" in exc_info.value.title

    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_verbose_logging_setup(self, mock_ensure_root, mock_setup):
        mock_ensure_root.return_value = self.temp_dir

        with (
            patch(
                "lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade"
            ) as mock_facade_cls,
            patch("click.echo"),
            patch("logging.getLogger") as mock_get_logger,
        ):
            mock_facade = Mock()
            mock_facade_cls.for_project.return_value = mock_facade

            mock_result = Mock(spec=DependencyAnalysisResult)
            mock_result.total_external_sources = 0
            mock_result.total_pipelines = 0
            mock_result.execution_stages = ()
            mock_result.circular_dependencies = ()
            mock_result.external_sources = ()
            mock_facade.inspection.analyze_dependencies.return_value = mock_result
            mock_facade.inspection.save_dependency_outputs.return_value = (
                self._make_outputs_result()
            )

            dep_logger = Mock()
            out_logger = Mock()
            mock_get_logger.side_effect = lambda name: {
                "lhp.core.services.dependency_analyzer": dep_logger,
                "lhp.core.dependencies.output": out_logger,
            }.get(name, Mock())

            self.command.execute(verbose=True)

            dep_logger.setLevel.assert_called_with(logging.DEBUG)
            out_logger.setLevel.assert_called_with(logging.DEBUG)

    def test_parse_output_formats_valid(self):
        valid_formats = "dot,json,text"
        result = self.command._parse_output_formats(valid_formats)
        assert sorted(result) == ["dot", "json", "text"]

    def test_parse_output_formats_single(self):
        result = self.command._parse_output_formats("json")
        assert result == ["json"]

    def test_parse_output_formats_all(self):
        result = self.command._parse_output_formats("all")
        assert result == ["all"]

    def test_parse_output_formats_invalid(self):
        with pytest.raises(click.BadParameter) as exc_info:
            self.command._parse_output_formats("invalid,another_invalid")

        error_msg = str(exc_info.value)
        assert "invalid" in error_msg and "another_invalid" in error_msg
        assert "Invalid output format(s):" in error_msg

    def test_resolve_output_path_custom(self):
        custom_dir = "/custom/output/path"
        project_root = Path("/project")

        result = self.command._resolve_output_path(custom_dir, project_root)
        assert result == Path(custom_dir).resolve()

    def test_resolve_output_path_default(self):
        project_root = Path("/project")

        result = self.command._resolve_output_path(None, project_root)
        assert result == project_root / ".lhp" / "dependencies"

    @patch.object(
        DependenciesCommand, "setup_from_context", side_effect=Exception("Setup failed")
    )
    def test_error_propagation_generic_exception(self, mock_setup):
        """Generic exceptions propagate without wrapping; cli_error_boundary handles them."""
        with pytest.raises(Exception, match="Setup failed"):
            self.command.execute()

    @patch.object(
        DependenciesCommand,
        "setup_from_context",
        side_effect=LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Configuration error",
            details="Invalid config",
        ),
    )
    def test_error_propagation_lhp_error(self, mock_setup):
        with pytest.raises(LHPError) as exc_info:
            self.command.execute()

        assert "LHP-CFG-" in exc_info.value.code
        assert "Configuration error" in exc_info.value.title

    def test_job_name_parameter_handling(self):
        with (
            patch(
                "lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade"
            ) as mock_facade_cls,
            patch.object(DependenciesCommand, "setup_from_context"),
            patch.object(
                DependenciesCommand, "ensure_project_root", return_value=self.temp_dir
            ),
            patch("click.echo"),
        ):
            mock_facade = Mock()
            mock_facade_cls.for_project.return_value = mock_facade

            mock_result = Mock(spec=DependencyAnalysisResult)
            mock_result.total_external_sources = 0
            mock_result.total_pipelines = 0
            mock_result.execution_stages = ()
            mock_result.circular_dependencies = ()
            mock_result.external_sources = ()
            mock_facade.inspection.analyze_dependencies.return_value = mock_result
            mock_facade.inspection.save_dependency_outputs.return_value = (
                self._make_outputs_result()
            )

            custom_job_name = "my_custom_job"
            self.command.execute(job_name=custom_job_name, output_format="job")

            # New facade signature: kwargs-only — assert via kwargs["job_name"].
            call_args = mock_facade.inspection.save_dependency_outputs.call_args
            assert call_args.kwargs["job_name"] == custom_job_name


class TestDependenciesCommandPipelineFilter:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self) -> DependencyAnalysisResult:
        return DependencyAnalysisResult(
            pipeline_dependencies={
                "pipeline1": (),
                "pipeline2": ("pipeline1",),
            },
            execution_stages=(("pipeline1",), ("pipeline2",)),
            circular_dependencies=(),
            external_sources=("external.table1",),
            total_pipelines=2,
            total_external_sources=1,
        )

    @patch("lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_pipeline_filter_with_job_name_raises_error_003(
        self,
        mock_ensure_root,
        mock_setup,
        mock_facade_cls,
    ):
        mock_ensure_root.return_value = self.temp_dir

        mock_facade = Mock()
        mock_facade_cls.for_project.return_value = mock_facade

        flowgroups = (
            _make_flowgroup_view(
                name="fg1", pipeline="bronze_pipeline", job_name="bronze_job"
            ),
            _make_flowgroup_view(
                name="fg2", pipeline="silver_pipeline", job_name="silver_job"
            ),
        )
        mock_facade.inspection.list_flowgroups.return_value = flowgroups

        with pytest.raises(LHPError) as exc_info:
            self.command.execute(pipeline="bronze_pipeline")

        error = exc_info.value
        assert error.code == "LHP-VAL-003"
        assert (
            "Pipeline filter not supported with job_name" in error.title
            or "pipeline" in error.title.lower()
        )

    @patch("lhp.cli.commands.dependencies_command.LakehousePlumberApplicationFacade")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    @patch("click.echo")
    def test_pipeline_filter_without_job_name_works(
        self,
        mock_echo,
        mock_ensure_root,
        mock_setup,
        mock_facade_cls,
    ):
        mock_ensure_root.return_value = self.temp_dir

        mock_facade = Mock()
        mock_facade_cls.for_project.return_value = mock_facade

        flowgroups = (
            _make_flowgroup_view(name="fg1", pipeline="bronze_pipeline", job_name=None),
            _make_flowgroup_view(name="fg2", pipeline="silver_pipeline", job_name=None),
        )
        mock_facade.inspection.list_flowgroups.return_value = flowgroups

        result = self.create_mock_analysis_result()
        mock_facade.inspection.analyze_dependencies.return_value = result

        deps_path = self.temp_dir / "deps.dot"
        deps_path.touch()
        mock_facade.inspection.save_dependency_outputs.return_value = (
            DependencyOutputsResult(
                success=True,
                entries=(
                    DependencyOutputEntry(format_name="dot", label="", path=deps_path),
                ),
                output_dir=self.temp_dir,
            )
        )

        self.command.execute(output_format="dot", pipeline="bronze_pipeline")

        mock_facade.inspection.analyze_dependencies.assert_called_once()

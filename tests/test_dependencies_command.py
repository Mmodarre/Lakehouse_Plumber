"""Tests for dependencies command implementation."""

import logging
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import click
import networkx as nx
import pytest

from lhp.cli.commands.dependencies_command import DependenciesCommand
from lhp.models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
    PipelineDependency,
)
from lhp.errors import ErrorCategory, LHPError


class TestDependenciesCommand:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self):
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={"total_pipelines": 2},
        )

        pipeline_deps = {
            "pipeline1": PipelineDependency(
                pipeline="pipeline1",
                depends_on=[],
                flowgroup_count=1,
                action_count=2,
                external_sources=["external.table1"],
                stage=0,
            ),
            "pipeline2": PipelineDependency(
                pipeline="pipeline2",
                depends_on=["pipeline1"],
                flowgroup_count=1,
                action_count=3,
                external_sources=[],
                stage=1,
            ),
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_deps,
            execution_stages=[["pipeline1"], ["pipeline2"]],
            circular_dependencies=[],
            external_sources=["external.table1"],
        )

    @patch("lhp.cli.commands.dependencies_command.DependencyAnalysisService")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_execute_with_pipeline_filter(
        self, mock_ensure_root, mock_setup, mock_analyzer_class
    ):
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        result = self.create_mock_analysis_result()
        result.pipeline_dependencies = {
            "target_pipeline": result.pipeline_dependencies["pipeline1"]
        }
        mock_analyzer.analyze_dependencies.return_value = result

        mock_fg1 = Mock(pipeline="target_pipeline", job_name=None)
        mock_fg2 = Mock(pipeline="other_pipeline", job_name=None)
        mock_analyzer.get_flowgroups.return_value = [mock_fg1, mock_fg2]

        with (
            patch("lhp.cli.commands.dependencies_command.DependencyOutputManager"),
            patch("click.echo"),
        ):
            self.command.execute(pipeline="target_pipeline")

        mock_analyzer.analyze_dependencies.assert_called_once_with(
            pipeline_filter="target_pipeline"
        )

    @patch("lhp.cli.commands.dependencies_command.DependencyAnalysisService")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_pipeline_validation_failure(
        self, mock_ensure_root, mock_setup, mock_analyzer_class
    ):
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_flowgroups = [
            Mock(pipeline="existing_pipeline1"),
            Mock(pipeline="existing_pipeline2"),
        ]
        mock_analyzer.get_flowgroups.return_value = mock_flowgroups

        with pytest.raises(LHPError) as exc_info:
            with (
                patch("lhp.cli.commands.dependencies_command.DependencyOutputManager"),
                patch("click.echo"),
            ):
                self.command.execute(pipeline="nonexistent_pipeline")

        assert "LHP-CFG-" in exc_info.value.code
        assert "Pipeline 'nonexistent_pipeline' not found" in exc_info.value.title

    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_verbose_logging_setup(self, mock_ensure_root, mock_setup):
        mock_ensure_root.return_value = self.temp_dir

        with (
            patch(
                "lhp.cli.commands.dependencies_command.DependencyAnalysisService"
            ) as mock_analyzer_class,
            patch("lhp.cli.commands.dependencies_command.DependencyOutputManager"),
            patch("click.echo"),
            patch("logging.getLogger") as mock_get_logger,
        ):

            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_result = Mock()
            mock_result.total_external_sources = 0
            mock_result.execution_stages = []
            mock_result.circular_dependencies = []
            mock_analyzer.analyze_dependencies.return_value = mock_result

            dep_logger = Mock()
            out_logger = Mock()
            mock_get_logger.side_effect = lambda name: {
                "lhp.core.dependencies.analyzer": dep_logger,
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
                "lhp.cli.commands.dependencies_command.DependencyAnalysisService"
            ) as mock_analyzer_class,
            patch(
                "lhp.cli.commands.dependencies_command.DependencyOutputManager"
            ) as mock_output_manager_class,
            patch.object(DependenciesCommand, "setup_from_context"),
            patch.object(
                DependenciesCommand, "ensure_project_root", return_value=self.temp_dir
            ),
            patch("click.echo"),
        ):

            mock_analyzer = Mock()
            mock_analyzer_class.return_value = mock_analyzer
            mock_result = Mock()
            mock_result.total_external_sources = 0
            mock_result.execution_stages = []
            mock_result.circular_dependencies = []
            mock_analyzer.analyze_dependencies.return_value = mock_result

            mock_output_manager = Mock()
            mock_output_manager_class.return_value = mock_output_manager
            mock_output_manager.save_outputs.return_value = {}

            custom_job_name = "my_custom_job"
            self.command.execute(job_name=custom_job_name, output_format="job")

            call_args = mock_output_manager.save_outputs.call_args
            assert call_args[0][4] == custom_job_name  # 5th positional argument (job_name)


class TestDependenciesCommandPipelineFilter:
    def setup_method(self):
        self.temp_dir = Path(tempfile.mkdtemp())
        self.command = DependenciesCommand()

    def teardown_method(self):
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def create_mock_analysis_result(self):
        graphs = DependencyGraphs(
            action_graph=nx.DiGraph(),
            flowgroup_graph=nx.DiGraph(),
            pipeline_graph=nx.DiGraph(),
            metadata={"total_pipelines": 2},
        )

        pipeline_deps = {
            "pipeline1": PipelineDependency(
                pipeline="pipeline1",
                depends_on=[],
                flowgroup_count=1,
                action_count=2,
                external_sources=["external.table1"],
                stage=0,
            ),
            "pipeline2": PipelineDependency(
                pipeline="pipeline2",
                depends_on=["pipeline1"],
                flowgroup_count=1,
                action_count=3,
                external_sources=[],
                stage=1,
            ),
        }

        return DependencyAnalysisResult(
            graphs=graphs,
            pipeline_dependencies=pipeline_deps,
            execution_stages=[["pipeline1"], ["pipeline2"]],
            circular_dependencies=[],
            external_sources=["external.table1"],
        )

    @patch("lhp.cli.commands.dependencies_command.DependencyOutputManager")
    @patch("lhp.cli.commands.dependencies_command.DependencyAnalysisService")
    @patch("lhp.cli.commands.dependencies_command.ProjectConfigLoader")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    def test_pipeline_filter_with_job_name_raises_error_003(
        self,
        mock_ensure_root,
        mock_setup,
        mock_config_loader,
        mock_analyzer_class,
        mock_output_manager_class,
    ):
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        from lhp.models.config import Action, ActionType, FlowGroup

        flowgroups = [
            FlowGroup(
                pipeline="bronze_pipeline",
                flowgroup="fg1",
                job_name="bronze_job",
                actions=[
                    Action(
                        name="load",
                        type=ActionType.LOAD,
                        source="raw.table",
                        target="v_table",
                    )
                ],
            ),
            FlowGroup(
                pipeline="silver_pipeline",
                flowgroup="fg2",
                job_name="silver_job",
                actions=[
                    Action(
                        name="load",
                        type=ActionType.LOAD,
                        source="bronze.table",
                        target="v_table",
                    )
                ],
            ),
        ]
        mock_analyzer.get_flowgroups.return_value = flowgroups

        with pytest.raises(LHPError) as exc_info:
            self.command.execute(pipeline="bronze_pipeline")

        error = exc_info.value
        assert error.code == "LHP-VAL-003"
        assert (
            "Pipeline filter not supported with job_name" in error.title
            or "pipeline" in error.title.lower()
        )

    @patch("lhp.cli.commands.dependencies_command.DependencyOutputManager")
    @patch("lhp.cli.commands.dependencies_command.DependencyAnalysisService")
    @patch("lhp.cli.commands.dependencies_command.ProjectConfigLoader")
    @patch.object(DependenciesCommand, "setup_from_context")
    @patch.object(DependenciesCommand, "ensure_project_root")
    @patch("click.echo")
    def test_pipeline_filter_without_job_name_works(
        self,
        mock_echo,
        mock_ensure_root,
        mock_setup,
        mock_config_loader,
        mock_analyzer_class,
        mock_output_manager_class,
    ):
        mock_ensure_root.return_value = self.temp_dir
        mock_analyzer = Mock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_output_manager = Mock()
        mock_output_manager_class.return_value = mock_output_manager

        from lhp.models.config import Action, ActionType, FlowGroup

        flowgroups = [
            FlowGroup(
                pipeline="bronze_pipeline",
                flowgroup="fg1",
                job_name=None,
                actions=[
                    Action(
                        name="load",
                        type=ActionType.LOAD,
                        source="raw.table",
                        target="v_table",
                    )
                ],
            ),
            FlowGroup(
                pipeline="silver_pipeline",
                flowgroup="fg2",
                job_name=None,
                actions=[
                    Action(
                        name="load",
                        type=ActionType.LOAD,
                        source="bronze.table",
                        target="v_table",
                    )
                ],
            ),
        ]
        mock_analyzer.get_flowgroups.return_value = flowgroups

        result = self.create_mock_analysis_result()
        mock_analyzer.analyze_dependencies.return_value = result

        generated_files = {"dot": self.temp_dir / "deps.dot"}
        (self.temp_dir / "deps.dot").touch()
        mock_output_manager.save_outputs.return_value = generated_files

        self.command.execute(output_format="dot", pipeline="bronze_pipeline")

        mock_analyzer.analyze_dependencies.assert_called_once()

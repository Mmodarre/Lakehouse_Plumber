"""Tests for generate command CLI implementation."""

import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from click.testing import CliRunner

from lhp.bundle.exceptions import BundleResourceError
from lhp.cli.commands.generate_command import GenerateCommand
from lhp.core.layers import (
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    ValidationResponse,
)


@pytest.fixture
def temp_project():
    """Create a temporary project structure."""
    temp_dir = Path(tempfile.mkdtemp())
    project_root = temp_dir / "test_project"
    project_root.mkdir()

    # Create lhp.yaml
    (project_root / "lhp.yaml").write_text("""
name: TestProject
version: 1.0.0
""")

    # Create substitutions directory and file
    (project_root / "substitutions").mkdir()
    (project_root / "substitutions" / "dev.yaml").write_text("""
dev:
  catalog: dev_catalog
  schema: dev_schema
""")

    # Create pipelines directory
    (project_root / "pipelines").mkdir()

    yield project_root

    shutil.rmtree(temp_dir)


@pytest.fixture
def generate_command():
    """Create GenerateCommand instance."""
    return GenerateCommand()


class TestGenerateCommandDisplayMethods:
    """Test display methods of GenerateCommand."""

    def test_display_generation_results_success_with_files(self, generate_command):
        """Test displaying successful generation with files written."""
        response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={},
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_generation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generated 1 file" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("Output location" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_results_success_dry_run(self, generate_command):
        """Test displaying successful generation in dry-run mode."""
        response = GenerationResponse(
            success=True,
            generated_filenames=(),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True},
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_generation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Dry run completed" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_results_success_no_files(self, generate_command):
        """Test displaying successful generation with no files written."""
        response = GenerationResponse(
            success=True,
            generated_filenames=(),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={},
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_generation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("up-to-date" in str(call).lower() for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_results_failure(self, generate_command):
        """Test displaying failed generation."""
        response = GenerationResponse(
            success=False,
            generated_filenames=(),
            files_written=0,
            total_flowgroups=0,
            output_location=None,
            performance_info={},
            error_message="Test error",
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_generation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generation failed" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("Test error" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_validation_results_success(self, generate_command):
        """Test displaying successful validation."""
        response = ValidationResponse(
            success=True, errors=[], warnings=[], validated_pipelines=["test_pipeline"]
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_validation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Validation successful" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 3

    def test_display_validation_results_with_warnings(self, generate_command):
        """Test displaying validation with warnings."""
        response = ValidationResponse(
            success=True,
            errors=[],
            warnings=["Warning 1", "Warning 2"],
            validated_pipelines=["test_pipeline"],
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_validation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Warnings found" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 3
            assert any("Warning 1" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 3

    def test_display_validation_results_failure(self, generate_command):
        """Test displaying failed validation."""
        response = ValidationResponse(
            success=False,
            errors=["Error 1", "Error 2"],
            warnings=[],
            validated_pipelines=[],
        )

        with patch("click.echo") as mock_echo:
            generate_command.display_validation_results(response)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Validation failed" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 3
            assert any("Error 1" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 3

    def test_display_startup_message(self, generate_command):
        """Test displaying startup message."""
        with (
            patch("click.echo") as mock_echo,
            patch.object(generate_command, "echo_verbose_info"),
        ):
            generate_command._display_startup_message("dev")

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generating pipeline code" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("dev" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_response_success_with_files(self, generate_command):
        """Test displaying generation response with files written."""
        response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={},
        )

        with patch("click.echo") as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("test_pipeline" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("Generated 1 file" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_response_dry_run(self, generate_command):
        """Test displaying generation response in dry-run mode."""
        response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True},
        )

        with patch("click.echo") as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Would generate" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("test.py" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_response_up_to_date(self, generate_command):
        """Test displaying generation response when up-to-date."""
        response = GenerationResponse(
            success=True,
            generated_filenames=(),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={},
        )

        with patch("click.echo") as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Up-to-date" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_generation_response_failure(self, generate_command):
        """Test displaying failed generation response."""
        response = GenerationResponse(
            success=False,
            generated_filenames=(),
            files_written=0,
            total_flowgroups=0,
            output_location=None,
            performance_info={},
            error_message="Generation error",
        )

        with patch("click.echo") as mock_echo:
            generate_command._display_generation_response(response, "test_pipeline")

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Generation failed" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("Generation error" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_completion_message_dry_run(self, generate_command):
        """Test displaying completion message for dry-run."""
        output_dir = Path("/output")

        with patch("click.echo") as mock_echo:
            generate_command._display_completion_message(0, output_dir, dry_run=True)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("Dry run completed" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_completion_message_with_files(self, generate_command):
        """Test displaying completion message with files generated."""
        output_dir = Path("/output")

        with patch("click.echo") as mock_echo:
            generate_command._display_completion_message(5, output_dir, dry_run=False)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("completed successfully" in str(call).lower() for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4
            assert any("5" in str(call) for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4

    def test_display_completion_message_no_files(self, generate_command):
        """Test displaying completion message with no files."""
        output_dir = Path("/output")

        with patch("click.echo") as mock_echo:
            generate_command._display_completion_message(0, output_dir, dry_run=False)

            assert mock_echo.called
            calls = [str(call) for call in mock_echo.call_args_list]
            assert any("up-to-date" in str(call).lower() for call in calls)  # SNAPSHOT-TODO: re-target to new Rich output in Phase 4


class TestGenerateCommandHelperMethods:
    """Test helper methods of GenerateCommand."""

    def test_create_application_facade(self, generate_command, temp_project):
        """Test creating application facade."""
        facade = generate_command._create_application_facade(
            temp_project, pipeline_config_path=None
        )

        assert isinstance(facade, LakehousePlumberApplicationFacade)
        assert facade.orchestrator is not None
        assert not hasattr(facade, "state_manager")

    def test_create_application_facade_with_pipeline_config(
        self, generate_command, temp_project
    ):
        """Test creating application facade with pipeline config."""
        config_file = temp_project / "pipeline_config.yaml"
        config_file.write_text("project_defaults:\n  serverless: false")

        facade = generate_command._create_application_facade(
            temp_project, pipeline_config_path=str(config_file)
        )

        assert isinstance(facade, LakehousePlumberApplicationFacade)

    def test_get_pipeline_names_specific(self, generate_command):
        """Test _get_pipeline_names with specific pipeline."""
        from lhp.models.config import FlowGroup

        result = GenerateCommand._get_pipeline_names(
            "test_pipeline",
            [
                FlowGroup(pipeline="other", flowgroup="fg1", actions=[]),
            ],
        )

        assert result == ["test_pipeline"]

    def test_get_pipeline_names_all(self, generate_command):
        """Test _get_pipeline_names extracts unique pipelines from flowgroups."""
        from lhp.models.config import FlowGroup

        all_flowgroups = [
            FlowGroup(pipeline="pipeline1", flowgroup="fg1", actions=[]),
            FlowGroup(pipeline="pipeline2", flowgroup="fg2", actions=[]),
            FlowGroup(pipeline="pipeline1", flowgroup="fg3", actions=[]),
        ]

        result = GenerateCommand._get_pipeline_names(None, all_flowgroups)

        assert len(result) == 2
        assert "pipeline1" in result
        assert "pipeline2" in result

    def test_get_pipeline_names_empty(self, generate_command):
        """Test _get_pipeline_names when no flowgroups exist."""
        result = GenerateCommand._get_pipeline_names(None, [])

        assert result == []

    def test_handle_bundle_operations_enabled(self, generate_command, temp_project):
        """Test handling bundle operations when enabled."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        # Create databricks.yml
        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")

        with (
            patch("click.echo") as mock_echo,
            patch(
                "lhp.cli.commands.generate_command.BundleManager"
            ) as mock_bundle_manager,
        ):
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance

            generate_command._handle_bundle_operations(
                temp_project,
                output_dir,
                "dev",
                no_bundle=False,
                dry_run=False,
                pipeline_config_path=None,
            )

            # Echo assertion deleted — the "Bundle support detected"
            # string is defined verbatim in the source under test
            # (generate_command.py: ``click.echo("Bundle support
            # detected")``). The behavioral contract — bundle sync was
            # actually triggered — is captured by the mock assertion
            # below.
            mock_manager_instance.sync_resources_with_generated_files.assert_called_once()

    def test_handle_bundle_operations_dry_run(self, generate_command, temp_project):
        """Test handling bundle operations in dry-run mode."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")

        with (
            patch("click.echo") as mock_echo,
            patch(
                "lhp.cli.commands.generate_command.BundleManager"
            ) as mock_bundle_manager,
        ):
            generate_command._handle_bundle_operations(
                temp_project,
                output_dir,
                "dev",
                no_bundle=False,
                dry_run=True,
                pipeline_config_path=None,
            )

            # Echo assertion deleted — the "would be performed" string
            # is defined verbatim in the source under test
            # (generate_command.py: ``click.echo("📦 Bundle sync would be
            # performed")``). The behavioral contract for dry-run — the
            # bundle manager was NOT constructed/invoked — is captured
            # by the mock assertion below.
            mock_bundle_manager.assert_not_called()

    def test_handle_bundle_operations_with_force_and_config(
        self, generate_command, temp_project
    ):
        """Test handling bundle operations with force and pipeline config."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")

        with (
            patch("click.echo") as mock_echo,
            patch(
                "lhp.cli.commands.generate_command.BundleManager"
            ) as mock_bundle_manager,
        ):
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance

            generate_command._handle_bundle_operations(
                temp_project,
                output_dir,
                "dev",
                no_bundle=False,
                dry_run=False,
                pipeline_config_path="pipeline_config.yaml",
            )

            # The previous "Regenerating ... pipeline-config override"
            # echo assertion probed a rendered banner that is defined
            # verbatim in the source under test. The behavioral contract
            # — that the pipeline-config override was honored — is
            # captured by verifying BundleManager received that path.
            mock_bundle_manager.assert_called_once()
            ctor_args, ctor_kwargs = mock_bundle_manager.call_args
            # BundleManager(project_root, pipeline_config_path, project_config=...)
            assert ctor_args[1] == "pipeline_config.yaml"

    def test_handle_bundle_operations_bundle_error(
        self, generate_command, temp_project
    ):
        """Test handling bundle operations when bundle error occurs."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")

        with patch(
            "lhp.cli.commands.generate_command.BundleManager"
        ) as mock_bundle_manager:
            mock_manager_instance = Mock()
            mock_bundle_manager.return_value = mock_manager_instance
            mock_manager_instance.sync_resources_with_generated_files.side_effect = (
                BundleResourceError("Bundle error")
            )

            with pytest.raises(BundleResourceError, match="Bundle error"):
                generate_command._handle_bundle_operations(
                    temp_project,
                    output_dir,
                    "dev",
                    no_bundle=False,
                    dry_run=False,
                    pipeline_config_path=None,
                )

    def test_handle_bundle_operations_unexpected_error(
        self, generate_command, temp_project
    ):
        """Test handling bundle operations when unexpected error occurs."""
        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        (temp_project / "databricks.yml").write_text("targets:\n  dev: {}")

        with patch(
            "lhp.cli.commands.generate_command.BundleManager"
        ) as mock_bundle_manager:
            mock_bundle_manager.side_effect = Exception("Unexpected error")

            with pytest.raises(Exception, match="Unexpected error"):
                generate_command._handle_bundle_operations(
                    temp_project,
                    output_dir,
                    "dev",
                    no_bundle=False,
                    dry_run=False,
                    pipeline_config_path=None,
                )

    def test_get_user_input(self, generate_command):
        """Test getting user input."""
        with patch("builtins.input", return_value="user response"):
            result = generate_command.get_user_input("Prompt: ")
            assert result == "user response"


class TestGenerateCommandExecute:
    """Test execute method of GenerateCommand."""

    def test_execute_no_pipelines_found(self, generate_command, temp_project):
        """Test execute when no pipelines are found."""
        from lhp.utils.error_formatter import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = []
        mock_facade_instance.state_manager = Mock()

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
            patch.object(generate_command, "_display_startup_message"),
            patch.object(
                generate_command,
                "check_substitution_file",
                return_value=temp_project / "substitutions" / "dev.yaml",
            ),
            patch.object(
                generate_command,
                "_create_application_facade",
                return_value=mock_facade_instance,
            ),
            patch("click.echo"),
        ):

            with pytest.raises(LHPConfigError, match="No flowgroups found"):
                generate_command.execute("dev")

    def test_execute_basic_flow(self, generate_command, temp_project):
        """Test basic execute flow."""
        from lhp.core.layers import BatchGenerationResponse
        from lhp.models.config import FlowGroup

        output_dir = temp_project / "generated" / "dev"
        output_dir.mkdir(parents=True)

        mock_response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=output_dir,
            performance_info={},
        )

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[])
        ]
        mock_facade_instance.state_manager = Mock()
        # New flow calls application_facade.generate_pipelines once with all
        # pipeline names; mock returns a BatchGenerationResponse aggregating
        # per-pipeline GenerationResponse objects.
        mock_facade_instance.generate_pipelines.return_value = BatchGenerationResponse(
            success=True,
            pipeline_responses={"test_pipeline": mock_response},
            total_files_written=1,
            aggregate_generated_filenames=("test.py",),
            output_location=output_dir,
        )

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
            patch.object(generate_command, "_display_startup_message"),
            patch.object(
                generate_command,
                "check_substitution_file",
                return_value=temp_project / "substitutions" / "dev.yaml",
            ),
            patch.object(
                generate_command,
                "_create_application_facade",
                return_value=mock_facade_instance,
            ),
            patch.object(generate_command, "_display_generation_response"),
            patch.object(generate_command, "_handle_bundle_operations"),
            patch.object(generate_command, "_display_completion_message"),
            patch("click.echo"),
        ):

            generate_command.execute("dev")

            # Verify key methods were called
            mock_facade_instance.orchestrator.discover_all_flowgroups.assert_called_once()
            mock_facade_instance.generate_pipelines.assert_called_once()

    def test_execute_with_dry_run(self, generate_command, temp_project):
        """Test execute with dry-run flag."""
        from lhp.core.layers import BatchGenerationResponse
        from lhp.models.config import FlowGroup

        mock_response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True},
        )

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[])
        ]
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generate_pipelines.return_value = BatchGenerationResponse(
            success=True,
            pipeline_responses={"test_pipeline": mock_response},
            total_files_written=0,
            aggregate_generated_filenames=("test.py",),
            output_location=None,
        )

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
            patch.object(generate_command, "_display_startup_message"),
            patch.object(
                generate_command,
                "check_substitution_file",
                return_value=temp_project / "substitutions" / "dev.yaml",
            ),
            patch.object(
                generate_command,
                "_create_application_facade",
                return_value=mock_facade_instance,
            ),
            patch.object(generate_command, "_display_generation_response"),
            patch.object(generate_command, "_handle_bundle_operations"),
            patch.object(generate_command, "_display_completion_message"),
            patch("click.echo"),
        ):

            generate_command.execute("dev", dry_run=True)

            # Verify dry_run results in output_dir=None passed to the
            # batch facade (the facade method is the new single entry point).
            call_kwargs = mock_facade_instance.generate_pipelines.call_args.kwargs
            assert call_kwargs["output_dir"] is None

    def test_execute_with_no_bundle(self, generate_command, temp_project):
        """Test execute with no_bundle flag."""
        from lhp.core.layers import BatchGenerationResponse
        from lhp.models.config import FlowGroup

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[])
        ]
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generate_pipelines.return_value = BatchGenerationResponse(
            success=True,
            pipeline_responses={},
            total_files_written=0,
            aggregate_generated_filenames=(),
            output_location=temp_project / "generated" / "dev",
        )

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
            patch.object(generate_command, "_display_startup_message"),
            patch.object(
                generate_command,
                "check_substitution_file",
                return_value=temp_project / "substitutions" / "dev.yaml",
            ),
            patch.object(
                generate_command,
                "_create_application_facade",
                return_value=mock_facade_instance,
            ),
            patch.object(generate_command, "_display_generation_response"),
            patch.object(generate_command, "_handle_bundle_operations") as mock_bundle,
            patch.object(generate_command, "_display_completion_message"),
            patch("click.echo"),
        ):

            generate_command.execute("dev", no_bundle=True)

            # Verify bundle operations were not called
            mock_bundle.assert_not_called()

    def test_execute_with_custom_output(self, generate_command, temp_project):
        """Test execute with custom output directory."""
        from lhp.core.layers import BatchGenerationResponse
        from lhp.models.config import FlowGroup

        custom_output = temp_project / "custom_output"

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = [
            FlowGroup(pipeline="test_pipeline", flowgroup="fg1", actions=[])
        ]
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generate_pipelines.return_value = BatchGenerationResponse(
            success=True,
            pipeline_responses={},
            total_files_written=0,
            aggregate_generated_filenames=(),
            output_location=custom_output,
        )

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
            patch.object(generate_command, "_display_startup_message"),
            patch.object(
                generate_command,
                "check_substitution_file",
                return_value=temp_project / "substitutions" / "dev.yaml",
            ),
            patch.object(
                generate_command,
                "_create_application_facade",
                return_value=mock_facade_instance,
            ),
            patch.object(generate_command, "_display_generation_response"),
            patch.object(generate_command, "_handle_bundle_operations"),
            patch.object(generate_command, "_display_completion_message"),
            patch("click.echo"),
        ):

            generate_command.execute("dev", output=str(custom_output))

            # Verify custom output_dir reached the batch facade method
            call_kwargs = mock_facade_instance.generate_pipelines.call_args.kwargs
            assert call_kwargs["output_dir"] == custom_output

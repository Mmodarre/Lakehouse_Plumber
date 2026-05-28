"""Tests for generate command CLI implementation."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lhp.cli.commands.generate_command import GenerateCommand
from lhp.api.responses import GenerationResponse


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


class TestGenerateCommandExecute:
    """Test execute method of GenerateCommand."""

    def test_execute_no_pipelines_found(self, generate_command, temp_project):
        """Test execute when no pipelines are found."""
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = []
        mock_facade_instance.state_manager = Mock()

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
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
        from lhp.api.responses import BatchGenerationResponse
        from lhp.models import FlowGroup

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
            patch.object(generate_command, "_handle_bundle_operations"),
            patch("click.echo"),
        ):
            generate_command.execute("dev")

            # Verify key methods were called
            mock_facade_instance.orchestrator.discover_all_flowgroups.assert_called_once()
            mock_facade_instance.generate_pipelines.assert_called_once()

    def test_execute_with_dry_run(self, generate_command, temp_project):
        """Test execute with dry-run flag."""
        from lhp.api.responses import BatchGenerationResponse
        from lhp.models import FlowGroup

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
            patch.object(generate_command, "_handle_bundle_operations"),
            patch("click.echo"),
        ):
            generate_command.execute("dev", dry_run=True)

            # Verify dry_run results in output_dir=None passed to the
            # batch facade (the facade method is the new single entry point).
            call_kwargs = mock_facade_instance.generate_pipelines.call_args.kwargs
            assert call_kwargs["output_dir"] is None

    def test_execute_with_no_bundle(self, generate_command, temp_project):
        """Test execute with no_bundle flag."""
        from lhp.api.responses import BatchGenerationResponse
        from lhp.models import FlowGroup

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
            patch.object(generate_command, "_handle_bundle_operations") as mock_bundle,
            patch("click.echo"),
        ):
            generate_command.execute("dev", no_bundle=True)

            # Verify bundle operations were not called
            mock_bundle.assert_not_called()

    def test_force_flag_emits_deprecation_warning(self, generate_command, temp_project):
        """``--force`` is a deprecated no-op; ensure the user-facing
        warning is recorded on the per-run ``WarningCollector``.

        The deprecation branch runs near the top of ``execute()`` before
        any pipeline work, so the cheapest behavioral test is to drive
        ``execute`` with ``force=True`` against an empty project (which
        will subsequently raise ``LHPConfigError`` for "no flowgroups")
        and inspect the collector that was constructed during the call.
        """
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = []
        mock_facade_instance.state_manager = Mock()

        captured: list = []
        from lhp.api import WarningCollector as real_cls

        def _capturing_factory(*args, **kwargs):
            inst = real_cls(*args, **kwargs)
            captured.append(inst)
            return inst

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
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
            patch(
                "lhp.cli.commands.generate_command.WarningCollector",
                side_effect=_capturing_factory,
            ),
            patch("click.echo"),
        ):
            with pytest.raises(LHPConfigError):
                generate_command.execute("dev", force=True)

        assert captured, "WarningCollector was not constructed during execute"
        collector = captured[0]
        deprecation_entries = [
            (cat, msg) for (cat, msg) in collector._warnings if cat == "deprecation"
        ]
        assert deprecation_entries, "Expected a deprecation warning to be recorded"
        assert any("--force" in msg for (_, msg) in deprecation_entries), (
            f"Expected --force mention in deprecation message, got: {deprecation_entries}"
        )

    def test_no_state_flag_emits_deprecation_warning(
        self, generate_command, temp_project
    ):
        """``--no-state`` is a deprecated no-op; mirror of the
        ``--force`` test to cover both legacy flags."""
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.orchestrator.discover_all_flowgroups.return_value = []
        mock_facade_instance.state_manager = Mock()

        captured: list = []
        from lhp.api import WarningCollector as real_cls

        def _capturing_factory(*args, **kwargs):
            inst = real_cls(*args, **kwargs)
            captured.append(inst)
            return inst

        with (
            patch.object(generate_command, "setup_from_context"),
            patch.object(
                generate_command, "ensure_project_root", return_value=temp_project
            ),
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
            patch(
                "lhp.cli.commands.generate_command.WarningCollector",
                side_effect=_capturing_factory,
            ),
            patch("click.echo"),
        ):
            with pytest.raises(LHPConfigError):
                generate_command.execute("dev", no_state=True)

        assert captured, "WarningCollector was not constructed during execute"
        collector = captured[0]
        deprecation_entries = [
            (cat, msg) for (cat, msg) in collector._warnings if cat == "deprecation"
        ]
        assert deprecation_entries, "Expected a deprecation warning to be recorded"

    def test_execute_with_custom_output(self, generate_command, temp_project):
        """Test execute with custom output directory."""
        from lhp.api.responses import BatchGenerationResponse
        from lhp.models import FlowGroup

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
            patch.object(generate_command, "_handle_bundle_operations"),
            patch("click.echo"),
        ):
            generate_command.execute("dev", output=str(custom_output))

            # Verify custom output_dir reached the batch facade method
            call_kwargs = mock_facade_instance.generate_pipelines.call_args.kwargs
            assert call_kwargs["output_dir"] == custom_output

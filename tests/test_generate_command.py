"""Tests for generate command CLI implementation."""

import shutil
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from lhp.api import (
    BatchGenerationResponse,
    FlowgroupView,
    GenerationCompleted,
    GenerationResponse,
    OperationStarted,
)
from lhp.api.responses import ValidationResponse
from lhp.cli.commands.generate_command import GenerateCommand


def _flowgroup_view(pipeline: str = "test_pipeline", file_path=None) -> FlowgroupView:
    """Both ``pipeline`` and ``file_path`` must be set: ``execute()`` reads both fields."""
    return FlowgroupView(
        name="fg1",
        pipeline=pipeline,
        file_path=file_path,
    )


def _ok_validation_response() -> ValidationResponse:
    return ValidationResponse(
        success=True,
        issues=(),
        validated_pipelines=(),
    )


def _generation_stream(batch_response: BatchGenerationResponse):
    """Yield OperationStarted then GenerationCompleted — the two-event contract the CLI consumes."""

    def _stream(*args, **kwargs):
        yield OperationStarted(operation_name="generate", env=kwargs.get("env"))
        yield GenerationCompleted(response=batch_response)

    return _stream


@pytest.fixture
def temp_project():
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
    return GenerateCommand()


class TestGenerateCommandExecute:
    """Test execute method of GenerateCommand."""

    def test_execute_no_pipelines_found(self, generate_command, temp_project):
        """Empty ``list_flowgroups()`` result raises LHP-CFG-014 before any generation."""
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = ()
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
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

        batch_response = BatchGenerationResponse(
            success=True,
            pipeline_responses={"test_pipeline": mock_response},
            total_files_written=1,
            aggregate_generated_filenames=("test.py",),
            output_location=output_dir,
        )

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = (
            _flowgroup_view(),
        )
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generation.generate_pipelines.side_effect = (
            _generation_stream(batch_response)
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

            mock_facade_instance.inspection.list_flowgroups.assert_called_once()
            mock_facade_instance.generation.generate_pipelines.assert_called_once()

    def test_execute_with_dry_run(self, generate_command, temp_project):
        """Dry-run passes ``output_dir=None`` to ``generate_pipelines``."""
        mock_response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=0,
            total_flowgroups=1,
            output_location=None,
            performance_info={"dry_run": True},
        )

        batch_response = BatchGenerationResponse(
            success=True,
            pipeline_responses={"test_pipeline": mock_response},
            total_files_written=0,
            aggregate_generated_filenames=("test.py",),
            output_location=None,
        )

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = (
            _flowgroup_view(),
        )
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generation.generate_pipelines.side_effect = (
            _generation_stream(batch_response)
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

            call_kwargs = (
                mock_facade_instance.generation.generate_pipelines.call_args.kwargs
            )
            assert call_kwargs["output_dir"] is None

    def test_execute_with_no_bundle(self, generate_command, temp_project):
        """``no_bundle=True`` skips bundle operations entirely."""
        batch_response = BatchGenerationResponse(
            success=True,
            pipeline_responses={},
            total_files_written=0,
            aggregate_generated_filenames=(),
            output_location=temp_project / "generated" / "dev",
        )

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = (
            _flowgroup_view(),
        )
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generation.generate_pipelines.side_effect = (
            _generation_stream(batch_response)
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

            mock_bundle.assert_not_called()

    def test_force_flag_emits_deprecation_warning(self, generate_command, temp_project):
        """``--force`` is a no-op; the deprecation branch fires before any pipeline work, so driving
        ``execute`` with an empty project (which raises LHPConfigError) is enough to inspect the collector.
        """
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = ()
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
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
        """``--no-state`` is a deprecated no-op."""
        from lhp.errors import LHPConfigError

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = ()
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
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
        """Custom ``output`` is forwarded as ``output_dir`` to ``generate_pipelines``."""
        custom_output = temp_project / "custom_output"

        batch_response = BatchGenerationResponse(
            success=True,
            pipeline_responses={},
            total_files_written=0,
            aggregate_generated_filenames=(),
            output_location=custom_output,
        )

        mock_facade_instance = Mock()
        mock_facade_instance.inspection.list_flowgroups.return_value = (
            _flowgroup_view(),
        )
        mock_facade_instance.inspection.validate_duplicate_flowgroups.return_value = (
            _ok_validation_response()
        )
        mock_facade_instance.state_manager = Mock()
        mock_facade_instance.generation.generate_pipelines.side_effect = (
            _generation_stream(batch_response)
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

            call_kwargs = (
                mock_facade_instance.generation.generate_pipelines.call_args.kwargs
            )
            assert call_kwargs["output_dir"] == custom_output

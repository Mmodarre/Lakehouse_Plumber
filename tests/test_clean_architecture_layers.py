"""Tests for Clean Architecture layer separation."""

from unittest.mock import Mock

from lhp.core.coordination import (
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    ValidationIssue,
    ValidationResponse,
)
from lhp.models.processing import PipelineDelta
from lhp.errors import (
    ErrorCategory,
    LHPError,
    LHPFileError,
    LHPValidationError,
)


class TestDataTransferObjects:
    """Test Data Transfer Objects for clean layer communication."""

    def test_generation_response(self):
        """Test GenerationResponse DTO."""
        from pathlib import Path

        response = GenerationResponse(
            success=True,
            generated_filenames=("test.py",),
            files_written=1,
            total_flowgroups=1,
            output_location=Path("/output"),
            performance_info={"time": 1.5},
        )

        assert response.is_successful()
        assert len(response.generated_filenames) == 1
        assert response.files_written == 1
        assert response.total_flowgroups == 1
        assert response.output_location == Path("/output")
        assert response.performance_info["time"] == 1.5
        assert response.error_message is None

    def test_validation_response(self):
        """Test ValidationResponse DTO."""
        response = ValidationResponse(
            success=False,
            issues=[
                ValidationIssue(code="", severity="error", title="Error 1"),
                ValidationIssue(code="", severity="error", title="Error 2"),
                ValidationIssue(code="", severity="warning", title="Warning 1"),
            ],
            validated_pipelines=["pipeline1"],
        )

        assert not response.success
        assert response.has_errors()
        assert response.has_warnings()
        assert response.error_count == 2
        assert response.warning_count == 1
        assert response.validated_pipelines == ["pipeline1"]


class TestFacadeOnDeltaUnwrap:
    """Cover the ``_on_delta`` closure inside
    :meth:`LakehousePlumberApplicationFacade.generate_pipelines`.

    The closure builds the per-pipeline
    :class:`~lhp.core.coordination.layers.GenerationResponse.original_error` from the
    worker's :class:`~lhp.models.processing.PipelineDelta`. The contract
    is:

    1. ``delta.lhp_error`` present → live ``LHPError`` instance surfaces
       unchanged on ``response.original_error`` (subclass identity + code
       preserved; NO GEN-901 wrap).
    2. ``delta.lhp_error is None`` → the dispatcher
       :func:`~lhp.errors.lhp_error_from_worker_failure`
       synthesizes the right LHPError subclass (preserving
       ``ValueError``/``FileNotFoundError`` dual-inheritance) with
       ``LHP-GEN-901``.

    Tests drive the closure by mocking
    ``orchestrator.generate_pipelines_by_fields`` to invoke its
    ``on_pipeline_complete`` argument with a synthetic delta, then
    inspect what the facade forwards to the user-supplied
    ``on_pipeline_complete`` (which is where the per-pipeline
    ``GenerationResponse`` is observable).
    """

    def _run_facade_with_delta(self, delta: PipelineDelta) -> GenerationResponse:
        mock_orchestrator = Mock()

        def _invoke_callback(*args, **kwargs):
            kwargs["on_pipeline_complete"](delta)
            return {delta.pipeline_name: delta.generated_filenames}

        mock_orchestrator.generate_pipelines_by_fields.side_effect = _invoke_callback

        facade = LakehousePlumberApplicationFacade(mock_orchestrator)

        captured: dict[str, GenerationResponse] = {}

        def _on_pipeline_complete(name: str, response: GenerationResponse) -> None:
            captured[name] = response

        facade.generate_pipelines(
            pipeline_fields=[delta.pipeline_name],
            env="dev",
            output_dir=None,
            on_pipeline_complete=_on_pipeline_complete,
        )
        return captured[delta.pipeline_name]

    def test_on_delta_surfaces_live_lhp_validation_error_unchanged(self):
        """LHPValidationError raised in worker travels via ``delta.lhp_error``
        and shows up on ``response.original_error`` with its original code +
        subclass identity — no GEN-901 wrap."""
        original = LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="007",
            title="FlowGroup validation failed",
            details="Action[0] 'load_x': Unknown load source type",
            context={"Pipeline": "p"},
            suggestions=["Check the config"],
        )
        delta = PipelineDelta.failure("test_pipeline", original)

        response = self._run_facade_with_delta(delta)

        assert response.is_successful() is False
        assert response.original_error is original
        assert response.original_error.code == "LHP-VAL-007"
        assert isinstance(response.original_error, LHPValidationError)

    def test_on_delta_surfaces_live_lhp_file_error_unchanged(self):
        """``LHPFileError`` round-trip preserves the dual-inheritance shape
        (FileNotFoundError) as well as the original code."""
        original = LHPFileError(
            category=ErrorCategory.IO,
            code_number="001",
            title="Missing flowgroup file",
            details="The flowgroup file is missing.",
            context={"Pipeline": "p"},
        )
        delta = PipelineDelta.failure("test_pipeline", original)

        response = self._run_facade_with_delta(delta)

        assert response.original_error is original
        assert response.original_error.code == "LHP-IO-001"
        assert isinstance(response.original_error, LHPFileError)
        # Dual-inheritance: ``except FileNotFoundError`` would still catch.
        assert isinstance(response.original_error, FileNotFoundError)

    def test_on_delta_wraps_non_lhp_value_error_via_dispatcher(self):
        """When the worker raised a plain ``ValueError`` (lhp_error is None),
        the facade synthesizes an ``LHPValidationError`` (not a plain
        ``LHPError``) so callers' ``except ValueError`` handlers continue
        to catch worker failures. The code is ``LHP-GEN-901``."""
        delta = PipelineDelta(
            pipeline_name="test_pipeline",
            success=False,
            lhp_error=None,
            error_type="ValueError",
            error_message="bad value",
            error_traceback="Traceback ...\nValueError: bad value\n",
        )

        response = self._run_facade_with_delta(delta)

        assert response.original_error is not None
        assert response.original_error.code == "LHP-GEN-901"
        # Dispatcher routes ValueError → LHPValidationError so dual-inherit
        # ``except ValueError`` still catches.
        assert isinstance(response.original_error, LHPValidationError)
        assert isinstance(response.original_error, ValueError)

    def test_on_delta_wraps_unknown_exception_as_plain_lhp_error(self):
        """When the worker raised an exception type the dispatcher doesn't
        map (e.g. ``KeyError``), the fallback is plain ``LHPError`` with
        ``LHP-GEN-901``."""
        delta = PipelineDelta(
            pipeline_name="test_pipeline",
            success=False,
            lhp_error=None,
            error_type="KeyError",
            error_message="'missing'",
            error_traceback="Traceback ...\nKeyError: 'missing'\n",
        )

        response = self._run_facade_with_delta(delta)

        assert response.original_error is not None
        assert response.original_error.code == "LHP-GEN-901"
        assert isinstance(response.original_error, LHPError)
        # KeyError isn't in the dispatcher's stdlib mapping, so we get
        # the base class — NOT a ValueError-flavored subclass.
        assert not isinstance(response.original_error, ValueError)

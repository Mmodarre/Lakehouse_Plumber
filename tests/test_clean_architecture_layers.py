"""Tests for Clean Architecture layer separation."""

from unittest.mock import Mock

from lhp.api import (
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    ValidationIssueView,
    ValidationResponse,
)
from lhp.errors import (
    ErrorCategory,
    LHPError,
    LHPFileError,
    LHPValidationError,
)
from lhp.models.processing import PipelineDelta


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
                ValidationIssueView(
                    code="", category="VAL", severity="error", title="Error 1"
                ),
                ValidationIssueView(
                    code="", category="VAL", severity="error", title="Error 2"
                ),
                ValidationIssueView(
                    code="", category="VAL", severity="warning", title="Warning 1"
                ),
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
    """Cover the per-delta unwrap inside
    :func:`lhp.api._generate_stream._consume_generate_stream`
    (``src/lhp/api/_generate_stream.py`` — D-generate relocated this private
    generate-phase body out of the facade for the §3.3 cap).

    E3 replaced the old ``_do_generate_pipelines`` + ``_on_delta`` closure with
    the generate-phase generator ``_consume_generate_stream``, which consumes
    the orchestrator's delta-stream and runs
    :func:`lhp.api._generation_converters._delta_to_generation_response`
    on each worker :class:`~lhp.models.processing.PipelineDelta` to build the
    public, frozen :class:`~lhp.api.GenerationResponse` and emit the §5.7
    ``PipelineStarted`` + terminal (``PipelineFailed`` carries the unwrapped
    LHP code).

    Per constitution §4.8, ``GenerationResponse`` no longer carries a
    live exception instance — the old ``original_error`` field was
    removed (``responses.py:145``) and replaced by the flat string
    ``error_code`` (+ a :class:`~lhp.api.ValidationIssueView` ``error``).
    The error-unwrap contract therefore now surfaces as a *code* on the
    response rather than an instance:

    1. ``delta.lhp_error`` present → its ``.code`` is forwarded
       unchanged onto ``response.error_code`` (NO GEN-901 wrap). The
       live subclass identity / dual-inheritance shape is still asserted
       — but on ``delta.lhp_error`` (the instance the converter reads),
       since the response cannot legally hold it (§4.8).
    2. ``delta.lhp_error is None`` → the dispatcher
       :func:`~lhp.errors.lhp_error_from_worker_failure`
       (``errors/types.py:250``) synthesizes the right ``LHPError``
       subclass (preserving ``ValueError``/``FileNotFoundError``
       dual-inheritance via ``_WORKER_ERROR_TYPE_TO_LHP_CLASS``,
       ``types.py:234``) with ``LHP-GEN-901``; the response carries
       ``error_code == "LHP-GEN-901"``. The dispatcher's subclass
       mapping is asserted directly against
       ``lhp_error_from_worker_failure``.

    Tests drive the unwrap by mocking the orchestrator method
    ``orchestrator.generate_pipelines`` to yield a synthetic delta (it is now a
    generator), then inspect the §5.7 events the facade emits and the
    per-pipeline ``GenerationResponse`` the converter builds. The driver targets
    ``lhp.api._generate_stream._consume_generate_stream`` (the generate-phase
    body that owns the per-delta unwrap) directly, bypassing the public
    generator's project preflight so the unwrap is exercised in isolation.

    Corroborating already-passing tests (the §4.8 ``error_code`` surface
    these now assert):

    * ``tests/api/test_generate_stream_protocol.py::
      TestGenerateGateStreamProtocol::
      test_gate_failure_emits_exactly_one_error_then_raises`` — a worker
      ``LHPError`` (``LHP-CFG-004``) travels back through the facade
      unwrap with its code preserved.
    * ``tests/api/test_generate_stream_protocol.py::
      TestGenerateCommitFailureOptionB::
      test_commit_oserror_surfaces_as_batch_failure_dto_not_raise`` — a
      non-LHP failure surfaces with ``error_code is None`` (no GEN-901
      coding of plain infra failures).
    * ``tests/api/test_responses_contract.py`` —
      ``failed_generation_response`` fixture (error path = ``error_code``
      + ``error`` view, no exception field) and ``TestFieldTypeContract``
      actively ban any ``LHPError``-typed DTO field (§4.8).
    """

    def _run_facade_with_delta(self, delta: PipelineDelta) -> GenerationResponse:
        import logging

        from lhp.api._generate_stream import _consume_generate_stream
        from lhp.api._generation_converters import _delta_to_generation_response
        from lhp.api.events import PipelineCompleted, PipelineFailed, PipelineStarted
        from lhp.api.facade import GenerationFacade

        mock_orchestrator = Mock()

        def _delta_stream(*_args, **_kwargs):
            # E3: the orchestrator's ``generate_pipelines`` is a generator
            # yielding per-pipeline ``PipelineDelta``s; the generate phase
            # consumes it and unwraps each via ``_delta_to_generation_response``.
            yield delta

        mock_orchestrator.generate_pipelines.side_effect = _delta_stream

        facade = LakehousePlumberApplicationFacade(mock_orchestrator)
        assert isinstance(facade.generation, GenerationFacade)

        # Drive the generate-phase body directly (it consumes the orchestrator
        # delta-stream and emits the §5.7 per-pipeline events), bypassing the
        # public generator's project preflight so the unwrap is exercised in
        # isolation. D-generate relocated this private generate-phase body out of
        # the facade into ``lhp.api._generate_stream._consume_generate_stream``
        # (a free function taking the orchestrator + logger explicitly) so the
        # facade module stays under the §3.3 cap; the per-delta unwrap contract
        # asserted below is unchanged. It is a generator that also RETURNS the
        # terminal batch DTO via ``StopIteration.value``.
        events: list = []
        gen = _consume_generate_stream(
            mock_orchestrator,
            logging.getLogger(__name__),
            pipeline_fields=[delta.pipeline_name],
            env="dev",
            output_dir=None,
        )
        try:
            while True:
                events.append(next(gen))
        except StopIteration:
            pass

        # No dangling Starts: exactly one PipelineStarted paired with one
        # terminal for this pipeline.
        starts = [e for e in events if isinstance(e, PipelineStarted)]
        terminals = [
            e for e in events if isinstance(e, (PipelineCompleted, PipelineFailed))
        ]
        assert [e.pipeline for e in starts] == [delta.pipeline_name]
        assert len(terminals) == 1
        # The facade forwards the unwrapped LHP code onto the failure event.
        if not delta.success:
            failed = terminals[0]
            assert isinstance(failed, PipelineFailed)
            assert failed.pipeline == delta.pipeline_name
            response = _delta_to_generation_response(delta, output_dir=None)
            assert failed.code == response.error_code

        # Return the per-pipeline response the facade built (via the same
        # converter) so the existing error_code / identity assertions hold.
        return _delta_to_generation_response(delta, output_dir=None)

    def test_on_delta_surfaces_live_lhp_validation_error_code(self):
        """LHPValidationError raised in worker travels via ``delta.lhp_error``;
        its code is forwarded unchanged onto ``response.error_code`` — no
        GEN-901 wrap. The live subclass identity is asserted on the delta
        (the response no longer holds the instance, §4.8)."""
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
        assert response.error_code == "LHP-VAL-007"
        # The §4.8 surface keeps the live instance off the DTO; the
        # subclass identity it preserved is checked on the delta.
        assert delta.lhp_error is original
        assert isinstance(delta.lhp_error, LHPValidationError)

    def test_on_delta_surfaces_live_lhp_file_error_code(self):
        """``LHPFileError`` round-trip forwards its code onto
        ``response.error_code``; the dual-inheritance shape
        (FileNotFoundError) is preserved on ``delta.lhp_error``."""
        original = LHPFileError(
            category=ErrorCategory.IO,
            code_number="001",
            title="Missing flowgroup file",
            details="The flowgroup file is missing.",
            context={"Pipeline": "p"},
        )
        delta = PipelineDelta.failure("test_pipeline", original)

        response = self._run_facade_with_delta(delta)

        assert response.error_code == "LHP-IO-001"
        assert delta.lhp_error is original
        assert isinstance(delta.lhp_error, LHPFileError)
        # Dual-inheritance: ``except FileNotFoundError`` would still catch.
        assert isinstance(delta.lhp_error, FileNotFoundError)

    def test_on_delta_wraps_non_lhp_value_error_via_dispatcher(self):
        """When the worker raised a plain ``ValueError`` (lhp_error is None),
        the converter synthesizes a ``GEN-901`` error via
        ``lhp_error_from_worker_failure`` and the response carries
        ``error_code == "LHP-GEN-901"``. The dispatcher routes ``ValueError``
        → ``LHPValidationError`` (dual-inherits ``ValueError``) — asserted
        directly against the dispatcher, since the synthesized instance is
        not held on the DTO (§4.8)."""
        from lhp.errors import lhp_error_from_worker_failure

        delta = PipelineDelta(
            pipeline_name="test_pipeline",
            success=False,
            lhp_error=None,
            error_type="ValueError",
            error_message="bad value",
            error_traceback="Traceback ...\nValueError: bad value\n",
        )

        response = self._run_facade_with_delta(delta)

        assert response.error_code == "LHP-GEN-901"
        # Dispatcher routes ValueError → LHPValidationError so dual-inherit
        # ``except ValueError`` still catches worker failures.
        synthesized = lhp_error_from_worker_failure(
            pipeline_name=delta.pipeline_name,
            error_type=delta.error_type,
            error_message=delta.error_message,
            error_traceback=delta.error_traceback,
        )
        assert synthesized.code == "LHP-GEN-901"
        assert isinstance(synthesized, LHPValidationError)
        assert isinstance(synthesized, ValueError)

    def test_on_delta_wraps_unknown_exception_as_plain_lhp_error(self):
        """When the worker raised an exception type the dispatcher doesn't
        map (e.g. ``KeyError``), the fallback is plain ``LHPError`` with
        ``LHP-GEN-901``; the response carries that code."""
        from lhp.errors import lhp_error_from_worker_failure

        delta = PipelineDelta(
            pipeline_name="test_pipeline",
            success=False,
            lhp_error=None,
            error_type="KeyError",
            error_message="'missing'",
            error_traceback="Traceback ...\nKeyError: 'missing'\n",
        )

        response = self._run_facade_with_delta(delta)

        assert response.error_code == "LHP-GEN-901"
        synthesized = lhp_error_from_worker_failure(
            pipeline_name=delta.pipeline_name,
            error_type=delta.error_type,
            error_message=delta.error_message,
            error_traceback=delta.error_traceback,
        )
        assert isinstance(synthesized, LHPError)
        # KeyError isn't in the dispatcher's stdlib mapping, so we get
        # the base class — NOT a ValueError-flavored subclass.
        assert not isinstance(synthesized, ValueError)

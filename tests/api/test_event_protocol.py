"""Stream-protocol contract tests for the long-running facade operations.

Enforces constitution §5.7 / §9.19 invariants on the three long-running
facade entry points:

- :meth:`lhp.api.facade.GenerationFacade.generate_pipelines`
- :meth:`lhp.api.facade.ValidationFacade.validate_pipelines`
- :meth:`lhp.api.facade.BundleFacade.sync_resources`

Each entry point MUST:

1. Yield exactly one :class:`OperationStarted` first.
2. On success, yield exactly one terminal ``*Completed`` last whose
   ``response`` field is the precise DTO type for that operation.
3. On failure, yield exactly one :class:`ErrorEmitted` and then raise
   the original :class:`LHPError`.
4. Return a fresh generator per call (no shared state across calls).
5. Emit zero :class:`ErrorEmitted` instances on the success path.

Cross-cutting tests cover :func:`lhp.api.collect_response`, the
:class:`OperationCompleted` intermediate-base contract,
pickle round-trip for every event class, and the §9.21 carve-out that
``ErrorEmitted.lhp_error`` is the only DTO field permitted to carry a
live exception type.

Tests import strictly from :mod:`lhp.api` and :mod:`lhp.errors` — no
internal modules — so the suite doubles as a guard against the
public-surface contract leaking implementation details.
"""

from __future__ import annotations

import dataclasses
import os
import pickle
import shutil
from pathlib import Path
from typing import Iterator, get_type_hints

import pytest
import yaml

from lhp.api import (
    BatchGenerationResponse,
    BatchValidationResponse,
    BundleSyncCompleted,
    BundleSyncResult,
    ErrorEmitted,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    LHPEvent,
    OperationCompleted,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    ValidationCompleted,
    WarningEmitted,
    collect_response,
)
from lhp.errors.categories import ErrorCategory
from lhp.errors.types import LHPError
from lhp.models import FlowGroup

_FIXTURE_PATH = Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"


@pytest.fixture
def project_facade(tmp_path: Path):
    """Construct a real facade over an isolated copy of the e2e fixture.

    The fixture is deep-copied into ``tmp_path`` per test so concurrent
    runs (and accidental in-test writes) don't pollute the source tree.
    The CWD is temporarily swapped to the project root because LHP
    resolves relative paths off ``Path.cwd()`` in several places.
    """
    project_root = tmp_path / "test_project"
    shutil.copytree(_FIXTURE_PATH, project_root)
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(project_root)
        yield facade, project_root
    finally:
        os.chdir(original_cwd)


@pytest.fixture
def output_dir(tmp_path: Path) -> Path:
    """Empty output directory for generation / bundle sync."""
    out = tmp_path / "generated_dev"
    out.mkdir(parents=True, exist_ok=True)
    return out


def _injected_error() -> LHPError:
    """Construct an LHPError suitable for failure-path injection."""
    return LHPError(
        category=ErrorCategory.GENERAL,
        code_number="999",
        title="injected",
        details="injected test failure for stream-protocol contract",
        context={"injection_site": "test_event_protocol"},
    )


@pytest.mark.unit
class TestValidatePipelinesProtocol:
    """Stream-protocol invariants for ``ValidationFacade.validate_pipelines``."""

    def test_validate_pipelines_yields_operation_started_first(self, project_facade):
        facade, _ = project_facade
        events = list(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "validate_pipelines"
        assert events[0].env == "dev"

    def test_validate_pipelines_yields_completed_last_on_success(self, project_facade):
        facade, _ = project_facade
        events = list(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert isinstance(events[-1], ValidationCompleted)
        assert isinstance(events[-1].response, BatchValidationResponse)
        assert isinstance(events[-1], OperationCompleted)

    def test_validate_pipelines_emits_error_then_raises_on_lhp_error(
        self, project_facade, mocker
    ):
        facade, _ = project_facade
        injected = _injected_error()
        mocker.patch.object(
            facade.validation,
            "_consume_validate_stream",
            side_effect=injected,
        )
        collected: list[LHPEvent] = []
        gen = facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value is injected
        assert exc_info.value.code == "LHP-GEN-999"
        assert isinstance(collected[-1], ErrorEmitted)
        assert collected[-1].lhp_error is injected

    def test_validate_pipelines_returns_fresh_generator_per_call(self, project_facade):
        facade, _ = project_facade
        gen_a = facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        gen_b = facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        assert gen_a is not gen_b
        events_a = list(gen_a)
        events_b = list(gen_b)
        assert isinstance(events_a[0], OperationStarted)
        assert isinstance(events_a[-1], ValidationCompleted)
        assert isinstance(events_b[0], OperationStarted)
        assert isinstance(events_b[-1], ValidationCompleted)

    def test_validate_pipelines_no_error_emitted_on_success(self, project_facade):
        facade, _ = project_facade
        events = list(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert not any(isinstance(e, ErrorEmitted) for e in events)


@pytest.mark.unit
class TestGeneratePipelinesProtocol:
    """Stream-protocol invariants for ``GenerationFacade.generate_pipelines``."""

    def test_generate_pipelines_yields_operation_started_first(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=(), env="dev", output_dir=output_dir
            )
        )
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "generate_pipelines"
        assert events[0].env == "dev"

    def test_generate_pipelines_yields_completed_last_on_success(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=(), env="dev", output_dir=output_dir
            )
        )
        assert isinstance(events[-1], GenerationCompleted)
        assert isinstance(events[-1].response, BatchGenerationResponse)
        assert isinstance(events[-1], OperationCompleted)

    def test_generate_pipelines_emits_error_then_raises_on_lhp_error(
        self, project_facade, output_dir, mocker
    ):
        """An ``LHPError`` from the generate engine surfaces as ``ErrorEmitted``
        then a raise (the §1.4 rendezvous).

        The rendezvous lives inside ``_consume_generate_stream`` (the
        generate-phase body), which consumes the orchestrator's delta-generator
        and, on an ``LHPError``, emits exactly one ``ErrorEmitted`` then
        re-raises. The error is therefore injected at the delta SOURCE —
        ``orchestrator.generate_pipelines``, the generator the facade drains.
        """
        facade, _ = project_facade
        injected = _injected_error()

        def _raising_delta_stream(*_args, **_kwargs):
            raise injected
            yield  # pragma: no cover - makes this a generator function

        mocker.patch.object(
            facade.generation._orchestrator,
            "generate_pipelines",
            side_effect=_raising_delta_stream,
        )
        collected: list[LHPEvent] = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=(), env="dev", output_dir=output_dir
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value is injected
        assert exc_info.value.code == "LHP-GEN-999"
        assert isinstance(collected[-1], ErrorEmitted)
        assert collected[-1].lhp_error is injected
        # No terminal GenerationCompleted on the failure path.
        assert not any(isinstance(e, GenerationCompleted) for e in collected)

    def test_generate_preflight_failure_emits_one_error_then_raises(
        self, project_facade, output_dir
    ):
        """A project-level preflight failure (§9.24) must surface on the
        generate stream as exactly ONE ``ErrorEmitted`` followed by a raise.

        The failure is a genuine duplicate ``(pipeline, flowgroup)`` fed
        through ``pre_discovered_all_flowgroups`` so the REAL
        ``_run_project_preflight`` runs end-to-end (duplicate check ->
        ``LHP-VAL-009``). This also pins the placement contract: the preflight
        surfacing lives in the OUTER ``generate_pipelines`` generator, BEFORE
        the ``_consume_generate_stream`` generate-phase body whose
        ``except Exception -> return failure`` arm would otherwise SWALLOW the
        raise — if mis-placed there, no ``ErrorEmitted`` would be observed.
        """
        facade, _ = project_facade
        duplicate_flowgroups = [
            FlowGroup(pipeline="dup_pipeline", flowgroup="dup_fg"),
            FlowGroup(pipeline="dup_pipeline", flowgroup="dup_fg"),
        ]
        collected: list[LHPEvent] = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=(),
            env="dev",
            output_dir=output_dir,
            pre_discovered_all_flowgroups=duplicate_flowgroups,
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)

        # Exactly one ErrorEmitted, and it precedes the raise (it is the
        # last event the generator yielded before propagating the error).
        error_events = [e for e in collected if isinstance(e, ErrorEmitted)]
        assert len(error_events) == 1
        assert isinstance(collected[-1], ErrorEmitted)
        # No terminal GenerationCompleted on the failure path.
        assert not any(isinstance(e, GenerationCompleted) for e in collected)
        # The duplicate preflight reconstructs an LHP-VAL-009 error, and the
        # same instance is carried on the emitted event then raised.
        assert exc_info.value.code == "LHP-VAL-009"
        assert collected[-1].lhp_error is exc_info.value

    def test_generate_pipelines_returns_fresh_generator_per_call(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        gen_a = facade.generation.generate_pipelines(
            pipeline_fields=(), env="dev", output_dir=output_dir
        )
        gen_b = facade.generation.generate_pipelines(
            pipeline_fields=(), env="dev", output_dir=output_dir
        )
        assert gen_a is not gen_b
        events_a = list(gen_a)
        events_b = list(gen_b)
        assert isinstance(events_a[0], OperationStarted)
        assert isinstance(events_a[-1], GenerationCompleted)
        assert isinstance(events_b[0], OperationStarted)
        assert isinstance(events_b[-1], GenerationCompleted)

    def test_generate_pipelines_no_error_emitted_on_success(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=(), env="dev", output_dir=output_dir
            )
        )
        assert not any(isinstance(e, ErrorEmitted) for e in events)


@pytest.mark.unit
class TestSyncResourcesProtocol:
    """Stream-protocol invariants for ``BundleFacade.sync_resources``."""

    def test_sync_resources_yields_operation_started_first(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(facade.bundle.sync_resources("dev", output_dir))
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "sync_resources"
        assert events[0].env == "dev"

    def test_sync_resources_yields_completed_last_on_success(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(facade.bundle.sync_resources("dev", output_dir))
        assert isinstance(events[-1], BundleSyncCompleted)
        assert isinstance(events[-1].response, BundleSyncResult)
        assert isinstance(events[-1], OperationCompleted)

    def test_sync_resources_emits_error_then_raises_on_lhp_error(
        self, project_facade, output_dir, mocker
    ):
        facade, _ = project_facade
        injected = _injected_error()
        mocker.patch.object(
            facade.bundle,
            "_do_sync_resources",
            side_effect=injected,
        )
        collected: list[LHPEvent] = []
        gen = facade.bundle.sync_resources("dev", output_dir)
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value is injected
        assert exc_info.value.code == "LHP-GEN-999"
        assert isinstance(collected[-1], ErrorEmitted)
        assert collected[-1].lhp_error is injected

    def test_sync_resources_returns_fresh_generator_per_call(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        gen_a = facade.bundle.sync_resources("dev", output_dir)
        gen_b = facade.bundle.sync_resources("dev", output_dir)
        assert gen_a is not gen_b
        events_a = list(gen_a)
        events_b = list(gen_b)
        assert isinstance(events_a[0], OperationStarted)
        assert isinstance(events_a[-1], BundleSyncCompleted)
        assert isinstance(events_b[0], OperationStarted)
        assert isinstance(events_b[-1], BundleSyncCompleted)

    def test_sync_resources_no_error_emitted_on_success(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        events = list(facade.bundle.sync_resources("dev", output_dir))
        assert not any(isinstance(e, ErrorEmitted) for e in events)


@pytest.mark.unit
class TestCollectResponse:
    """``collect_response`` walks a stream and returns the terminal DTO."""

    def test_collect_response_returns_terminal_response_for_validate(
        self, project_facade
    ):
        facade, _ = project_facade
        response = collect_response(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert isinstance(response, BatchValidationResponse)

    def test_collect_response_returns_terminal_response_for_generate(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        response = collect_response(
            facade.generation.generate_pipelines(
                pipeline_fields=(), env="dev", output_dir=output_dir
            )
        )
        assert isinstance(response, BatchGenerationResponse)

    def test_collect_response_returns_terminal_response_for_sync(
        self, project_facade, output_dir
    ):
        facade, _ = project_facade
        response = collect_response(facade.bundle.sync_resources("dev", output_dir))
        assert isinstance(response, BundleSyncResult)

    def test_collect_response_reraises_on_lhp_error(self):
        """When the underlying generator raises after ErrorEmitted, the raise
        propagates from ``collect_response`` — it does NOT swallow it."""
        injected = _injected_error()

        def _failing_stream() -> Iterator[LHPEvent]:
            yield OperationStarted(operation_name="injected", env="dev")
            yield ErrorEmitted(lhp_error=injected)
            raise injected

        with pytest.raises(LHPError) as exc_info:
            collect_response(_failing_stream())
        assert exc_info.value is injected

    def test_collect_response_raises_on_empty_completed(self):
        """A stream that ends without a terminal Completed event is a
        contract violation; ``collect_response`` raises ``RuntimeError``."""

        def _incomplete_stream() -> Iterator[LHPEvent]:
            yield OperationStarted(operation_name="incomplete", env="dev")

        with pytest.raises(RuntimeError, match="terminal Completed event"):
            collect_response(_incomplete_stream())

    @staticmethod
    def _terminal_response() -> BatchValidationResponse:
        """A valid terminal DTO reused by the warnings_sink tests."""
        return BatchValidationResponse(
            success=True,
            pipeline_responses={},
            total_errors=0,
            total_warnings=0,
            validated_pipelines=(),
        )

    def test_collect_response_warnings_sink_captures_in_arrival_order(self):
        """Two ``WarningEmitted`` interleaved among other events are appended
        to the supplied sink in arrival order, and the terminal response is
        still returned correctly."""
        response = self._terminal_response()
        first = WarningEmitted("first warning", code="LHP-W-001")
        second = WarningEmitted("second warning", code="LHP-W-002")

        def _stream() -> Iterator[LHPEvent]:
            yield OperationStarted(operation_name="warn", env="dev")
            yield first
            yield OperationStarted(operation_name="progress", env="dev")
            yield second
            yield ValidationCompleted(response=response)

        sink: list[WarningEmitted] = []
        result = collect_response(_stream(), warnings_sink=sink)

        assert sink == [first, second]
        assert result is response

    def test_collect_response_warnings_sink_default_is_byte_identical(self):
        """Omitting the sink (or passing ``None``) preserves prior behavior:
        the return value matches the equivalent no-sink call and nothing
        raises."""
        response = self._terminal_response()

        def _stream() -> Iterator[LHPEvent]:
            yield OperationStarted(operation_name="warn", env="dev")
            yield WarningEmitted("ignored warning", code="LHP-W-003")
            yield ValidationCompleted(response=response)

        # Default (no kwarg) and explicit None both behave like the prior
        # warnings-discarding implementation.
        assert collect_response(_stream()) is response
        assert collect_response(_stream(), warnings_sink=None) is response

    def test_collect_response_warnings_sink_appends_to_prepopulated(self):
        """A caller-supplied, pre-populated sink has new warnings appended;
        existing entries are preserved, not replaced."""
        response = self._terminal_response()
        preexisting = WarningEmitted("preexisting warning", code="LHP-W-000")
        new_warning = WarningEmitted("new warning", code="LHP-W-004")

        def _stream() -> Iterator[LHPEvent]:
            yield OperationStarted(operation_name="warn", env="dev")
            yield new_warning
            yield ValidationCompleted(response=response)

        sink: list[WarningEmitted] = [preexisting]
        result = collect_response(_stream(), warnings_sink=sink)

        assert sink == [preexisting, new_warning]
        assert result is response


@pytest.mark.unit
class TestEventHierarchy:
    """Class-hierarchy invariants for the stream protocol."""

    def test_lhp_event_is_marker_only(self):
        """``LHPEvent`` declares zero dataclass fields — it is a marker."""
        assert len(dataclasses.fields(LHPEvent)) == 0

    def test_operation_completed_is_intermediate_base(self):
        """Every ``*Completed`` is an ``OperationCompleted`` and an ``LHPEvent``;
        ``OperationStarted`` and ``ErrorEmitted`` are ``LHPEvent`` but NOT
        ``OperationCompleted``."""
        completed_event = ValidationCompleted(
            response=BatchValidationResponse(
                success=True,
                pipeline_responses={},
                total_errors=0,
                total_warnings=0,
                validated_pipelines=(),
            )
        )
        assert isinstance(completed_event, OperationCompleted)
        assert isinstance(completed_event, LHPEvent)

        # GenerationCompleted
        gen_event = GenerationCompleted(
            response=BatchGenerationResponse(
                success=True,
                pipeline_responses={},
                total_files_written=0,
                aggregate_generated_filenames=(),
                output_location=None,
            )
        )
        assert isinstance(gen_event, OperationCompleted)
        assert isinstance(gen_event, LHPEvent)

        # BundleSyncCompleted
        sync_event = BundleSyncCompleted(
            response=BundleSyncResult(
                success=True,
                synced_file_count=0,
                deleted_file_count=0,
                bundle_path=None,
            )
        )
        assert isinstance(sync_event, OperationCompleted)
        assert isinstance(sync_event, LHPEvent)

        started = OperationStarted(operation_name="probe", env="dev")
        assert isinstance(started, LHPEvent)
        assert not isinstance(started, OperationCompleted)

        err = ErrorEmitted(lhp_error=_injected_error())
        assert isinstance(err, LHPEvent)
        assert not isinstance(err, OperationCompleted)


@pytest.mark.unit
class TestEventPickleRoundTrip:
    """All event classes survive ``pickle`` round-trip with ``==``."""

    def test_event_pickle_round_trip(self):
        lhp_event = LHPEvent()
        assert pickle.loads(pickle.dumps(lhp_event)) == lhp_event

        started = OperationStarted(operation_name="validate_pipelines", env="dev")
        assert pickle.loads(pickle.dumps(started)) == started

        gen_resp = BatchGenerationResponse(
            success=True,
            pipeline_responses={},
            total_files_written=0,
            aggregate_generated_filenames=(),
            output_location=None,
        )
        gen_event = GenerationCompleted(response=gen_resp)
        restored_gen = pickle.loads(pickle.dumps(gen_event))
        assert restored_gen == gen_event
        assert restored_gen.response == gen_resp

        val_resp = BatchValidationResponse(
            success=True,
            pipeline_responses={},
            total_errors=0,
            total_warnings=0,
            validated_pipelines=(),
        )
        val_event = ValidationCompleted(response=val_resp)
        restored_val = pickle.loads(pickle.dumps(val_event))
        assert restored_val == val_event
        assert restored_val.response == val_resp

        sync_resp = BundleSyncResult(
            success=True,
            synced_file_count=3,
            deleted_file_count=1,
            bundle_path=Path("/tmp/resources/lhp"),
        )
        sync_event = BundleSyncCompleted(response=sync_resp)
        restored_sync = pickle.loads(pickle.dumps(sync_event))
        assert restored_sync == sync_event
        assert restored_sync.response == sync_resp

        # LHPError fields survive pickle (code/category/details)
        injected = LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="042",
            title="picklable injection",
            details="LHPError round-trip",
            context={"pipeline": "bronze"},
            suggestions=["fix it", "re-run"],
        )
        err_event = ErrorEmitted(lhp_error=injected)
        restored_err = pickle.loads(pickle.dumps(err_event))
        assert restored_err.lhp_error.code == "LHP-VAL-042"
        assert restored_err.lhp_error.category == ErrorCategory.VALIDATION
        assert restored_err.lhp_error.details == "LHPError round-trip"
        assert restored_err.lhp_error.context == {"pipeline": "bronze"}


@pytest.mark.unit
class TestEventFieldTypeDiscipline:
    """Event field annotations comply with §4.8 (with §9.21 carve-out)."""

    _FORBIDDEN_FRAGMENTS = ("Any", "Exception", "Dict[", "List[")

    @pytest.mark.parametrize(
        "cls",
        [
            LHPEvent,
            OperationStarted,
            OperationCompleted,
            GenerationCompleted,
            ValidationCompleted,
            BundleSyncCompleted,
            ErrorEmitted,
        ],
    )
    def test_event_field_types_are_constitutional(self, cls):
        """No field annotation contains ``Any``, ``Exception``, ``Dict[``,
        or ``List[`` — except ``ErrorEmitted.lhp_error: LHPError`` which is
        the §9.21-permitted carve-out for the failure-rendezvous protocol."""
        hints = get_type_hints(
            cls,
            localns={
                "BatchGenerationResponse": BatchGenerationResponse,
                "BatchValidationResponse": BatchValidationResponse,
                "BundleSyncResult": BundleSyncResult,
                "LHPError": LHPError,
            },
        )
        for field_name, annotation in hints.items():
            # The §9.21 carve-out: LHPError is allowed on ErrorEmitted.
            if cls is ErrorEmitted and field_name == "lhp_error":
                assert annotation is LHPError
                continue
            rendered = repr(annotation)
            for fragment in self._FORBIDDEN_FRAGMENTS:
                assert fragment not in rendered, (
                    f"{cls.__name__}.{field_name} has forbidden type "
                    f"fragment '{fragment}' in annotation {rendered!r}"
                )


# All-or-nothing failure-path event semantics (no-dangling-Starts rule)
#
# These tests pin the RATIFIED no-dangling-Starts contract on the two
# multi-pipeline facade entry points (generate / validate). They drive REAL
# projects through the facade (no hand-built event lists, no mocks of the
# emission path) so the suite actually exercises the §5.7 emission contract.
#
# THE RULE (load-bearing, asserted on EVERY path below):
#
#     For each pipeline in input order, a ``PipelineStarted`` is emitted only
#     once its terminal (``PipelineCompleted`` xor ``PipelineFailed``) is in
#     hand. A Started is therefore NEVER dangling — it is always immediately
#     followed by its own terminal — and the global count equality
#
#         count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)
#
#     holds on BOTH the success path AND the gate-abort path (counting only
#     what was emitted before the §1.4 raise closes the stream).
#
#   * Generate abort path: on gate failure, ``PipelineFailed`` is emitted for
#     each failing pipeline (input order), then exactly one ``ErrorEmitted``,
#     then the stream raises. Pipelines in the aborted batch that did NOT fail
#     emit NO per-pipeline events. The count equality still holds for what was
#     emitted.
#   * Validate path: REPORTS (no abort / no raise) — every pipeline gets a
#     Started+terminal pair (``PipelineFailed`` if it has errors, else
#     ``PipelineCompleted``); the count equality holds with full equality
#     across the whole batch.


def _e5_write_pipeline(
    project_root: Path, pipeline: str, table: str, *, dup: bool
) -> None:
    """Write one flowgroup into ``project_root``.

    When ``dup`` it declares two ``create_table`` writes to the SAME table — a
    genuine per-flowgroup ``LHP-CFG-004`` that the generate gate aggregates
    into an all-or-nothing abort and that validate REPORTS as a per-pipeline
    finding. The failure is engine-produced, not injected.
    """
    pdir = project_root / "pipelines" / pipeline
    pdir.mkdir(parents=True, exist_ok=True)
    actions = [
        {
            "name": f"load_{pipeline}",
            "type": "load",
            "target": f"v_{pipeline}_raw",
            "source": {
                "type": "cloudfiles",
                "path": "${landing_path}/" + pipeline,
                "format": "json",
            },
        },
        {
            "name": f"write_{pipeline}_a",
            "type": "write",
            "source": f"v_{pipeline}_raw",
            "write_target": {
                "type": "streaming_table",
                "catalog": "${catalog}",
                "schema": "${bronze_schema}",
                "table": table,
                "create_table": True,
            },
        },
    ]
    if dup:
        actions.append(
            {
                "name": f"write_{pipeline}_b",
                "type": "write",
                "source": f"v_{pipeline}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            }
        )
    with open(pdir / f"{pipeline}_fg.yaml", "w") as f:
        yaml.dump(
            {"pipeline": pipeline, "flowgroup": f"{pipeline}_fg", "actions": actions}, f
        )


def _e5_project(tmp_path: Path, pipelines: dict[str, bool]) -> Path:
    """Write a minimal project. ``pipelines`` maps pipeline name → ``dup`` flag."""
    project_root = tmp_path / "e5_proj"
    for sub in ("presets", "templates", "substitutions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)
    (project_root / "lhp.yaml").write_text("name: e5_event_protocol\nversion: '1.0'\n")
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_catalog",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/dev/landing",
                }
            },
            f,
        )
    for name, dup in pipelines.items():
        _e5_write_pipeline(project_root, name, f"{name}_table", dup=dup)
    return project_root


@pytest.fixture
def e5_facade_in(tmp_path: Path):
    """Return a builder that writes a project and yields ``(facade, output_dir)``.

    A real project on disk, the CWD swapped to its root (LHP resolves relative
    paths off ``Path.cwd()``), and a real facade with version enforcement
    disabled (the minimal project has no pinned LHP version).
    """
    created: list[str] = []

    def _build(pipelines: dict[str, bool]):
        project_root = _e5_project(tmp_path, pipelines)
        output_dir = project_root / "generated" / "dev"
        created.append(os.getcwd())
        os.chdir(project_root)
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        return facade, output_dir

    yield _build
    if created:
        os.chdir(created[0])


def _pipeline_count_equality_holds(events: list[LHPEvent]) -> bool:
    """The load-bearing no-dangling-Starts invariant, as a predicate:
    ``count(PipelineStarted) == count(PipelineCompleted) + count(PipelineFailed)``.
    """
    starts = sum(1 for e in events if isinstance(e, PipelineStarted))
    completed = sum(1 for e in events if isinstance(e, PipelineCompleted))
    failed = sum(1 for e in events if isinstance(e, PipelineFailed))
    return starts == completed + failed


def _assert_each_start_immediately_paired(events: list[LHPEvent]) -> None:
    """Every ``PipelineStarted`` is IMMEDIATELY followed by exactly one terminal
    for the SAME pipeline — the structural proof there are no dangling Starts.
    """
    i = 0
    while i < len(events):
        ev = events[i]
        if isinstance(ev, PipelineStarted):
            assert i + 1 < len(events), (
                f"dangling PipelineStarted({ev.pipeline}) — no following event"
            )
            terminal = events[i + 1]
            assert isinstance(terminal, (PipelineCompleted, PipelineFailed)), (
                f"PipelineStarted({ev.pipeline}) not immediately followed by a "
                f"terminal; got {type(terminal).__name__}"
            )
            assert terminal.pipeline == ev.pipeline, (
                f"PipelineStarted({ev.pipeline}) paired with terminal for a "
                f"different pipeline ({terminal.pipeline})"
            )
            i += 2
            continue
        i += 1


@pytest.mark.integration
class TestGenerateNoDanglingStartsSuccessPath:
    """Generate SUCCESS path: every Started is paired with a Completed and the
    count equality ``Started == Completed + Failed`` holds (no dangling Starts).
    """

    def test_count_equality_and_pairing_on_success(self, e5_facade_in):
        """No-dangling-Starts rule on the success path.

        Drains a real two-pipeline generate stream to completion and asserts:
        the count equality holds, every ``PipelineStarted(p)`` is immediately
        followed by its own terminal, both terminals are ``PipelineCompleted``,
        and the terminal ``GenerationCompleted`` is LAST.
        """
        facade, output_dir = e5_facade_in({"alpha": False, "beta": False})

        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["alpha", "beta"],
                env="dev",
                output_dir=output_dir,
            )
        )

        # OperationStarted first.
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "generate_pipelines"

        # Load-bearing count equality — no dangling Starts.
        starts = [e for e in events if isinstance(e, PipelineStarted)]
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        failed = [e for e in events if isinstance(e, PipelineFailed)]
        assert len(starts) == len(completed) + len(failed)
        assert _pipeline_count_equality_holds(events)
        # Both pipelines started and BOTH succeeded.
        assert len(starts) == 2
        assert not failed
        assert sorted(e.pipeline for e in completed) == ["alpha", "beta"]

        # Structural pairing: each Start immediately precedes its own terminal.
        _assert_each_start_immediately_paired(events)

        # Terminal GenerationCompleted is LAST (an OperationCompleted subclass).
        assert isinstance(events[-1], GenerationCompleted)
        assert isinstance(events[-1], OperationCompleted)
        assert events[-1].response.success is True

    def test_every_started_directly_precedes_same_pipeline_terminal(self, e5_facade_in):
        """The per-pipeline subsequence is strictly ``[Started(p), Completed(p)]``
        repeated — a Started is emitted only once its terminal is in hand, so it
        can never be observed without its immediately following terminal."""
        facade, output_dir = e5_facade_in({"alpha": False, "beta": False})
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["alpha", "beta"],
                env="dev",
                output_dir=output_dir,
            )
        )
        per_pipeline = [
            e
            for e in events
            if isinstance(e, (PipelineStarted, PipelineCompleted, PipelineFailed))
        ]
        assert len(per_pipeline) == 4  # 2 Starts + 2 terminals, no danglers
        for start, terminal in zip(
            per_pipeline[0::2], per_pipeline[1::2], strict=False
        ):
            assert isinstance(start, PipelineStarted)
            assert isinstance(terminal, PipelineCompleted)
            assert start.pipeline == terminal.pipeline


@pytest.mark.integration
class TestGenerateNoDanglingStartsAbortPath:
    """Generate ABORT path: the failing pipeline emits Started+Failed (input
    order), then exactly one ``ErrorEmitted``, then the stream raises; the
    non-failing pipeline in the aborted batch emits ZERO per-pipeline events;
    the count equality STILL holds for what was emitted before the raise.
    """

    def test_abort_path_no_dangling_starts_and_count_equality(self, e5_facade_in):
        """No-dangling-Starts rule on the gate-abort path (the exhaustive proof).

        The batch is ``[clean_p, bad_p]`` in input order; ``bad_p`` carries a
        duplicate-table ``LHP-CFG-004`` the generate gate aggregates into an
        all-or-nothing abort. After draining until the §1.4 raise:

        * ``bad_p`` emitted ``PipelineStarted`` + ``PipelineFailed`` (paired);
        * ``clean_p`` (the non-failing pipeline in the aborted batch) emitted
          ZERO per-pipeline events;
        * exactly ONE ``ErrorEmitted`` was emitted, and it is the LAST event
          before the raise;
        * the stream raised (§1.4 closes it) with no ``GenerationCompleted``;
        * the count equality ``Started == Completed + Failed`` STILL holds for
          everything emitted before the raise.
        """
        # clean_p ordered BEFORE bad_p so we also pin input-order behaviour.
        facade, output_dir = e5_facade_in({"clean_p": False, "bad_p": True})

        collected: list[LHPEvent] = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=["clean_p", "bad_p"],
            env="dev",
            output_dir=output_dir,
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value.code == "LHP-CFG-004"

        # OperationStarted is still the first event.
        assert isinstance(collected[0], OperationStarted)

        # Load-bearing count equality holds for what WAS emitted before the raise.
        assert _pipeline_count_equality_holds(collected)
        _assert_each_start_immediately_paired(collected)

        # Only the failing pipeline produced per-pipeline events; the non-failing
        # pipeline in the aborted batch emitted NOTHING.
        starts = [e for e in collected if isinstance(e, PipelineStarted)]
        failed = [e for e in collected if isinstance(e, PipelineFailed)]
        assert [e.pipeline for e in starts] == ["bad_p"]
        assert [e.pipeline for e in failed] == ["bad_p"]
        assert failed[0].code == "LHP-CFG-004"
        assert not any(isinstance(e, PipelineCompleted) for e in collected)

        # Exactly one ErrorEmitted, and it is the trailing event before the raise.
        error_events = [e for e in collected if isinstance(e, ErrorEmitted)]
        assert len(error_events) == 1
        assert isinstance(collected[-1], ErrorEmitted)
        assert collected[-1].lhp_error is exc_info.value

        # The §1.4 raise closed the stream: no terminal GenerationCompleted.
        assert not any(isinstance(e, GenerationCompleted) for e in collected)

    def test_abort_path_failed_events_in_input_order(self, e5_facade_in):
        """When MULTIPLE pipelines fail the gate, their ``PipelineFailed`` events
        appear in INPUT order, each paired with its own ``PipelineStarted``, and
        the count equality holds across all of them (still no dangling Starts).
        The lone clean pipeline emits nothing."""
        # Two failing pipelines (bad_a, bad_c) bracketing a clean one (mid_b);
        # input order is bad_a → mid_b → bad_c.
        facade, output_dir = e5_facade_in(
            {"bad_a": True, "mid_b": False, "bad_c": True}
        )

        collected: list[LHPEvent] = []
        gen = facade.generation.generate_pipelines(
            pipeline_fields=["bad_a", "mid_b", "bad_c"],
            env="dev",
            output_dir=output_dir,
        )
        with pytest.raises(LHPError):
            for event in gen:
                collected.append(event)

        # No dangling Starts, count equality, and Failed events in input order.
        assert _pipeline_count_equality_holds(collected)
        _assert_each_start_immediately_paired(collected)
        failed = [e for e in collected if isinstance(e, PipelineFailed)]
        assert [e.pipeline for e in failed] == ["bad_a", "bad_c"]
        # The clean pipeline in the aborted batch emitted no per-pipeline events.
        starts = [e for e in collected if isinstance(e, PipelineStarted)]
        assert [e.pipeline for e in starts] == ["bad_a", "bad_c"]
        assert not any(isinstance(e, PipelineCompleted) for e in collected)
        # Exactly one rendezvous ErrorEmitted, last before the raise.
        assert sum(1 for e in collected if isinstance(e, ErrorEmitted)) == 1
        assert isinstance(collected[-1], ErrorEmitted)


@pytest.mark.integration
class TestValidateNoDanglingStartsReportPath:
    """Validate path REPORTS (no abort, no raise): every pipeline pairs
    (Started+terminal), the count equality holds with FULL equality across the
    batch, and the terminal ``ValidationCompleted`` is LAST.
    """

    def test_mixed_batch_every_pipeline_pairs_no_raise(self, e5_facade_in):
        """No-dangling-Starts rule on the validate report path.

        A mixed clean/error batch ``[ok_p, bad_p]``: validate has no
        all-or-nothing gate, so it REPORTS — EVERY pipeline yields a
        Started+terminal pair (``PipelineFailed`` for ``bad_p``,
        ``PipelineCompleted`` for ``ok_p``). The count equality holds with full
        equality (unlike generate's abort path, which suppresses the
        non-failing pipeline). The terminal ``ValidationCompleted`` is LAST and
        carries a ``success=False`` batch — REPORTED, not raised.
        """
        facade, _ = e5_facade_in({"ok_p": False, "bad_p": True})

        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["ok_p", "bad_p"],
                env="dev",
            )
        )

        # OperationStarted first.
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "validate_pipelines"

        # FULL count equality across the whole batch (validate REPORTS): both
        # pipelines pair, so Started == Completed + Failed with EVERY pipeline
        # contributing — no dangling Starts, and (unlike generate's abort) no
        # suppressed pipeline.
        starts = [e for e in events if isinstance(e, PipelineStarted)]
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        failed = [e for e in events if isinstance(e, PipelineFailed)]
        assert _pipeline_count_equality_holds(events)
        assert len(starts) == 2
        assert len(completed) + len(failed) == 2
        _assert_each_start_immediately_paired(events)

        assert sorted(e.pipeline for e in starts) == ["bad_p", "ok_p"]
        assert [e.pipeline for e in completed] == ["ok_p"]
        assert [e.pipeline for e in failed] == ["bad_p"]
        assert failed[0].code.startswith("LHP-")
        assert failed[0].message

        # No rendezvous ErrorEmitted (validate REPORTS, never raises on findings).
        assert not any(isinstance(e, ErrorEmitted) for e in events)

        # Terminal ValidationCompleted is LAST, carrying a reported failure batch.
        assert isinstance(events[-1], ValidationCompleted)
        assert isinstance(events[-1], OperationCompleted)
        assert isinstance(events[-1].response, BatchValidationResponse)
        assert events[-1].response.success is False

    def test_all_clean_batch_pairs_with_completed(self, e5_facade_in):
        """An all-clean validate batch pairs every Started with a Completed (no
        Failed, no raise) and the count equality holds with full equality."""
        facade, _ = e5_facade_in({"one": False, "two": False})
        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["one", "two"], env="dev"
            )
        )
        assert _pipeline_count_equality_holds(events)
        _assert_each_start_immediately_paired(events)
        completed = [e for e in events if isinstance(e, PipelineCompleted)]
        assert sorted(e.pipeline for e in completed) == ["one", "two"]
        assert not any(isinstance(e, PipelineFailed) for e in events)
        assert isinstance(events[-1], ValidationCompleted)
        assert events[-1].response.success is True


@pytest.mark.integration
class TestProgressStreamFraming:
    """Cross-path framing: ``OperationStarted`` first, phases paired
    (``PhaseStarted``/``PhaseCompleted`` of the same phase), and the terminal
    ``OperationCompleted`` subclass last — on both generate and validate.
    """

    def test_generate_operation_started_first_phases_paired_completed_last(
        self, e5_facade_in
    ):
        facade, output_dir = e5_facade_in({"alpha": False, "beta": False})
        events = list(
            facade.generation.generate_pipelines(
                pipeline_fields=["alpha", "beta"],
                env="dev",
                output_dir=output_dir,
            )
        )
        # OperationStarted is the unique first event.
        assert isinstance(events[0], OperationStarted)
        assert sum(1 for e in events if isinstance(e, OperationStarted)) == 1

        # Phases pair up: the PhaseStarted sequence equals the PhaseCompleted
        # sequence. The full generate orchestration consolidates discover →
        # preflight → generate → format → monitoring into the one stream. This
        # is a DEFAULT run (``apply_formatting`` unset → resolves to the
        # project's ``lhp.yaml`` setting, ``True`` when absent), so the
        # ``format`` phase runs between generate and monitoring; the monitoring
        # phase always runs on a successful non-dry-run generate (the absorbed
        # finalize is a no-op when monitoring is not configured, as here).
        # ``bundle_sync`` is absent because this project is not bundle-enabled.
        started_phases = [e.phase for e in events if isinstance(e, PhaseStarted)]
        completed_phases = [e.phase for e in events if isinstance(e, PhaseCompleted)]
        assert started_phases == completed_phases
        assert started_phases == [
            "discover",
            "preflight",
            "generate",
            "format",
            "monitoring",
        ]

        # Exactly one terminal OperationCompleted subclass, and it is LAST.
        terminals = [e for e in events if isinstance(e, OperationCompleted)]
        assert len(terminals) == 1
        assert terminals[0] is events[-1]
        assert isinstance(events[-1], GenerationCompleted)

    def test_validate_operation_started_first_phases_paired_completed_last(
        self, e5_facade_in
    ):
        facade, _ = e5_facade_in({"alpha": False, "beta": False})
        events = list(
            facade.validation.validate_pipelines(
                pipeline_fields=["alpha", "beta"], env="dev"
            )
        )
        assert isinstance(events[0], OperationStarted)
        assert sum(1 for e in events if isinstance(e, OperationStarted)) == 1

        started_phases = [e.phase for e in events if isinstance(e, PhaseStarted)]
        completed_phases = [e.phase for e in events if isinstance(e, PhaseCompleted)]
        assert started_phases == completed_phases
        assert started_phases == ["discover", "preflight", "validate"]

        terminals = [e for e in events if isinstance(e, OperationCompleted)]
        assert len(terminals) == 1
        assert terminals[0] is events[-1]
        assert isinstance(events[-1], ValidationCompleted)

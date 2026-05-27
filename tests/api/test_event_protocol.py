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
:class:`OperationCompleted` intermediate-base contract (C7 Part A),
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

from lhp.api import (
    BatchGenerationResponse,
    BatchValidationResponse,
    BundleSyncResult,
    BundleSyncCompleted,
    ErrorEmitted,
    GenerationCompleted,
    LakehousePlumberApplicationFacade,
    LHPEvent,
    OperationCompleted,
    OperationStarted,
    ValidationCompleted,
    collect_response,
)
from lhp.errors.categories import ErrorCategory
from lhp.errors.types import LHPError


_FIXTURE_PATH = (
    Path(__file__).parent.parent / "e2e" / "fixtures" / "testing_project"
)


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

    def test_validate_pipelines_yields_operation_started_first(
        self, project_facade
    ):
        facade, _ = project_facade
        events = list(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert isinstance(events[0], OperationStarted)
        assert events[0].operation_name == "validate_pipelines"
        assert events[0].env == "dev"

    def test_validate_pipelines_yields_completed_last_on_success(
        self, project_facade
    ):
        facade, _ = project_facade
        events = list(
            facade.validation.validate_pipelines(pipeline_fields=(), env="dev")
        )
        assert isinstance(events[-1], ValidationCompleted)
        assert isinstance(events[-1].response, BatchValidationResponse)
        # Part-A exercise: terminal events are reachable via the
        # OperationCompleted base.
        assert isinstance(events[-1], OperationCompleted)

    def test_validate_pipelines_emits_error_then_raises_on_lhp_error(
        self, project_facade, mocker
    ):
        facade, _ = project_facade
        injected = _injected_error()
        mocker.patch.object(
            facade.validation,
            "_do_validate_pipelines",
            side_effect=injected,
        )
        collected: list[LHPEvent] = []
        gen = facade.validation.validate_pipelines(
            pipeline_fields=(), env="dev"
        )
        with pytest.raises(LHPError) as exc_info:
            for event in gen:
                collected.append(event)
        assert exc_info.value is injected
        assert exc_info.value.code == "LHP-GEN-999"
        assert isinstance(collected[-1], ErrorEmitted)
        assert collected[-1].lhp_error is injected

    def test_validate_pipelines_returns_fresh_generator_per_call(
        self, project_facade
    ):
        facade, _ = project_facade
        gen_a = facade.validation.validate_pipelines(
            pipeline_fields=(), env="dev"
        )
        gen_b = facade.validation.validate_pipelines(
            pipeline_fields=(), env="dev"
        )
        assert gen_a is not gen_b
        events_a = list(gen_a)
        events_b = list(gen_b)
        assert isinstance(events_a[0], OperationStarted)
        assert isinstance(events_a[-1], ValidationCompleted)
        assert isinstance(events_b[0], OperationStarted)
        assert isinstance(events_b[-1], ValidationCompleted)

    def test_validate_pipelines_no_error_emitted_on_success(
        self, project_facade
    ):
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
        facade, _ = project_facade
        injected = _injected_error()
        mocker.patch.object(
            facade.generation,
            "_do_generate_pipelines",
            side_effect=injected,
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
            facade.validation.validate_pipelines(
                pipeline_fields=(), env="dev"
            )
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
        response = collect_response(
            facade.bundle.sync_resources("dev", output_dir)
        )
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

        # OperationStarted is LHPEvent but NOT OperationCompleted
        started = OperationStarted(operation_name="probe", env="dev")
        assert isinstance(started, LHPEvent)
        assert not isinstance(started, OperationCompleted)

        # ErrorEmitted is LHPEvent but NOT OperationCompleted
        err = ErrorEmitted(lhp_error=_injected_error())
        assert isinstance(err, LHPEvent)
        assert not isinstance(err, OperationCompleted)


@pytest.mark.unit
class TestEventPickleRoundTrip:
    """All event classes survive ``pickle`` round-trip with ``==``."""

    def test_event_pickle_round_trip(self):
        # LHPEvent (marker, no fields)
        lhp_event = LHPEvent()
        assert pickle.loads(pickle.dumps(lhp_event)) == lhp_event

        # OperationStarted
        started = OperationStarted(
            operation_name="validate_pipelines", env="dev"
        )
        assert pickle.loads(pickle.dumps(started)) == started

        # GenerationCompleted (with BatchGenerationResponse)
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

        # ValidationCompleted (with BatchValidationResponse)
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

        # BundleSyncCompleted (with BundleSyncResult)
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

        # ErrorEmitted (with LHPError — code/category/details survive)
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

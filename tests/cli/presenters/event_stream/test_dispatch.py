"""Dispatch-loop and ABC fail-fast tests for the event-stream sink."""

from __future__ import annotations

from typing import Iterator, List, Optional, Tuple

import pytest

from lhp.api.events import (
    ErrorEmitted,
    GenerationCompleted,
    LHPEvent,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
)
from lhp.cli.presenters.event_stream._event_dispatch import EventSink, drive


class _StubError:
    """Duck-typed stand-in for an LHPError: exposes only ``.code``.

    Lets the dispatch test exercise ``on_error`` without importing
    ``lhp.errors`` (sole-bridge invariant, constitution §9.5).
    """

    def __init__(self, code: str) -> None:
        self.code = code


class _RecordingSink(EventSink):
    """Records every callback invocation in arrival order."""

    def __init__(self) -> None:
        self.calls: List[Tuple[str, tuple]] = []
        self.terminal_response: object = None

    def on_operation_started(self, operation_name: str, env: Optional[str]) -> None:
        self.calls.append(("operation_started", (operation_name, env)))

    def on_phase_started(self, phase: str) -> None:
        self.calls.append(("phase_started", (phase,)))

    def on_phase_completed(self, phase: str, duration_s: float, success: bool) -> None:
        self.calls.append(("phase_completed", (phase, duration_s, success)))

    def on_pipeline_started(self, pipeline: str) -> None:
        self.calls.append(("pipeline_started", (pipeline,)))

    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        self.calls.append(("pipeline_completed", (pipeline, duration_s, files_written)))

    def on_pipeline_failed(self, pipeline: str, code: str, message: str) -> None:
        self.calls.append(("pipeline_failed", (pipeline, code, message)))

    def on_warning(self, message, code, category, file, flowgroup) -> None:
        self.calls.append(("warning", (message, code, category, file, flowgroup)))

    def on_error(self, code: str) -> None:
        self.calls.append(("error", (code,)))

    def on_terminal(self, response: object) -> None:
        self.calls.append(("terminal", (response,)))
        self.terminal_response = response


def test_drive_dispatches_in_order_and_returns_terminal_response():
    response = object()
    events: List[LHPEvent] = [
        OperationStarted(operation_name="generate", env="dev"),
        PhaseStarted(phase="discovery"),
        PhaseCompleted(phase="discovery", duration_s=0.5, success=True),
        GenerationCompleted(response=response),
    ]
    sink = _RecordingSink()

    returned = drive(iter(events), sink)

    assert [name for name, _ in sink.calls] == [
        "operation_started",
        "phase_started",
        "phase_completed",
        "terminal",
    ]
    assert sink.calls[0][1] == ("generate", "dev")
    assert sink.calls[1][1] == ("discovery",)
    assert sink.calls[2][1] == ("discovery", 0.5, True)
    # on_terminal received the response, and drive returned that same object.
    assert sink.terminal_response is response
    assert returned is response


def test_drive_dispatches_on_error_then_propagates_raise():
    """Generate/plan failure-rendezvous: ErrorEmitted, then the LHPError raises."""

    class _Boom(Exception):
        pass

    def _failing_stream() -> Iterator[LHPEvent]:
        yield OperationStarted(operation_name="generate", env="dev")
        yield ErrorEmitted(lhp_error=_StubError(code="LHP-IO-001"))
        raise _Boom("fatal")

    sink = _RecordingSink()

    with pytest.raises(_Boom):
        drive(_failing_stream(), sink)

    # on_error fired (duck-typed code), and the raise propagated out of drive.
    assert [name for name, _ in sink.calls] == ["operation_started", "error"]
    assert sink.calls[1] == ("error", ("LHP-IO-001",))


def test_event_sink_cannot_be_instantiated_directly():
    with pytest.raises(TypeError):
        EventSink()  # type: ignore[abstract]


def test_event_sink_subclass_missing_callback_cannot_be_instantiated():
    class _Incomplete(EventSink):
        # Implements every callback except on_terminal.
        def on_operation_started(self, operation_name, env):
            pass

        def on_phase_started(self, phase):
            pass

        def on_phase_completed(self, phase, duration_s, success):
            pass

        def on_pipeline_started(self, pipeline):
            pass

        def on_pipeline_completed(self, pipeline, duration_s, files_written):
            pass

        def on_pipeline_failed(self, pipeline, code, message):
            pass

        def on_warning(self, message, code, category, file, flowgroup):
            pass

        def on_error(self, code):
            pass

    with pytest.raises(TypeError):
        _Incomplete()  # type: ignore[abstract]

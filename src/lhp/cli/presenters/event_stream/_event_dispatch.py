"""Event-kind dispatch for the event-stream renderers.

Defines the :class:`EventSink` interface that the live and log renderers
implement, and the :func:`drive` loop that walks an ``Iterator[LHPEvent]``
and fans each event out to the matching sink callback in arrival order.

``EventSink`` is an :class:`abc.ABC` (not ``typing.Protocol``) per
constitution §9.25 / §4.12: it has two implementers (``LiveRenderer`` and
``LogRenderer``), and the ABC gives construction-time fail-fast (§4.12) —
instantiating a subclass that forgets a callback raises ``TypeError``
immediately rather than deferring to first dispatch.

Per the sole-bridge invariant (§9.5) this module never imports
``lhp.errors``: :meth:`EventSink.on_error` reads ``event.lhp_error.code``
duck-typed as a ``str``. ``drive`` does not swallow exceptions — when the
generator raises an ``LHPError`` after yielding ``ErrorEmitted`` (the
generate/plan failure-rendezvous), the raise propagates naturally so the
caller can tear down the live display and re-raise.
"""

from __future__ import annotations

import abc
import logging
from pathlib import Path
from typing import Iterator, Optional

from lhp.api.events import (
    ErrorEmitted,
    LHPEvent,
    OperationCompleted,
    OperationStarted,
    PhaseCompleted,
    PhaseStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineStarted,
    WarningEmitted,
)

logger = logging.getLogger(__name__)


class EventSink(abc.ABC):
    """Sink for the event kinds the renderers react to.

    One ``@abstractmethod`` per handled event kind; :func:`drive`
    dispatches each event to the matching callback in arrival order.
    Implemented by ``LiveRenderer`` and ``LogRenderer`` (≥2 implementers,
    so an ABC over a single concrete class is justified per §9.25).

    Construction fails fast (§4.12): a subclass missing any callback
    cannot be instantiated.
    """

    @abc.abstractmethod
    def begin(self) -> None:
        """Lifecycle hook fired once before the first event is pulled.

        Not an event-kind callback, but abstract for the same fail-fast reason
        (§4.12): the caller invokes it before :func:`drive` so an interactive
        renderer can paint its first frame before the stream's first
        ``next()`` — which may block on discovery — is awaited. The
        non-interactive renderer implements it as a no-op (it paints nothing
        before the stream opens).
        """

    @abc.abstractmethod
    def on_operation_started(self, operation_name: str, env: Optional[str]) -> None:
        """The single :class:`OperationStarted` opening the stream."""

    @abc.abstractmethod
    def on_phase_started(self, phase: str) -> None:
        """A :class:`PhaseStarted` progress event."""

    @abc.abstractmethod
    def on_phase_completed(self, phase: str, duration_s: float, success: bool) -> None:
        """A :class:`PhaseCompleted` progress event."""

    @abc.abstractmethod
    def on_pipeline_started(self, pipeline: str) -> None:
        """A :class:`PipelineStarted` progress event."""

    @abc.abstractmethod
    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        """A :class:`PipelineCompleted` progress event."""

    @abc.abstractmethod
    def on_pipeline_failed(self, pipeline: str, code: str, message: str) -> None:
        """A :class:`PipelineFailed` progress event (run continues)."""

    @abc.abstractmethod
    def on_warning(
        self,
        message: str,
        code: str,
        category: str,
        file: Optional[Path],
        flowgroup: Optional[str],
    ) -> None:
        """A data-only :class:`WarningEmitted` (never halts the stream)."""

    @abc.abstractmethod
    def on_error(self, code: str) -> None:
        """An :class:`ErrorEmitted` carrying the live exception.

        ``code`` is read duck-typed from ``event.lhp_error.code`` by
        :func:`drive`; this module never imports ``lhp.errors`` (§9.5).
        The owning ``LHPError`` raises immediately after this event, so
        :func:`drive` re-raises it out of the loop.
        """

    @abc.abstractmethod
    def on_terminal(self, response: object) -> None:
        """The terminal :class:`OperationCompleted` carrying the response DTO."""


def drive(events: Iterator[LHPEvent], sink: EventSink) -> object:
    """Walk an ``LHPEvent`` stream, dispatching each event to ``sink``.

    Iterates ``events`` once, fanning each event out to the matching
    :class:`EventSink` callback in arrival order. The terminal is
    discovered via ``isinstance(ev, OperationCompleted)`` — mirroring
    :func:`lhp.api.collect_response` — so concrete terminal subclasses
    need not be enumerated; :meth:`EventSink.on_terminal` receives that
    event's ``response`` and the same value is returned after the loop.

    The loop is deliberately not wrapped in a swallowing ``try``/``except``:
    on the generate/plan failure-rendezvous the generator yields
    :class:`ErrorEmitted` (dispatched to :meth:`EventSink.on_error`) and
    then raises the underlying ``LHPError``. That raise propagates out of
    this function so the caller can tear down the live display and re-raise
    on a clean stream. ``drive`` never imports ``lhp.errors``.
    """
    final_response: object = None
    for event in events:
        if isinstance(event, OperationStarted):
            sink.on_operation_started(event.operation_name, event.env)
        elif isinstance(event, PhaseStarted):
            sink.on_phase_started(event.phase)
        elif isinstance(event, PhaseCompleted):
            sink.on_phase_completed(event.phase, event.duration_s, event.success)
        elif isinstance(event, PipelineStarted):
            sink.on_pipeline_started(event.pipeline)
        elif isinstance(event, PipelineCompleted):
            sink.on_pipeline_completed(
                event.pipeline, event.duration_s, event.files_written
            )
        elif isinstance(event, PipelineFailed):
            sink.on_pipeline_failed(event.pipeline, event.code, event.message)
        elif isinstance(event, WarningEmitted):
            sink.on_warning(
                event.message,
                event.code,
                event.category,
                event.file,
                event.flowgroup,
            )
        elif isinstance(event, ErrorEmitted):
            # Duck-typed: read .code without importing lhp.errors (§9.5).
            sink.on_error(event.lhp_error.code)
        elif isinstance(event, OperationCompleted):
            sink.on_terminal(event.response)
            final_response = event.response
        else:
            # A new LHPEvent subtype that no branch above handles. Surface it
            # at WARNING (§7.1) rather than dropping it silently, so an unwired
            # event type is visible in logs and gets a dispatch branch instead
            # of being invisibly lost. Production does not crash on it: the run
            # continues and the event is simply skipped for rendering.
            logger.warning(
                f"drive: unhandled event kind {type(event).__name__} "
                f"has no dispatch branch and was skipped"
            )
    return final_response

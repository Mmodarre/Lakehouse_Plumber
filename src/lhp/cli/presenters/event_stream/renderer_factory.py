"""Renderer selection and the single ``render`` entry point for the stream.

:func:`select_renderer` decides ONCE, on ``console.is_terminal`` (overridable
by the ``no_progress`` flag or the ``CI`` / ``LHP_NO_PROGRESS`` environment
variables), whether to drive the stream through the interactive
:class:`LiveRenderer` or the line-per-event :class:`LogRenderer`.

:func:`render` is the sole public entry point the CLI commands call: it picks
the renderer, drains the stream via
:func:`...event_stream._event_dispatch.drive`, and returns the accumulated
:class:`RunOutcome` on success. If the stream raises — the generate/plan
failure-rendezvous yields ``ErrorEmitted`` and then raises the underlying
``LHPError`` — this function tears the renderer down cleanly, hands the
caller's ``on_abort`` the outcome accumulated so far, and re-raises the SAME
exception so the CLI's ``error_boundary`` can render the panel on a clean
stderr.

Sole-bridge invariant (constitution §9.5): this module renders rich but MUST
NOT import ``lhp.errors``. The teardown path never inspects the propagating
exception beyond Python's ``Exception`` / ``BaseException`` split (interrupts
skip ``on_abort``): ``on_abort`` receives the renderer's outcome, not the
error, and the exception is re-raised untouched — the error code is read
duck-typed inside the renderers, and the rich panel is single-sourced in
``cli/error_panel.py``.
"""

from __future__ import annotations

import logging
import os
from dataclasses import replace
from typing import TYPE_CHECKING, Callable, Iterator, Optional, Union

from lhp.cli.presenters.event_stream._event_dispatch import EventSink, drive
from lhp.cli.presenters.event_stream._model import RunOutcome
from lhp.cli.presenters.event_stream._outcome import merge_terminal_validation
from lhp.cli.presenters.event_stream.live_renderer import LiveRenderer
from lhp.cli.presenters.event_stream.log_renderer import LogRenderer

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import ProgressSink
    from lhp.api.events import LHPEvent

    from ._model import RenderOptions, RunHeader

logger = logging.getLogger(__name__)

# The factory's whole job is to pick exactly one of these two concrete sinks.
# Both expose ``outcome -> RunOutcome``; the ``EventSink`` ABC (which ``drive``
# consumes) deliberately does not, so the selection helper returns the union
# rather than the bare ABC.
_Renderer = Union[LiveRenderer, LogRenderer]


def _force_log_renderer(console: "Console", *, no_progress: bool) -> bool:
    """Decide once whether to fall back to the non-interactive renderer.

    Returns ``True`` (use :class:`LogRenderer`) when stdout is not a TTY,
    progress is explicitly disabled via ``no_progress``, or either the
    ``CI`` or ``LHP_NO_PROGRESS`` environment variable is set. Otherwise
    ``False`` (use :class:`LiveRenderer`).
    """
    if no_progress:
        return True
    if os.environ.get("CI"):
        return True
    if os.environ.get("LHP_NO_PROGRESS"):
        return True
    return not console.is_terminal


def select_renderer(
    header: "RunHeader",
    *,
    no_progress: bool = False,
    show_details: bool = False,
    progress: "Optional[ProgressSink]" = None,
    console: "Console",
    err_console: "Console",
) -> _Renderer:
    """Pick the renderer for one run, deciding interactivity once.

    The selection is made a single time here and is not revisited while
    the stream drains. :class:`LiveRenderer` drives its self-animating status
    bar on ``console`` (and prints permanent lines above it); the
    fallback :class:`LogRenderer` writes one stable line per rendered
    event to ``err_console``. ``show_details`` is threaded into the chosen
    renderer so per-file warning lines are emitted only on demand (default:
    suppressed, leaving the count to the summary's per-code rollup).

    ``progress`` is the shared :class:`~lhp.api.ProgressSink` the facade
    advances per-flowgroup; it is handed to :class:`LiveRenderer` so the bar
    reads live ``done`` / ``total`` off the SAME instance. The bar's total comes
    from the sink (the header carries no pipeline count: pipelines are coarser
    than the flowgroups the sink counts, and on the diff/plan path no count is
    driven at all). :class:`LogRenderer` has no live bar, so it ignores
    ``progress``.
    """
    if _force_log_renderer(console, no_progress=no_progress):
        logger.debug("renderer-factory: selected LogRenderer (non-interactive)")
        return LogRenderer(err_console, show_details=show_details)
    logger.debug("renderer-factory: selected LiveRenderer (interactive)")
    return LiveRenderer(console, progress=progress, show_details=show_details)


def render(
    events: "Iterator[LHPEvent]",
    header: "RunHeader",
    *,
    options: "RenderOptions",
    no_progress: bool = False,
    progress: "Optional[ProgressSink]" = None,
    console: Optional["Console"] = None,
    err_console: Optional["Console"] = None,
    on_abort: Optional[Callable[[RunOutcome], None]] = None,
) -> RunOutcome:
    """Drive one event stream through the selected renderer.

    Selects :class:`LiveRenderer` vs :class:`LogRenderer` once (see
    :func:`select_renderer`), drains ``events`` via :func:`drive`, and
    returns the renderer's accumulated :class:`RunOutcome` on success. The
    ``progress`` sink — the SAME instance the caller passed to the facade's
    long-running operation — is handed to the live renderer so its flowgroup
    bar reflects live progress; the log renderer ignores it. On
    the validate path — which never raises on findings and folds every
    issue, fully attributed, into the terminal
    :class:`BatchValidationResponse` (while ALSO emitting a per-pipeline
    ``PipelineFailed`` for live progress) — :func:`merge_terminal_validation`
    reconciles that overlap: it REPLACES the event-derived failures with the
    authoritative response-derived set (so one issue is not counted twice) and
    UNIONs the warnings, leaving ``exit_for_outcome`` correct; generate / plan
    terminals pass through unchanged.

    On the generate/plan failure-rendezvous the stream raises the
    underlying ``LHPError`` after yielding ``ErrorEmitted``; this function
    tears the renderer down cleanly, passes ``on_abort`` the outcome the
    renderer accumulated (see :func:`_report_abort`), THEN re-raises the
    SAME exception so ``error_boundary`` renders the panel on a clean
    stderr. A ``KeyboardInterrupt`` or ``SystemExit`` is torn down and
    re-raised without calling ``on_abort``. Per §9.5 the propagating
    exception is treated opaquely — never inspected beyond Python's
    ``Exception`` / ``BaseException`` split, re-raised untouched, never
    replaced by a failing ``on_abort`` — and ``lhp.errors`` is never imported
    here.
    """
    if console is None or err_console is None:
        from lhp.cli import console as _console_module

        if console is None:
            console = _console_module.console
        if err_console is None:
            err_console = _console_module.err_console

    renderer = select_renderer(
        header,
        no_progress=no_progress,
        show_details=options.show_details,
        progress=progress,
        console=console,
        err_console=err_console,
    )
    try:
        # Paint the first frame BEFORE pulling the first event: the stream's
        # opening ``next()`` may block while the facade discovers the worklist,
        # so the live renderer starts its spinner here (no-op on the log
        # renderer) to avoid a blank screen during discovery.
        renderer.begin()
        drive(events, renderer)
    except Exception:
        # §9.5: never inspect or import the error type. Tear the renderer
        # down first (idempotent — the renderer may already have torn its
        # Live down inside on_error) so no live display outlasts the stream
        # while the caller's report runs, then re-raise the SAME exception
        # for the CLI error boundary to render on a clean stderr.
        _teardown(renderer)
        _report_abort(on_abort, renderer.outcome)
        raise
    except BaseException:
        # Ctrl-C and SystemExit must exit promptly: restore the terminal and
        # propagate without running the caller's report.
        _teardown(renderer)
        raise
    # Validate folds its (fully attributed) issues into the terminal
    # BatchValidationResponse while also emitting per-pipeline PipelineFailed,
    # so reconcile the two surfaces here (single RunOutcome builder): the
    # terminal failure set REPLACES the event-derived one to avoid
    # double-counting. Generate / plan terminals pass through unchanged — their
    # failures arrive as in-stream events.
    return merge_terminal_validation(renderer.outcome, renderer.outcome.response)


def _report_abort(
    on_abort: Optional[Callable[[RunOutcome], None]], outcome: RunOutcome
) -> None:
    """Hand an aborted run's outcome to ``on_abort``, keeping per-pipeline failures.

    Only failure lines that name a pipeline are passed on. The log renderer
    also records the stream's ``ErrorEmitted`` as a failure with an empty
    pipeline, which the live renderer does not; dropping it makes both
    renderers report the same failures, and the error itself still reaches
    the caller as the propagating exception. The callback runs while that
    exception is propagating, so its own failure is logged, never raised.
    """
    if on_abort is None:
        return
    try:
        failures = tuple(line for line in outcome.failures if line.pipeline)
        on_abort(replace(outcome, failures=failures))
    except Exception:  # the stream's exception must be the one that propagates
        logger.debug("renderer-factory: on_abort callback failed", exc_info=True)


def _teardown(renderer: EventSink) -> None:
    """Best-effort teardown of a renderer's live display, if any.

    :class:`LiveRenderer` owns a ``rich.live.Live`` exposed via a private
    ``_teardown`` hook; :class:`LogRenderer` has no live region. Teardown
    must never mask the propagating exception, so any failure here is
    swallowed after being logged.
    """
    hook = getattr(renderer, "_teardown", None)
    if hook is None:
        return
    try:
        hook()
    except Exception:
        logger.exception("renderer-factory: error tearing down live renderer")

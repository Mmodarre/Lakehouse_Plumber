"""TTY renderer for the event stream: an animated, self-refreshing status bar.

:class:`LiveRenderer` drives a single :class:`rich.live.Live` whose renderable
is a :class:`._live_renderable._LiveStatus` — a one-line, self-animating status
bar (a real :class:`rich.spinner.Spinner` plus a flowgroup progress bar,
``done/total`` counter, elapsed clock, and the current phase label). Under
``Live(auto_refresh=True)`` Rich's refresh thread re-renders that renderable on
every tick, so the spinner animates and the bar tracks the
:class:`~lhp.api.ProgressSink` *without* the renderer pushing a frame per tick.
Each frame is hard-truncated to ``console.width`` inside the renderable, so it
is always exactly one line (resize-safe — no multi-line cursor math).

Everything durable — stage lines, deduped warnings, per-pipeline failures — is
printed permanently *above* the status via ``live.console.print``. Successes
flash only in the status line and leave no permanent trace (that is how they
"wipe").

**Progress is read, not counted here.** The bar's ``done`` / ``total`` come from
the shared :class:`~lhp.api.ProgressSink` that the facade advances per-flowgroup
(the renderer is handed the SAME instance the facade drives). The renderer does
not maintain its own flowgroup counter; the per-pipeline ``on_pipeline_*``
callbacks only clear the current-pipeline label and record failures. The
renderable reads ``done`` / ``total`` as independent scalars, once each per
tick — never a torn composite (see :mod:`._live_renderable`).

Per the sole-bridge invariant (constitution §9.5) this module imports ``rich``
but never ``lhp.errors``: :meth:`on_error` records ``code`` (a duck-typed
``str`` supplied by :func:`drive`), tears the Live down cleanly, and prints no
panel — the underlying ``LHPError`` raises immediately after, and
``error_boundary`` renders the panel on a clean stderr. It imports
:class:`lhp.api.ProgressSink` (the allowed CLI -> public-API edge, §5.3).

The status-line budget logic (priority truncation: bar + counter never dropped
when ``total > 0``; pipeline name then elapsed truncate first) lives in
:mod:`._status_line`; the animated frame composition lives in
:mod:`._live_renderable`.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.text import Text

from lhp.api import ProgressSink
from lhp.cli.presenters.event_stream._event_dispatch import EventSink
from lhp.cli.presenters.event_stream._live_renderable import (
    _LiveStatus,
    interrupted_phase_line,
)
from lhp.cli.presenters.event_stream._model import (
    FailureLine,
    RunOutcome,
    WarningLine,
)
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_CHECK,
    GLYPH_CROSS,
    GLYPH_WARN,
    duration_suffix,
)

logger = logging.getLogger(__name__)


class LiveRenderer(EventSink):
    """Render an event stream as permanent lines plus one animated status bar.

    Single responsibility: drive a single :class:`rich.live.Live`, emitting
    permanent stage / warning / failure lines and keeping a self-animating
    one-line status bar refreshed off the shared :class:`~lhp.api.ProgressSink`.
    The accumulated :class:`RunOutcome` is exposed via :attr:`outcome` once the
    stream is drained.
    """

    def __init__(
        self,
        console: Console,
        *,
        progress: Optional[ProgressSink] = None,
        show_details: bool = False,
    ) -> None:
        self._console = console
        self._show_details = show_details
        # The bar reads its done/total from this sink. In a real run the facade
        # is handed the SAME instance and advances it per-flowgroup; when no
        # sink is supplied (e.g. a unit test driving events directly) a private
        # one is created so the renderable always has scalars to read.
        self._progress = progress if progress is not None else ProgressSink()
        self._status = _LiveStatus(self._progress, console)
        # Live knobs (§4.2): crop overflow keeps the frame to one line,
        # transient wipes it on stop, screen=False keeps scrollback intact for
        # the permanent lines. auto_refresh + ~12 fps animates the spinner on
        # Rich's refresh thread between event-driven updates.
        self._live = Live(
            self._status,
            console=console,
            vertical_overflow="crop",
            transient=True,
            refresh_per_second=12,
            screen=False,
            auto_refresh=True,
        )
        self._started = False

        # Run state folded into the terminal RunOutcome.
        self._response: object = None
        self._warnings: List[WarningLine] = []
        self._failures: List[FailureLine] = []
        self._seen_warning_keys: set[Tuple[str, Optional[str]]] = set()
        self._errored = False

        # Every event-driven status frame and permanent line, recorded as plain
        # strings for width assertions and content checks (see status_frames).
        self._status_frames: List[str] = []
        self.permanent_lines: List[str] = []

    # -- lifecycle -----------------------------------------------------------
    def begin(self) -> None:
        """Paint the first frame before the stream's first event is pulled.

        Started by ``render`` ahead of :func:`drive`, so the animated spinner
        and an initial ``starting`` phase label are on screen *before* the
        first ``next()`` on the event iterator — which may block while the
        facade discovers the worklist. ``total`` is still ``0`` here, so the
        ``total > 0`` guard collapses the line to ``spinner + phase`` (no
        misleading ``0/0`` bar). The first real :class:`PhaseStarted`
        (``discover``) overwrites the label moments later.
        """
        if self._status.phase is None and not self._status.operation:
            self._status.phase = "starting"
        self._ensure_live()
        self._refresh_status()

    def _ensure_live(self) -> None:
        if not self._started:
            self._live.start(refresh=True)
            self._started = True

    def _teardown(self, *, surface_incomplete: bool = True) -> None:
        if self._started:
            # Surface any started-but-uncompleted phase (§5.7) while the Live
            # is still up, so it does not vanish with the transient bar.
            if surface_incomplete:
                for phase in self._status.open_phases:
                    self._print_above(interrupted_phase_line(phase))
            self._status.open_phases.clear()
            self._live.stop()
            self._started = False

    @property
    def outcome(self) -> RunOutcome:
        """The accumulated terminal result of draining the stream."""
        return RunOutcome(
            response=self._response,
            warnings=tuple(self._warnings),
            failures=tuple(self._failures),
            errored=self._errored,
        )

    # -- status refresh ------------------------------------------------------
    @property
    def status_frames(self) -> List[str]:
        """Plain text of every event-driven frame pushed to the Live.

        Recorded for width assertions and content checks. Transient status
        frames are physically interleaved with erase sequences in a captured
        buffer, so tests inspect this list rather than the buffer. Spinner
        animation between events happens on Rich's refresh thread and is not
        recorded here.
        """
        return self._status_frames

    def _refresh_status(self) -> None:
        """Push a fresh status frame to the Live and record its plain text.

        Records the current frame (sampled from the renderable's clock) so a
        synchronous drive can assert on the exact frames it produced; the Live
        refresh thread reaches the same renderable for between-event animation.
        """
        frame = self._status.current_frame()
        self._status_frames.append(frame.plain)
        self._live.update(self._status, refresh=True)

    # -- permanent lines -----------------------------------------------------
    def _print_above(self, renderable: Text) -> None:
        """Print a permanent line above the transient status bar.

        Records the plain text so callers/tests can inspect permanent output
        without parsing the erase-sequence-laden capture buffer.
        """
        self.permanent_lines.append(renderable.plain)
        self._live.console.print(renderable)

    # -- EventSink callbacks -------------------------------------------------
    def on_operation_started(self, operation_name: str, env: Optional[str]) -> None:
        self._status.operation = operation_name
        self._ensure_live()
        self._refresh_status()

    def on_phase_started(self, phase: str) -> None:
        self._status.mark_phase_started(phase)
        self._refresh_status()

    def on_phase_completed(self, phase: str, duration_s: float, success: bool) -> None:
        # Permanent stage line: glyph + phase, duration right-aligned. Stages
        # that never started never reach here, so they are omitted (no
        # "skipped"). A failed phase carries the cross glyph.
        glyph, style = (
            (GLYPH_CROSS, "bold red") if not success else (GLYPH_CHECK, "bold green")
        )
        line = Text()
        line.append(f"{glyph} ", style=style)
        line.append(phase)
        line.append(duration_suffix(duration_s), style="dim")
        self._print_above(line)
        self._status.mark_phase_completed(phase)
        self._refresh_status()

    def on_pipeline_started(self, pipeline: str) -> None:
        self._status.pipeline = pipeline
        self._refresh_status()

    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        # Success flashes only in the status line — no permanent line. The bar's
        # advance is driven by the facade through the ProgressSink, not counted
        # here, so this only clears the current-pipeline label.
        if self._status.pipeline == pipeline:
            self._status.pipeline = None
        self._refresh_status()

    def on_pipeline_failed(self, pipeline: str, code: str, message: str) -> None:
        self._failures.append(
            FailureLine(pipeline=pipeline, code=code, message=message)
        )
        line = Text()
        line.append(f"{GLYPH_CROSS} ", style="bold red")
        line.append(pipeline)
        line.append("  ")
        line.append(code, style="bold red")
        line.append("  ")
        line.append(message)
        self._print_above(line)
        if self._status.pipeline == pipeline:
            self._status.pipeline = None
        self._refresh_status()

    def on_warning(
        self,
        message: str,
        code: str,
        category: str,
        file: Optional[Path],
        flowgroup: Optional[str],
    ) -> None:
        # Accumulate once per distinct (code, file) — the counts banner and the
        # summary's per-code rollup both read self._warnings. The permanent
        # line (and its status refresh) is emitted only with show_details, so
        # the default run is not buried under one line per file.
        file_str = str(file) if file is not None else None
        key = (code, file_str)
        if key in self._seen_warning_keys:
            return
        self._seen_warning_keys.add(key)
        self._warnings.append(WarningLine(code=code, message=message, file=file_str))
        if not self._show_details:
            return
        line = Text()
        line.append(f"{GLYPH_WARN} ", style="bold yellow")
        if file_str:
            line.append(file_str)
            line.append("  ")
        line.append(code, style="bold yellow")
        line.append("  ")
        line.append(message)
        self._print_above(line)
        self._refresh_status()

    def on_error(self, code: str) -> None:
        # Duck-typed code only; never import lhp.errors (§9.5). Record that an
        # ErrorEmitted was seen and tear the Live down cleanly. Print NO panel:
        # the LHPError raises immediately after this, and error_boundary
        # renders the panel on a clean stderr.
        self._errored = True
        # No trace here: the LHPError raises right after and error_boundary
        # owns the panel on a clean stderr (§9.5).
        self._teardown(surface_incomplete=False)

    def on_terminal(self, response: object) -> None:
        self._response = response
        self._teardown(surface_incomplete=True)

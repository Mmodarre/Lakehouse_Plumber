"""TTY renderer for the event stream: an animated, self-refreshing status region.

:class:`LiveRenderer` drives a single :class:`rich.live.Live` whose renderable
is a :class:`._live_renderable._LiveStatus` — a self-animating, FIXED two-line
region: line 1 the status bar (a real :class:`rich.spinner.Spinner` plus a
flowgroup progress bar, ``done/total`` counter, elapsed clock, and phase label);
line 2 a dim, rotating flavor line. Under ``Live(auto_refresh=True)`` Rich's
refresh thread re-renders it every tick, so the spinner animates, the bar tracks
the :class:`~lhp.api.ProgressSink`, and the flavor word rotates *without* the
renderer pushing a frame per tick. Each line is hard-truncated to
``console.width`` INDEPENDENTLY, so neither can wrap and the transient region
stays exactly two rows; ``Live``'s ``vertical_overflow="crop"`` then bounds the
region to the viewport without scrolling even below two rows (see
:meth:`__init__`).

Everything durable — stage lines, per-pipeline completions, deduped warnings,
per-pipeline failures — is printed permanently *above* the transient region via
``live.console.print``. Those permanent scrollback lines are SINGLE-line (e.g.
``✓ generate  3.1s`` for a phase, ``✓ bronze  2 files`` for a pipeline — NO
per-pipeline seconds); only the live region is two lines. The transient region
itself flashes and "wipes": teardown's transient clear erases BOTH live lines,
leaving the permanent single-line scrollback untouched.

**Progress is read, not counted here.** The bar's ``done`` / ``total`` / live
current-item label all come from the shared :class:`~lhp.api.ProgressSink` that
the facade advances per-flowgroup (the renderer is handed the SAME instance the
facade drives). The renderer does not maintain its own flowgroup counter; the
per-pipeline ``on_pipeline_*`` callbacks record failures and print the durable
completion line, but do not hold the current-item label. The renderable reads
``done`` / ``total`` / ``current`` as independent scalars, once each per tick —
never a torn composite (see :mod:`._live_renderable`).

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
    """Render an event stream as permanent lines plus one animated status region.

    Single responsibility: drive a single :class:`rich.live.Live`, emitting
    permanent SINGLE-line stage / warning / failure lines and keeping a
    self-animating, fixed TWO-line status region (line 1 the status bar, line 2
    the dim flavor line) refreshed off the shared :class:`~lhp.api.ProgressSink`.
    :class:`RunOutcome` is exposed via :attr:`outcome` once the stream is drained.
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
        # Live knobs (§4.2): vertical_overflow="crop" bounds the fixed two-line
        # region to the viewport WITHOUT scrolling — below two rows mid-tick,
        # crop keeps the top rows verbatim (line 1, the status, survives a 1-row
        # viewport) and drops the overflow, so the region never scrolls or wraps
        # and the clear/redraw cursor math stays sound. (Rich's default
        # "ellipsis" would replace the last visible row with a centered "...",
        # erasing the status at one row; "visible" would let the region scroll —
        # both break the fixed height, so crop is the only region-safe choice.)
        # transient wipes BOTH live lines on stop; screen=False keeps scrollback
        # intact for the permanent single-line output; auto_refresh + ~12 fps
        # animates the spinner and rotates the flavor word on Rich's refresh
        # thread between event-driven updates.
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

        # Line 1 of each event-driven frame + every permanent line, as plain
        # strings for width/content checks (see status_frames).
        self._status_frames: List[str] = []
        self.permanent_lines: List[str] = []

    # -- lifecycle -----------------------------------------------------------
    def begin(self) -> None:
        """Paint the first frame before the stream's first event is pulled.

        Started by ``render`` ahead of :func:`drive`, so the spinner and a
        ``starting`` label show *before* the first ``next()`` — which may block
        while the facade discovers the worklist. ``total`` is still ``0``, so the
        ``total > 0`` guard collapses line 1 to ``spinner + phase`` (no ``0/0``
        bar); the first real :class:`PhaseStarted` overwrites it moments later.
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
            # is still up, so it does not vanish with the transient region.
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
        """Plain text of line 1 for every event-driven frame pushed to the Live.

        Recorded (via :meth:`_LiveStatus.current_frame`) for width/content checks.
        Transient frames are interleaved with erase sequences in the capture
        buffer, so tests inspect this list; refresh-thread animation is omitted.
        """
        return self._status_frames

    def _refresh_status(self) -> None:
        """Push a fresh frame to the Live and record line 1's plain text.

        Records line 1 via :meth:`_LiveStatus.current_frame` so a synchronous
        drive can assert on the exact frames it produced; the Live refresh thread
        reaches the same renderable for between-event animation of both lines.
        """
        frame = self._status.current_frame()
        self._status_frames.append(frame.plain)
        self._live.update(self._status, refresh=True)

    # -- permanent lines -----------------------------------------------------
    def _print_above(self, renderable: Text) -> None:
        """Print a permanent SINGLE-line entry above the transient region.

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
        # Permanent SINGLE-line stage line: glyph + phase + duration. Unstarted
        # stages never reach here (none read "skipped"); a failure crosses out.
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
        # Begin-marker only: the live current-item label is read off the
        # facade-driven ProgressSink.current (set per completed flowgroup), so
        # this no longer sets a renderer-held label — it just paints a frame so
        # the spinner/elapsed advance at the pipeline boundary.
        self._refresh_status()

    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        # A finished pipeline can leave a DURABLE single-line trace above the
        # transient region (a uv-like trail): `✓ <pipeline>  <N> file(s)`. This
        # per-pipeline trail is OPT-IN via `--show-details`; by default the TTY
        # display stays clean (phase lines + bar + summary only) so a project
        # with dozens of pipelines doesn't flood scrollback. No per-pipeline
        # seconds — only per-PHASE lines carry a duration; the `duration_s`
        # parameter is kept solely for EventSink ABC / drive() parity. The bar
        # always refreshes regardless of the flag.
        if self._show_details:
            noun = "file" if files_written == 1 else "files"
            line = Text()
            line.append(f"{GLYPH_CHECK} ", style="bold green")
            line.append(pipeline)
            line.append(f"  {files_written} {noun}", style="dim")
            self._print_above(line)
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
        self._refresh_status()

    def on_warning(
        self,
        message: str,
        code: str,
        category: str,
        file: Optional[Path],
        flowgroup: Optional[str],
    ) -> None:
        # Accumulate once per distinct (code, file) for the counts banner and
        # per-code summary; the permanent line is emitted only with show_details.
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
        # ErrorEmitted was seen and tear the Live down cleanly, surfacing no
        # incomplete-phase trace. Print NO panel: the LHPError raises right after
        # this and error_boundary owns the panel on a clean stderr.
        self._errored = True
        self._teardown(surface_incomplete=False)

    def on_terminal(self, response: object) -> None:
        self._response = response
        self._teardown(surface_incomplete=True)

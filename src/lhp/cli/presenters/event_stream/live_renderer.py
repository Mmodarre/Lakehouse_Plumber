"""TTY renderer for the event stream: a single transient status line.

:class:`LiveRenderer` drives a single :class:`rich.live.Live` whose
renderable is a one-line :class:`~rich.text.Text` status bar, hard-truncated
to ``console.width`` on every refresh so it is always exactly one line
(resize-safe — no multi-line cursor math). Everything durable — stage lines,
deduped warnings, per-pipeline failures — is printed permanently *above* the
status via ``live.console.print``. Successes flash only in the status line and
leave no permanent trace (that is how they "wipe").

Per the sole-bridge invariant (constitution §9.5) this module imports ``rich``
but never ``lhp.errors``: :meth:`on_error` records ``code`` (a duck-typed
``str`` supplied by :func:`drive`), tears the Live down cleanly, and prints no
panel — the underlying ``LHPError`` raises immediately after, and
``error_boundary`` renders the panel on a clean stderr.

The status-line budget logic (priority truncation: bar + counter never
dropped; pipeline name then elapsed truncate first) lives in
:mod:`._status_line`.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import List, Optional, Tuple

from rich.console import Console
from rich.live import Live
from rich.text import Text

from lhp.cli.presenters.event_stream._event_dispatch import EventSink
from lhp.cli.presenters.event_stream._model import (
    FailureLine,
    RunOutcome,
    WarningLine,
)
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_CHECK,
    GLYPH_CROSS,
    GLYPH_SPINNER,
    GLYPH_WARN,
    build_status_line,
    duration_suffix,
)

logger = logging.getLogger(__name__)


class LiveRenderer(EventSink):
    """Render an event stream as permanent lines plus one transient status bar.

    Single responsibility: drive a single :class:`rich.live.Live`, emitting
    permanent stage / warning / failure lines and refreshing a one-line
    status bar. The accumulated :class:`RunOutcome` is exposed via
    :attr:`outcome` once the stream is drained.
    """

    def __init__(
        self, console: Console, *, total: int = 0, show_details: bool = False
    ) -> None:
        self._console = console
        self._show_details = show_details
        # Live knobs are locked by the spec (§4.2): crop overflow keeps the
        # frame to one line, transient wipes it on stop, 4 fps is the cadence,
        # screen=False keeps scrollback intact for the permanent lines.
        self._live = Live(
            Text(""),
            console=console,
            vertical_overflow="crop",
            transient=True,
            refresh_per_second=4,
            screen=False,
            auto_refresh=False,
        )
        self._started = False

        # Run state folded into the terminal RunOutcome.
        self._response: object = None
        self._warnings: List[WarningLine] = []
        self._failures: List[FailureLine] = []
        self._seen_warning_keys: set[Tuple[str, Optional[str]]] = set()
        self._errored = False

        # Status-line state.
        self._operation = ""
        self._current_phase: Optional[str] = None
        self._current_pipeline: Optional[str] = None
        self._done_count = 0
        # Total pipelines for the n/m counter; supplied from RunHeader by the
        # renderer factory (the count is not carried in the event stream).
        self._total = total
        self._start_perf = time.perf_counter()

        # Every status frame built and every permanent line printed, recorded
        # as plain strings for width assertions and content checks. Transient
        # status frames are physically interleaved with erase sequences in a
        # captured buffer, so tests inspect these lists rather than the buffer.
        self.status_frames: List[str] = []
        self.permanent_lines: List[str] = []

    # -- lifecycle -----------------------------------------------------------
    def _ensure_live(self) -> None:
        if not self._started:
            self._live.start(refresh=False)
            self._started = True

    def _teardown(self) -> None:
        if self._started:
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
    def _refresh_status(self) -> None:
        """Rebuild the one-line status bar and push it to the Live frame."""
        elapsed = time.perf_counter() - self._start_perf
        text = build_status_line(
            spinner=GLYPH_SPINNER,
            phase=self._current_phase or self._operation,
            done=self._done_count,
            total=self._total,
            pipeline=self._current_pipeline,
            elapsed_s=elapsed,
            width=self._console.width,
        )
        # Safety net: guarantee exactly one line no wider than the terminal,
        # so a resize between refreshes can never wrap. build_status_line
        # already budgets to width; this clamps the worst case.
        text.truncate(self._console.width, overflow="crop")
        self.status_frames.append(text.plain)
        self._live.update(text, refresh=True)

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
        self._operation = operation_name
        self._start_perf = time.perf_counter()
        self._ensure_live()
        self._refresh_status()

    def on_phase_started(self, phase: str) -> None:
        self._current_phase = phase
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
        if self._current_phase == phase:
            self._current_phase = None
        self._refresh_status()

    def on_pipeline_started(self, pipeline: str) -> None:
        self._current_pipeline = pipeline
        self._refresh_status()

    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        # Success flashes only in the status line — no permanent line.
        self._done_count += 1
        if self._current_pipeline == pipeline:
            self._current_pipeline = None
        self._refresh_status()

    def on_pipeline_failed(self, pipeline: str, code: str, message: str) -> None:
        self._done_count += 1
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
        if self._current_pipeline == pipeline:
            self._current_pipeline = None
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
        self._teardown()

    def on_terminal(self, response: object) -> None:
        self._response = response
        self._teardown()

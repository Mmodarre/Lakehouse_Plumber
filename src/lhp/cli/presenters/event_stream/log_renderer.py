"""Non-interactive (CI / non-TTY) renderer for the event stream.

``LogRenderer`` is the :class:`EventSink` selected when stdout is not a
terminal (or progress is force-disabled). Unlike the live renderer it
never constructs a ``rich.live.Live`` region: it emits one stable
``err_console.print(...)`` line per *distinct* rendered event — a stage
line for each completed phase / pipeline, a cross line for each per-pipeline
failure, and a single line per distinct ``(code, file)`` warning (repeats
collapse). Begin-markers (``PhaseStarted`` / ``PipelineStarted``) and the
record-only callbacks (``on_error`` / ``on_terminal``) emit nothing; the
run-header banner and the failure/next-step block are the summary
presenter's job, not this renderer's.

All output goes to the injected stderr ``Console``; stdout stays empty.

Sole-bridge invariant (constitution §5.2 / §9.5): this module renders
rich but MUST NOT import ``lhp.errors``. ``on_error`` receives the error
code already duck-typed to ``str`` by
:func:`...event_stream._event_dispatch.drive`; the live exception is never
touched here, so error formatting stays single-sourced in
``cli/error_panel.py``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional, Set, Tuple

from rich.text import Text

from ._model import FailureLine, RunOutcome, WarningLine

if TYPE_CHECKING:
    from pathlib import Path

    from rich.console import Console

from ._event_dispatch import EventSink

logger = logging.getLogger(__name__)

_OK_GLYPH = "✓"
_FAIL_GLYPH = "✗"
_OK_STYLE = "bold green"
_FAIL_STYLE = "bold red"


class LogRenderer(EventSink):
    """Line-per-event sink for non-interactive runs.

    Accumulates the same :class:`RunOutcome` state as the live renderer
    (deduped warnings, per-pipeline failures, the ``errored`` flag, and
    the terminal response) while printing one stable stderr line per
    distinct rendered event. Never constructs ``rich.live.Live``.
    """

    def __init__(self, err_console: "Console", *, show_details: bool = False) -> None:
        self._err_console = err_console
        self._show_details = show_details
        self._warnings: List[WarningLine] = []
        self._failures: List[FailureLine] = []
        self._seen_warning_keys: Set[Tuple[str, Optional[str]]] = set()
        self._errored: bool = False
        self._response: object = None

    # -- non-printing progress markers --------------------------------------
    def begin(self) -> None:
        """No-op: the non-interactive renderer paints nothing pre-stream.

        The live renderer uses :meth:`begin` to start its spinner before the
        first (possibly discovery-blocked) event; the log renderer has no live
        region and emits one stable line per *completed* event, so there is
        nothing to paint before the stream opens.
        """

    def on_operation_started(self, operation_name: str, env: Optional[str]) -> None:
        """Opening :class:`OperationStarted`; the banner is the summary's job."""
        logger.debug(f"log-render: operation started {operation_name} env={env}")

    def on_phase_started(self, phase: str) -> None:
        """Begin-marker; the paired ``on_phase_completed`` carries the line."""
        logger.debug(f"log-render: phase started {phase}")

    def on_pipeline_started(self, pipeline: str) -> None:
        """Begin-marker; the paired completion/failure carries the line."""
        logger.debug(f"log-render: pipeline started {pipeline}")

    # -- printing rendered events -------------------------------------------
    def on_phase_completed(self, phase: str, duration_s: float, success: bool) -> None:
        """Print one stage line: glyph + phase label + duration."""
        self._err_console.print(self._stage_line(phase, duration_s, success))

    def on_pipeline_completed(
        self, pipeline: str, duration_s: float, files_written: int
    ) -> None:
        """Print one success stage line for a finished pipeline."""
        line = self._stage_line(pipeline, duration_s, success=True)
        noun = "file" if files_written == 1 else "files"
        line.append(f"  {files_written} {noun}", style="dim")
        self._err_console.print(line)

    def on_pipeline_failed(self, pipeline: str, code: str, message: str) -> None:
        """Record the failure for the outcome and print one cross line."""
        self._failures.append(
            FailureLine(pipeline=pipeline, code=code, message=message)
        )
        text = Text()
        text.append(f"{_FAIL_GLYPH} ", style=_FAIL_STYLE)
        text.append(pipeline, style="bold")
        if code:
            text.append("  ")
            text.append(code, style=_FAIL_STYLE)
        if message:
            text.append("  ")
            text.append(message)
        self._err_console.print(text)

    def on_warning(
        self,
        message: str,
        code: str,
        category: str,
        file: Optional[Path],
        flowgroup: Optional[str],
    ) -> None:
        """Record once per distinct ``(code, file)``; print only with details.

        ``file`` arrives as a :class:`pathlib.Path` (or ``None``) from the
        event; it is normalised to ``str`` for both the dedup key and the
        recorded :class:`WarningLine`. The accumulation is unconditional (the
        counts banner and the summary's per-code rollup both read it); the
        per-file line is printed only when ``show_details`` is set, so the
        default run shows a count, not one line per file.
        """
        file_str = str(file) if file is not None else None
        key = (code, file_str)
        if key in self._seen_warning_keys:
            return
        self._seen_warning_keys.add(key)
        self._warnings.append(WarningLine(code=code, message=message, file=file_str))
        if not self._show_details:
            return
        text = Text()
        text.append("! ", style="bold yellow")
        if code:
            text.append(code, style="bold yellow")
            text.append("  ")
        text.append(message)
        if file_str:
            text.append("  ")
            text.append(file_str, style="dim")
        self._err_console.print(text)

    # -- record-only callbacks ----------------------------------------------
    def on_error(self, code: str) -> None:
        """Record the error code for the outcome; print no panel (§9.5).

        The owning ``LHPError`` raises immediately after this event, so the
        summary's error boundary renders the full panel on a clean stream.
        """
        self._errored = True
        self._failures.append(
            FailureLine(pipeline="", code=code, message="operation failed")
        )
        logger.debug(f"log-render: error emitted {code}")

    def on_terminal(self, response: object) -> None:
        """Capture the terminal response DTO for the outcome."""
        self._response = response

    # -- outcome ------------------------------------------------------------
    @property
    def outcome(self) -> RunOutcome:
        """The accumulated :class:`RunOutcome` for this drained stream."""
        return RunOutcome(
            response=self._response,
            warnings=tuple(self._warnings),
            failures=tuple(self._failures),
            errored=self._errored,
        )

    # -- helpers ------------------------------------------------------------
    def _stage_line(self, label: str, duration_s: float, success: bool) -> Text:
        """A ``<glyph> <label> (<duration>s)`` stage line."""
        glyph = _OK_GLYPH if success else _FAIL_GLYPH
        style = _OK_STYLE if success else _FAIL_STYLE
        text = Text()
        text.append(f"{glyph} ", style=style)
        text.append(label)
        text.append(f" ({duration_s:.2f}s)", style="dim")
        return text

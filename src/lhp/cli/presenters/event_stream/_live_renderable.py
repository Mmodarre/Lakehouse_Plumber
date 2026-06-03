"""The dynamic, self-animating renderable behind the live status bar.

:class:`_LiveStatus` is the renderable handed to the
:class:`rich.live.Live` driven by :class:`..live_renderer.LiveRenderer`.
Under ``Live(auto_refresh=True)`` Rich's refresh thread re-invokes the
renderable's :meth:`~_LiveStatus.__rich_console__` on every tick, so the
status line *animates* without the renderer pushing a frame per tick: the
:class:`rich.spinner.Spinner` advances purely as a function of elapsed time,
and the flowgroup bar/counter reflect the latest :class:`~lhp.api.ProgressSink`
scalars.

**Lock-free read contract.** The coordinator thread writes
``ProgressSink.done`` / ``ProgressSink.total`` while this renderable (on Rich's
refresh thread) reads them. The two are INDEPENDENT scalars with no
cross-field invariant (see :class:`lhp.api.ProgressSink`), so each is read
EXACTLY ONCE per tick into a local before use — never a composite/derived read
across both. One-frame staleness is acceptable; a torn composite is not.

**Animation is a pure function of time.** The spinner frame is
``Spinner.render(elapsed)``; sampling at two different clock values yields two
different frames. The clock is injectable (``clock=``) so a test can render at
explicit times and observe the animation deterministically, off the refresh
thread entirely.

**``total > 0`` guard.** Before the run's total is known, or on a path that
does not drive the counter (diff/plan), ``total`` is ``0`` and
:func:`.._status_line.build_status_line` collapses to ``spinner + phase`` — no
misleading ``0/0`` bar. The composed line is hard-truncated to ``console.width``
here, so it can never overflow on a mid-tick resize.

No ``lhp.errors`` import (sole-bridge invariant, constitution §9.5); this module
is pure presentation. It imports :class:`lhp.api.ProgressSink` — the single
public progress observer — which is the allowed CLI -> public-API edge (§5.3).
"""

from __future__ import annotations

import time
from typing import Callable, Optional

from rich.console import Console, ConsoleOptions, RenderResult
from rich.spinner import Spinner
from rich.text import Text

from lhp.api import ProgressSink
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_WARN,
    build_status_line,
)

# A braille spinner whose per-frame glyph is single-cell, matching the flat,
# single-cell status-line budget in ``_status_line`` (so character count still
# equals cell width once the frame is spliced in).
_SPINNER_NAME = "dots"


def interrupted_phase_line(phase: str) -> Text:
    """A permanent trace line for a phase that started but never completed.

    Built here (rather than inline in :class:`..live_renderer.LiveRenderer`) to
    keep the renderer under its presenter line cap. A :class:`PhaseStarted`
    with no matching :class:`PhaseCompleted` violates the §5.7 pairing
    invariant; on teardown the renderer prints this warning-glyphed line via
    ``live.console.print`` so the incomplete phase leaves a VISIBLE trace
    instead of silently disappearing when the transient status bar is wiped.
    """
    line = Text()
    line.append(f"{GLYPH_WARN} ", style="bold yellow")
    line.append(phase)
    line.append("  did not complete", style="dim")
    return line


class _LiveStatus:
    """Self-animating renderable for the one-line live status bar.

    Owns a :class:`rich.spinner.Spinner` and a reference to the run's
    :class:`~lhp.api.ProgressSink`; the live renderer mutates the plain
    :attr:`phase` / :attr:`pipeline` / :attr:`operation` attributes as events
    arrive. Each render recomputes the full line from the current state, so the
    bar advances when the sink advances and the spinner advances with the clock.

    The single responsibility is "produce the current status frame"; recording,
    permanent lines, and ``Live`` lifecycle stay in the renderer.
    """

    def __init__(
        self,
        progress: ProgressSink,
        console: Console,
        *,
        clock: Callable[[], float] = time.perf_counter,
    ) -> None:
        self._progress = progress
        self._console = console
        self._clock = clock
        self._spinner = Spinner(_SPINNER_NAME)
        self._start = clock()

        # Mutated by the renderer as events arrive; read on every frame.
        self.operation: str = ""
        self.phase: Optional[str] = None
        self.pipeline: Optional[str] = None
        # PhaseStarted names with no matching PhaseCompleted yet, in arrival
        # order. Co-located with `phase` (this object owns phase state); the
        # renderer reads it on teardown to surface interrupted phases (§5.7).
        self.open_phases: list[str] = []

    def mark_phase_started(self, phase: str) -> None:
        """Record a phase as open (idempotent), preserving arrival order."""
        self.phase = phase
        if phase not in self.open_phases:
            self.open_phases.append(phase)

    def mark_phase_completed(self, phase: str) -> None:
        """Close a phase: drop it from the open set and clear the label."""
        if phase in self.open_phases:
            self.open_phases.remove(phase)
        if self.phase == phase:
            self.phase = None

    def render_frame(self, now: float) -> Text:
        """Build the status frame as a pure function of ``now`` and state.

        ``now`` is an absolute clock reading (same base as the injected
        ``clock``); the spinner frame and the elapsed clock are both derived
        from it, so two distinct ``now`` values yield two distinct frames. The
        :class:`~lhp.api.ProgressSink` scalars are snapshotted ONCE each into
        locals before use (lock-free read contract — see the module docstring).
        """
        done = self._progress.done
        total = self._progress.total
        elapsed = now - self._start
        # Spinner.render is a pure function of its time argument: the frame
        # index is derived from elapsed / frame-interval, so distinct elapsed
        # values can select distinct frames (the source of the animation).
        # Its declared return is rich's RenderableType union, but a text-less
        # spinner (no ``text=``) always returns a single-cell Text; narrow to
        # that so the .plain str the status line needs is type-safe.
        rendered = self._spinner.render(elapsed)
        spinner_frame = rendered.plain if isinstance(rendered, Text) else str(rendered)
        text = build_status_line(
            spinner=spinner_frame,
            phase=self.phase or self.operation,
            done=done,
            total=total,
            pipeline=self.pipeline,
            elapsed_s=elapsed,
            width=self._console.width,
        )
        # Safety net: guarantee exactly one line no wider than the terminal so a
        # resize between refreshes can never wrap. build_status_line budgets to
        # width; this clamps the worst case (overlong phase/pipeline names).
        text.truncate(self._console.width, overflow="crop")
        return text

    def current_frame(self) -> Text:
        """The frame for *now*, sampled from the injected clock.

        The renderer uses this to record the exact frame it pushes on an event
        callback; the Live refresh thread reaches the same result through
        :meth:`__rich_console__`.
        """
        return self.render_frame(self._clock())

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        """Yield the current frame; re-invoked per tick by ``Live`` auto-refresh.

        Reading the clock here (rather than caching a frame) is what makes the
        spinner animate under ``auto_refresh=True`` without the renderer pushing
        a frame per tick.
        """
        yield self.current_frame()

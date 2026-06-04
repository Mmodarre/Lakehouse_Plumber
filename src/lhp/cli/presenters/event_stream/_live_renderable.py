"""The dynamic, self-animating renderable behind the live status region.

:class:`_LiveStatus` is the renderable handed to the
:class:`rich.live.Live` driven by :class:`..live_renderer.LiveRenderer`.
Under ``Live(auto_refresh=True)`` Rich's refresh thread re-invokes the
renderable's :meth:`~_LiveStatus.__rich_console__` on every tick, so the
region *animates* without the renderer pushing a frame per tick: the
:class:`rich.spinner.Spinner` advances purely as a function of elapsed time,
the flowgroup bar/counter reflect the latest :class:`~lhp.api.ProgressSink`
scalars, and the dim flavor word rotates on its own time bucket.

**Fixed two-line region.** Every render yields exactly two renderables — line 1
is the status bar (:func:`.._status_line.build_status_line`), line 2 is the dim
rotating flavor line (:func:`.._status_line.build_flavor_line`). Each is
hard-truncated to ``console.width`` INDEPENDENTLY, so neither can wrap onto an
extra row on a mid-tick resize; the region's height stays exactly two. The
renderer relies on that fixed height for its clear/redraw cursor math, so the
region must never grow or shrink between ticks.

**Lock-free read contract.** The coordinator thread writes
``ProgressSink.done`` / ``ProgressSink.total`` while this renderable (on Rich's
refresh thread) reads them. The two are INDEPENDENT scalars with no
cross-field invariant (see :class:`lhp.api.ProgressSink`), so each is read
EXACTLY ONCE per tick into a local before use — never a composite/derived read
across both. One-frame staleness is acceptable; a torn composite is not.

**Animation is a pure function of time, off a per-run starting phase.** The
spinner frame is ``Spinner.render(elapsed)`` and the flavor word is
``flavor_word_for(elapsed, offset=...)``; sampling at two different clock values
yields two different frames. To stop every run looking identical (always frame
◐ + the same first word at ``elapsed≈0``), this renderable draws a random
STARTING ``offset`` for the line-2 spinner phase and word ONCE at construction
(via :mod:`random`, the only RNG use in the flavor path) and threads it into
those two pure functions — never a per-frame draw, which would jitter rather
than spin. The clock is injectable (``clock=``) AND the offsets are injectable
(``spinner_offset=`` / ``word_offset=``) so a test can pin both and render at
explicit times to observe the animation deterministically, off the refresh
thread entirely.

**``total > 0`` guard.** Before the run's total is known, or on a path that
does not drive the counter (diff/plan), ``total`` is ``0`` and
:func:`.._status_line.build_status_line` collapses to ``spinner + phase`` — no
misleading ``0/0`` bar.

No ``lhp.errors`` import (sole-bridge invariant, constitution §9.5); this module
is pure presentation. It imports :class:`lhp.api.ProgressSink` — the single
public progress observer — which is the allowed CLI -> public-API edge (§5.3).
"""

from __future__ import annotations

import random
import time
from typing import Callable, Optional

from rich.console import Console, ConsoleOptions, Group, RenderResult
from rich.spinner import Spinner
from rich.text import Text

from lhp.api import ProgressSink
from lhp.cli.presenters.event_stream._flavor_words import (
    SPINNER_FRAMES,
    flavor_spinner_frame_for,
    flavor_word_count,
    flavor_word_for,
)
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_WARN,
    build_flavor_line,
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
    """Self-animating renderable for the fixed two-line live status region.

    Owns a :class:`rich.spinner.Spinner` and a reference to the run's
    :class:`~lhp.api.ProgressSink`; the live renderer mutates the plain
    :attr:`phase` / :attr:`pipeline` / :attr:`operation` attributes as events
    arrive. Each render recomputes both lines from the current state, so the
    bar advances when the sink advances and the spinner/flavor advance with the
    clock.

    The single responsibility is "produce the current status region"; recording,
    permanent lines, and ``Live`` lifecycle stay in the renderer.
    """

    def __init__(
        self,
        progress: ProgressSink,
        console: Console,
        *,
        clock: Callable[[], float] = time.perf_counter,
        spinner_offset: Optional[int] = None,
        word_offset: Optional[int] = None,
    ) -> None:
        self._progress = progress
        self._console = console
        self._clock = clock
        self._spinner = Spinner(_SPINNER_NAME)
        self._start = clock()

        # Per-RUN phase offsets for the line-2 flavor (the spinner quadrant and
        # the rotating word), so each invocation of the CLI begins on a
        # DIFFERENT quadrant/word instead of always frame ◐ + the first word.
        # This is the ONLY randomness in the flavor path and it is drawn EXACTLY
        # ONCE here at construction — never per frame: per-frame random would
        # make the spinner jitter rather than spin and would break the
        # clock-injected renderer tests. The offsets only shift the STARTING
        # point of the otherwise pure, deterministic time-bucket rotation in
        # `_flavor_words`; once fixed, render output is a pure function of the
        # injected clock. Tests pass explicit ints to pin a known sequence; in
        # production (None) one value is drawn per offset via `random.randrange`.
        self._spinner_offset: int = (
            random.randrange(len(SPINNER_FRAMES))
            if spinner_offset is None
            else spinner_offset
        )
        self._word_offset: int = (
            random.randrange(flavor_word_count())
            if word_offset is None
            else word_offset
        )

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
        """Build line 1, the status frame, as a pure function of ``now``/state.

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
        # Safety net: clamp line 1 to the terminal width so a resize between
        # refreshes can never wrap it onto a second row. build_status_line
        # budgets to width; this clamps the worst case (overlong names).
        text.truncate(self._console.width, overflow="crop")
        return text

    def render_flavor(self, now: float) -> Text:
        """Build line 2, the dim flavor line, as a pure function of ``now``.

        The elapsed clock is derived from ``now`` the same way as
        :meth:`render_frame`, then fed to BOTH
        :func:`.._flavor_words.flavor_spinner_frame_for` (the animated
        circle-quadrant lead-in) and :func:`.._flavor_words.flavor_word_for`
        (the rotating word) — each a deterministic time-bucket pick on its own
        period, shifted by this run's fixed :attr:`_spinner_offset` /
        :attr:`_word_offset` so the rotation STARTS on a per-run quadrant/word
        — so the spinner advances and the word rotates as the clock advances,
        and two distinct ``now`` values can select two distinct frames. With the
        offsets fixed at construction the result stays a pure function of
        ``now``. :func:`.._status_line.build_flavor_line` hard-truncates the
        result to the terminal width independently of line 1.
        """
        elapsed = now - self._start
        spinner_frame = flavor_spinner_frame_for(elapsed, offset=self._spinner_offset)
        word = flavor_word_for(elapsed, offset=self._word_offset)
        return build_flavor_line(spinner_frame, word, self._console.width)

    def current_frame(self) -> Text:
        """Line 1 for *now*, sampled from the injected clock.

        The renderer uses this to record the exact status line it pushes on an
        event callback; the Live refresh thread reaches the same result through
        :meth:`__rich_console__`.
        """
        return self.render_frame(self._clock())

    def current_flavor(self) -> Text:
        """Line 2 (the dim flavor line) for *now*, sampled from the clock."""
        return self.render_flavor(self._clock())

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:
        """Yield the two-line region; re-invoked per tick by ``Live``.

        Reading the clock here (rather than caching frames) is what makes the
        spinner animate and the flavor word rotate under ``auto_refresh=True``
        without the renderer pushing a frame per tick. The two lines are grouped
        so the region is one renderable of fixed height (line 1 status, line 2
        flavor), each already truncated to ``console.width``.
        """
        yield Group(self.current_frame(), self.current_flavor())

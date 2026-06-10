"""Glyphs and width-budget layout for the live status region's two lines.

The live status region rendered by :class:`..live_renderer.LiveRenderer` is a
FIXED two-line region: line 1 is the status bar (:func:`build_status_line`),
line 2 is the rotating dim flavor line (:func:`build_flavor_line`). Each line is
laid out and hard-truncated to ``console.width`` INDEPENDENTLY here, so neither
can ever wrap onto an extra row — the region's height stays exactly two,
resize-safe, which is what lets the renderer's cursor math (clear/redraw) treat
it as a fixed block.

Line 1 keeps the load-bearing pieces visible under width pressure:
:func:`build_status_line` packs the bar to a width budget with a fixed priority
— the spinner, phase label, progress bar, and ``done/total`` counter are never
dropped; the trailing pipeline name truncates first, then the elapsed clock.

The ``spinner`` argument is the CURRENT animated spinner frame (a single-cell
glyph sampled from a :class:`rich.spinner.Spinner` per refresh tick by the live
renderable), not a fixed glyph — animation is the caller's concern; this module
only lays the frame out.

The flowgroup bar and ``done/total`` counter render ONLY when ``total > 0``.
Before the run's total is known (the startup / "discovering" window) or on a
path that does not drive the counters (e.g. ``diff``/plan), ``total`` is ``0``
and the line collapses to ``spinner + phase`` (plus the optional pipeline name
and elapsed clock) — never a misleading ``0/0`` bar.

Plain block glyphs are used for the bar (NOT ``rich.progress.Progress``) so the
whole line is one flat :class:`~rich.text.Text` that can be hard-truncated in a
single call. Every glyph here is single-cell, so character count equals cell
width and a character-budget is a faithful width budget.

No ``lhp.errors`` import (sole-bridge invariant, constitution §9.5); this module
is pure presentation.
"""

from __future__ import annotations

from typing import Optional

from rich.text import Text

# Single-cell glyphs (constitution STYLE.md §4 plus the bar from the
# spec §4.3). All width 1, so len(str) == cell width on the assembled line.
# The spinner glyph is no longer a static constant: P3 moved to a live
# rich.spinner.Spinner whose current frame is passed in as the `spinner` arg.
GLYPH_CHECK = "✓"
GLYPH_CROSS = "✗"
GLYPH_WARN = "⚠"
_BAR_FULL = "█"
_BAR_EMPTY = "░"
# The dim second (flavor) line's lead-in is no longer a static glyph: it is an
# ANIMATED spinner sampled per tick from the four circle quadrants in
# ._flavor_words.SPINNER_FRAMES (U+25D0..U+25D3, Geometric Shapes block, Unicode
# category So). Each frame is single-cell and NOT an emoji, so constitution §7.6
# is satisfied — the same Live-frame glyph family as the U+2713 check and line
# 1's braille spinner. STYLE.md §4's "three glyphs, total" governs only the
# static commands (STYLE.md §1 carves the Live frames out), so this surface is
# free to use it. The current frame is passed in as build_flavor_line's
# ``spinner`` arg; animation is the caller's concern (see the module docstring).

# Two-space gutter between status segments, matching the spec example
# `⠹ generate ███████░░░  12/18  silver_orders  3.1s`.
_GUTTER = "  "
# Smallest progress bar we ever draw; the bar flexes down to this but is never
# dropped entirely.
_MIN_BAR = 1
# Preferred (uncramped) progress-bar width.
_MAX_BAR = 10


def duration_suffix(duration_s: float) -> str:
    """Right-pad-friendly duration suffix for a permanent stage line.

    Two leading spaces separate it from the phase label; one decimal second
    matches the spec's ``3.1s`` form.
    """
    return f"  {duration_s:.1f}s"


def _counter(done: int, total: int) -> str:
    return f"{done}/{total}"


def _bar(done: int, total: int, span: int) -> str:
    """A ``span``-wide block bar; clamped to ``[_MIN_BAR, span]``."""
    span = max(_MIN_BAR, span)
    if total <= 0:
        filled = 0
    else:
        # Floor (not round): the filled-cell count is then a monotonic
        # non-decreasing step function of `done` that never overshoots `span`
        # and never loses a cell between adjacent `done` values — round() can
        # jump by two or briefly regress at narrow `span`, which reads as jitter.
        filled = span * min(done, total) // total
        filled = max(0, min(span, filled))
    return _BAR_FULL * filled + _BAR_EMPTY * (span - filled)


def _append_optionals(
    text: Text,
    *,
    pipeline: Optional[str],
    elapsed: str,
    slack: int,
) -> None:
    """Append the pipeline name then the elapsed clock that fit ``slack``.

    Reservation priority (which drops first under width pressure): the elapsed
    clock is reserved first, the pipeline name second — so the pipeline name is
    the first to be dropped as the line narrows. Visual order is the spec's
    (pipeline before elapsed), so the two are appended in that order after both
    fit decisions are made.
    """
    elapsed_cost = len(_GUTTER) + len(elapsed)
    show_elapsed = slack >= elapsed_cost
    if show_elapsed:
        slack -= elapsed_cost

    show_pipeline = False
    if pipeline:
        pipeline_cost = len(_GUTTER) + len(pipeline)
        show_pipeline = slack >= pipeline_cost

    if show_pipeline and pipeline:
        text.append(_GUTTER)
        text.append(pipeline)
    if show_elapsed:
        text.append(_GUTTER)
        text.append(elapsed, style="dim")


def _build_spinner_only_line(
    *, head: str, pipeline: Optional[str], elapsed: str, width: int
) -> Text:
    """The ``total <= 0`` layout: ``spinner + phase`` plus optional trailers.

    No flowgroup bar and no ``0/0`` counter — used in the startup/"discovering"
    window and on counter-less paths (diff/plan). The head (spinner + phase) is
    mandatory; pipeline and elapsed fill remaining slack in that priority.
    """
    text = Text()
    text.append(head)
    _append_optionals(text, pipeline=pipeline, elapsed=elapsed, slack=width - len(head))
    return text


def build_status_line(
    *,
    spinner: str,
    phase: str,
    done: int,
    total: int,
    pipeline: Optional[str],
    elapsed_s: float,
    width: int,
) -> Text:
    """Build line 1, the status bar, within a ``width`` character budget.

    Priority (high to low): ``spinner + phase``, then ``bar + counter``, then
    the elapsed clock, then the pipeline name. The bar flexes between
    :data:`_MIN_BAR` and :data:`_MAX_BAR` columns; the counter is never
    dropped. When even the mandatory head overflows, the returned text is left
    for the caller to hard-truncate (it always clamps to ``width``).

    When ``total <= 0`` the flowgroup bar and counter are omitted entirely (the
    total is not yet known, or the path does not drive the counter): the line
    is just ``spinner + phase`` plus the optional trailers. ``spinner`` is the
    live animated frame for this tick (see the module docstring).

    Returned as a styled :class:`~rich.text.Text`; segment widths are computed
    on the plain string (every glyph is single-cell).
    """
    width = max(0, width)
    head = f"{spinner} {phase} " if phase else f"{spinner} "
    elapsed = f"{elapsed_s:.1f}s"

    if total <= 0:
        return _build_spinner_only_line(
            head=head, pipeline=pipeline, elapsed=elapsed, width=width
        )

    counter = _counter(done, total)

    # Reserve the counter at its TERMINAL width (`total/total`) rather than its
    # current width. `done` widens the counter as it crosses powers of ten
    # (`9/18` -> `10/18`), and that would otherwise shrink the bar by a cell
    # mid-run. Reserving the widest it will ever be keeps `bar_span` — and the
    # bar's left edge — fixed for the whole run, so the fill is the only thing
    # that moves frame-to-frame.
    counter_reserve = len(_counter(total, total))

    # Mandatory minimum = head + smallest bar + gutter + reserved counter.
    mandatory = len(head) + _MIN_BAR + len(_GUTTER) + counter_reserve

    # `bar_span` is computed from this fixed budget (width minus the mandatory
    # reserve), so it does NOT flex as the trailers appear/disappear or as the
    # counter widens — those only ever consume the slack LEFT OVER after the bar
    # has grown to _MAX_BAR. Grow the bar first (up to _MAX_BAR), then the slack
    # feeds elapsed, then pipeline, in _append_optionals.
    slack = width - mandatory
    bar_span = _MIN_BAR
    if slack > 0:
        grow = min(_MAX_BAR - _MIN_BAR, slack)
        bar_span += max(0, grow)
        slack -= max(0, grow)

    bar = _bar(done, total, bar_span)

    text = Text()
    text.append(head)
    text.append(bar, style="dim")
    text.append(_GUTTER)
    text.append(counter, style="bold")
    _append_optionals(text, pipeline=pipeline, elapsed=elapsed, slack=slack)
    return text


def build_flavor_line(spinner: str, word: str, width: int) -> Text:
    """Build the second line: two-space indent, red ``spinner`` frame, dim ``word``.

    The flavor line sits under the status line in the fixed two-line region. It
    is hard-truncated (never wrapped) to ``width`` so a mid-tick resize can
    never push it onto a second visual row — the same width contract the status
    line above it obeys, applied independently here.

    The line is styled in two segments: the leading indent and the trailing
    ``word`` are ``dim`` (secondary motion, never primary data), while the
    ``spinner`` frame itself is ``red`` so the spinning lead-in catches the eye
    as the one moving accent on an otherwise dim line.

    ``spinner`` is the CURRENT animated lead-in frame for this tick (a single
    circle quadrant from :data:`._flavor_words.SPINNER_FRAMES`), not a fixed
    glyph — animation is the caller's concern; this module only lays the frame
    out, the same way :func:`build_status_line` treats line 1's spinner.

    Every glyph is single-cell (the circle-quadrant ``spinner`` frame and the
    ASCII ``word`` from :mod:`._flavor_words`), so the plain-string length
    equals the cell width and truncating on character count is a faithful width
    clamp.
    """
    width = max(0, width)
    text = Text("  ", style="dim")
    text.append(spinner, style="red")
    text.append(f" {word}", style="dim")
    text.truncate(width, overflow="crop")
    return text

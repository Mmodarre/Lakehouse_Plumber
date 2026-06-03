"""Status-bar glyphs and the priority-truncation budget for the live status line.

The single-line status bar rendered by :class:`..live_renderer.LiveRenderer`
must always fit ``console.width`` (resize-safe) while keeping the load-bearing
pieces visible. :func:`build_status_line` packs the bar to a width budget with
a fixed priority: the spinner, phase label, progress bar, and ``done/total``
counter are never dropped; the trailing pipeline name truncates first, then the
elapsed clock.

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

# Single-cell glyphs (constitution STYLE.md §4 plus the spinner/bar from the
# spec §4.3). All width 1, so len(str) == cell width on the assembled line.
GLYPH_SPINNER = "⠹"
GLYPH_CHECK = "✓"
GLYPH_CROSS = "✗"
GLYPH_WARN = "⚠"
_BAR_FULL = "█"
_BAR_EMPTY = "░"

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
        filled = round(span * min(done, total) / total)
        filled = max(0, min(span, filled))
    return _BAR_FULL * filled + _BAR_EMPTY * (span - filled)


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
    """Build the one-line status bar within a ``width`` character budget.

    Priority (high to low): ``spinner + phase``, then ``bar + counter``, then
    the elapsed clock, then the pipeline name. The bar flexes between
    :data:`_MIN_BAR` and :data:`_MAX_BAR` columns; the counter is never
    dropped. When even the mandatory head overflows, the returned text is left
    for the caller to hard-truncate (it always clamps to ``width``).

    Returned as a styled :class:`~rich.text.Text`; segment widths are computed
    on the plain string (every glyph is single-cell).
    """
    width = max(0, width)
    counter = _counter(done, total)
    head = f"{spinner} {phase} " if phase else f"{spinner} "
    elapsed = f"{elapsed_s:.1f}s"

    # Mandatory minimum = head + smallest bar + gutter + counter.
    mandatory = len(head) + _MIN_BAR + len(_GUTTER) + len(counter)

    # Budget left for the bar to grow and for the optional trailing segments.
    # Grow the bar first (up to _MAX_BAR), then add elapsed, then pipeline.
    slack = width - mandatory
    bar_span = _MIN_BAR
    if slack > 0:
        grow = min(_MAX_BAR - _MIN_BAR, slack)
        bar_span += max(0, grow)
        slack -= max(0, grow)

    show_elapsed = False
    elapsed_cost = len(_GUTTER) + len(elapsed)
    if slack >= elapsed_cost:
        show_elapsed = True
        slack -= elapsed_cost

    show_pipeline = False
    if pipeline:
        pipeline_cost = len(_GUTTER) + len(pipeline)
        if slack >= pipeline_cost:
            show_pipeline = True
            slack -= pipeline_cost

    bar = _bar(done, total, bar_span)

    text = Text()
    text.append(head)
    text.append(bar, style="dim")
    text.append(_GUTTER)
    text.append(counter, style="bold")
    # Layout order matches the spec example (pipeline before elapsed); the
    # pipeline is the first to be dropped as width shrinks, elapsed second.
    if show_pipeline and pipeline:
        text.append(_GUTTER)
        text.append(pipeline)
    if show_elapsed:
        text.append(_GUTTER)
        text.append(elapsed, style="dim")
    return text

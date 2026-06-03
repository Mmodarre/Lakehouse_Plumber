"""TTY-renderer tests for :class:`LiveRenderer`.

Drives the renderer with an injected ``Console(force_terminal=True, width=W)``
and asserts the width-safety and content invariants of the spec (§4.2/§4.3):

- every status frame fits ``W`` (resize-safe) at narrow and wide widths;
- the bar + counter survive at the narrowest width;
- a failed phase carries the cross glyph on its permanent stage line;
- a fully successful run prints NO permanent success line;
- an ``ErrorEmitted`` tears the Live down and lets the exception propagate.
"""

from __future__ import annotations

import io

import pytest
from rich.console import Console

from lhp.cli.presenters.event_stream._event_dispatch import drive
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_CHECK,
    GLYPH_CROSS,
)
from lhp.cli.presenters.event_stream.live_renderer import LiveRenderer

# Test module (not under cli/presenters/**), so it MAY import lhp.errors — used
# only to narrow the expected propagating exception. The renderer under test
# never imports lhp.errors (sole-bridge invariant, §9.5).
from lhp.errors import LHPError
from tests.cli.presenters.event_stream._fixtures import (
    clean_generate_stream,
    error_raise_stream,
    one_failure_stream,
)


def _terminal_console(width: int) -> tuple[Console, io.StringIO]:
    """A forced-terminal Console at a fixed width, capturing to a buffer."""
    buf = io.StringIO()
    console = Console(
        file=buf,
        force_terminal=True,
        width=width,
        height=24,
        color_system=None,
    )
    return console, buf


# ---------------------------------------------------------------------------
# Width safety
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("width", [30, 40, 200])
def test_every_status_frame_fits_width(width):
    console, _ = _terminal_console(width)
    renderer = LiveRenderer(console, total=2)

    drive(iter(clean_generate_stream()), renderer)

    assert renderer.status_frames, "expected at least one status frame"
    for frame in renderer.status_frames:
        assert len(frame) <= width, (
            f"status frame {frame!r} exceeds width {width}: len={len(frame)}"
        )


def test_bar_and_counter_present_at_narrow_width():
    width = 30
    console, _ = _terminal_console(width)
    renderer = LiveRenderer(console, total=2)

    drive(iter(clean_generate_stream()), renderer)

    # EVERY frame must keep the bar + counter even at this narrow width.
    bar_glyphs = {"█", "░"}
    assert renderer.status_frames
    for frame in renderer.status_frames:
        assert any(g in frame for g in bar_glyphs), (
            f"frame dropped the bar at width {width}: {frame!r}"
        )
        assert "/" in frame, f"frame dropped the counter at width {width}: {frame!r}"
    # And the counter reaches its terminal n/m value.
    assert any("2/2" in frame for frame in renderer.status_frames)


# ---------------------------------------------------------------------------
# Content invariants
# ---------------------------------------------------------------------------
def test_failed_phase_renders_cross_on_stage_line():
    # one_failure_stream ends its 'generate' phase with success=False.
    console, _ = _terminal_console(120)
    renderer = LiveRenderer(console, total=2)

    drive(iter(one_failure_stream()), renderer)

    # The failed phase's permanent stage line carries the cross glyph next to
    # the phase name.
    stage_line = next(
        (
            line
            for line in renderer.permanent_lines
            if line.startswith(GLYPH_CROSS) and "generate" in line
        ),
        None,
    )
    assert stage_line is not None, (
        f"no crossed 'generate' stage line; permanent={renderer.permanent_lines!r}"
    )
    # The failing pipeline's failure line is recorded as a FailureLine.
    assert any(f.pipeline == "silver" for f in renderer.outcome.failures)


def test_success_path_prints_no_permanent_success_line():
    console, _ = _terminal_console(120)
    renderer = LiveRenderer(console, total=2)

    drive(iter(clean_generate_stream()), renderer)

    # Successful pipelines flash only in the (transient) status line, so their
    # names must never appear on any PERMANENT line.
    for line in renderer.permanent_lines:
        assert "bronze" not in line, f"permanent success line for bronze: {line!r}"
        assert "silver" not in line, f"permanent success line for silver: {line!r}"
    # No permanent line is a check-marked per-pipeline success line. (Stage
    # lines may carry the check glyph, but they name a phase, not a pipeline.)
    assert any(GLYPH_CHECK in line for line in renderer.permanent_lines), (
        "expected permanent stage lines to use the check glyph"
    )


# ---------------------------------------------------------------------------
# Error rendezvous
# ---------------------------------------------------------------------------
def test_error_emitted_tears_down_live_and_propagates():
    console, _ = _terminal_console(120)
    renderer = LiveRenderer(console)

    # error_raise_stream() yields ErrorEmitted then RAISES the LHPError; the
    # raise must propagate out of drive(), and the Live must be torn down.
    with pytest.raises(LHPError):
        drive(error_raise_stream(), renderer)

    assert renderer.outcome.errored is True
    # Live is torn down (no panel printed here — error_boundary owns the panel
    # on a clean stderr). Assert via both the renderer's own teardown flag and
    # Rich's Live.is_started.
    assert renderer._started is False
    assert renderer._live.is_started is False

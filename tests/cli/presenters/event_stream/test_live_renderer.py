"""TTY-renderer tests for :class:`LiveRenderer` and its animated renderable.

Drives the renderer with an injected ``Console(force_terminal=True, width=W)``
and asserts the width-safety, animation, and progress invariants of the spec
(§4.2/§4.3):

- the renderable yields distinct spinner frames across simulated time (it
  animates — it is not a frozen glyph);
- the flowgroup counter advances as the shared :class:`~lhp.api.ProgressSink`
  advances (the bar reads the sink, never an internal event counter);
- every composed status frame fits ``console.width`` (resize-safe) at narrow
  and wide widths;
- the rendered count never exceeds ``total`` across interleaved advances
  (lock-free read-after-advance consistency);
- a failed phase carries the cross glyph on its permanent stage line;
- a fully successful run prints NO permanent success line;
- an ``ErrorEmitted`` tears the Live down and lets the exception propagate.

The renderer reads progress off the SAME :class:`~lhp.api.ProgressSink` the
facade advances per-flowgroup. These tests have no facade, so they drive the
sink directly — setting the total on ``OperationStarted`` and advancing it once
per pipeline completion, mirroring what the facade does in a real run.
"""

from __future__ import annotations

import io
from typing import Iterator, List

import pytest
from rich.console import Console

from lhp.api import ProgressSink
from lhp.api.events import (
    LHPEvent,
    OperationStarted,
    PipelineCompleted,
    PipelineFailed,
)
from lhp.cli.presenters.event_stream._event_dispatch import drive
from lhp.cli.presenters.event_stream._live_renderable import _LiveStatus
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


def _drive_with_progress(
    events: List[LHPEvent], sink: ProgressSink
) -> Iterator[LHPEvent]:
    """Yield ``events`` while advancing ``sink`` like the facade would.

    Sets the run total on ``OperationStarted`` (one flowgroup per pipeline
    completion in these fixtures) and calls :meth:`ProgressSink.on_advance`
    after each ``PipelineCompleted`` / ``PipelineFailed``, so the live bar that
    reads the sink reflects real progress as the stream drains.
    """
    total = sum(isinstance(e, (PipelineCompleted, PipelineFailed)) for e in events)
    for event in events:
        if isinstance(event, OperationStarted):
            sink.on_total(total)
        yield event
        if isinstance(event, (PipelineCompleted, PipelineFailed)):
            sink.on_advance()


# ---------------------------------------------------------------------------
# Animation: the spinner is not a frozen glyph
# ---------------------------------------------------------------------------
def test_renderable_yields_distinct_spinner_frames_across_time():
    console, _ = _terminal_console(80)
    sink = ProgressSink()
    status = _LiveStatus(sink, console)

    # render_frame is a pure function of its time argument; sample at three
    # well-separated times and require at least two DISTINCT leading glyphs
    # (the spinner cell is the first character of the line).
    base = status._start
    glyphs = {status.render_frame(base + dt).plain[:1] for dt in (0.0, 0.4, 0.8, 1.2)}
    assert len(glyphs) >= 2, f"spinner did not animate across time; saw only {glyphs!r}"


def test_default_construction_uses_animated_spinner_not_static_glyph():
    # A frozen glyph would yield exactly one distinct leading character across
    # all times. The animated Spinner must produce more than one.
    console, _ = _terminal_console(80)
    status = _LiveStatus(ProgressSink(), console)
    base = status._start
    frames = [status.render_frame(base + i * 0.1).plain[:1] for i in range(12)]
    assert len(set(frames)) >= 2, f"spinner frozen at {set(frames)!r}"


# ---------------------------------------------------------------------------
# Progress: the bar reads the ProgressSink and advances with it
# ---------------------------------------------------------------------------
def test_counter_advances_as_sink_advances():
    console, _ = _terminal_console(80)
    sink = ProgressSink()
    status = _LiveStatus(sink, console)
    status.phase = "generate"

    sink.on_total(3)
    assert "0/3" in status.render_frame(status._start).plain

    sink.on_advance()
    assert "1/3" in status.render_frame(status._start).plain

    sink.on_advance()
    sink.on_advance()
    assert "3/3" in status.render_frame(status._start).plain


def test_bar_hidden_before_total_known():
    # total == 0 (startup / "discovering" window, or the diff/plan path) must
    # NOT show a 0/0 bar — just the spinner + phase.
    console, _ = _terminal_console(80)
    status = _LiveStatus(ProgressSink(), console)
    status.phase = "discovering"

    frame = status.render_frame(status._start).plain
    assert "0/0" not in frame, f"misleading 0/0 bar before total known: {frame!r}"
    assert "█" not in frame and "░" not in frame, f"bar drawn at total=0: {frame!r}"
    assert "discovering" in frame


def test_count_never_exceeds_total_across_advances():
    # Read-after-advance consistency: the rendered count never exceeds total,
    # and reflects each advance (lock-free independent-scalar reads).
    console, _ = _terminal_console(80)
    sink = ProgressSink()
    status = _LiveStatus(sink, console)
    status.phase = "generate"

    sink.on_total(4)
    for expected_done in range(5):  # 0..4
        frame = status.render_frame(status._start).plain
        assert f"{expected_done}/4" in frame, (
            f"expected {expected_done}/4, got {frame!r}"
        )
        done_part = int(frame.split("/4")[0].split()[-1])
        assert done_part <= 4, f"rendered count {done_part} exceeds total 4: {frame!r}"
        if expected_done < 4:
            sink.on_advance()


# ---------------------------------------------------------------------------
# Width safety (end-to-end through drive, sink advanced like the facade)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("width", [30, 40, 200])
def test_every_status_frame_fits_width(width):
    console, _ = _terminal_console(width)
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(clean_generate_stream(), sink), renderer)

    assert renderer.status_frames, "expected at least one status frame"
    for frame in renderer.status_frames:
        assert len(frame) <= width, (
            f"status frame {frame!r} exceeds width {width}: len={len(frame)}"
        )


def test_bar_and_counter_present_once_total_known():
    width = 30
    console, _ = _terminal_console(width)
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(clean_generate_stream(), sink), renderer)

    # Once the total is known (and it is, immediately on OperationStarted here),
    # the bar + counter must survive even at this narrow width.
    bar_glyphs = {"█", "░"}
    counter_frames = [f for f in renderer.status_frames if "/" in f]
    assert counter_frames, "no frame ever carried the counter"
    for frame in counter_frames:
        assert any(g in frame for g in bar_glyphs), (
            f"frame dropped the bar at width {width}: {frame!r}"
        )
    # And the counter reaches its terminal n/m value.
    assert any("2/2" in frame for frame in renderer.status_frames)


# ---------------------------------------------------------------------------
# Content invariants
# ---------------------------------------------------------------------------
def test_failed_phase_renders_cross_on_stage_line():
    # one_failure_stream ends its 'generate' phase with success=False.
    console, _ = _terminal_console(120)
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(one_failure_stream(), sink), renderer)

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
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(clean_generate_stream(), sink), renderer)

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
# begin(): first frame painted before the first event is pulled
# ---------------------------------------------------------------------------
def test_begin_starts_live_and_paints_starting_frame_before_any_event():
    # begin() is fired by render() BEFORE the stream's first next() (which may
    # block on discovery), so the Live must be started and a non-0/0 "starting"
    # frame recorded without a single event having been dispatched.
    console, _ = _terminal_console(80)
    renderer = LiveRenderer(console)

    renderer.begin()

    assert renderer._started is True
    assert renderer._live.is_started is True
    assert renderer.status_frames, "begin() recorded no initial frame"
    first = renderer.status_frames[0]
    assert "starting" in first, f"initial frame lacks a phase label: {first!r}"
    # total is still 0 here -> the total>0 guard collapses to spinner + phase
    # (no misleading 0/0 bar).
    assert "0/0" not in first, f"misleading 0/0 bar in the startup frame: {first!r}"
    assert "█" not in first and "░" not in first, f"bar drawn at total=0: {first!r}"


def test_begin_then_operation_started_label_advances_off_starting():
    # The first real PhaseStarted overwrites the placeholder label moments
    # after begin().
    console, _ = _terminal_console(80)
    renderer = LiveRenderer(console)

    renderer.begin()
    renderer.on_operation_started("generate", "dev")
    renderer.on_phase_started("discover")

    assert "discover" in renderer.status_frames[-1]


# ---------------------------------------------------------------------------
# Incomplete-phase trace: a started phase with no PhaseCompleted is surfaced
# ---------------------------------------------------------------------------
def test_incomplete_phase_leaves_visible_trace_on_teardown():
    # OperationStarted -> PhaseStarted("generate") -> teardown WITHOUT a
    # matching PhaseCompleted (§5.7 pairing broken). The phase must not vanish
    # with the transient bar: a permanent "did not complete" trace line names
    # it on teardown.
    console, _ = _terminal_console(80)
    renderer = LiveRenderer(console)

    renderer.begin()
    renderer.on_operation_started("generate", "dev")
    renderer.on_phase_started("generate")
    # on_terminal is the realistic "stream ended" teardown (no PhaseCompleted
    # for 'generate' ever arrived).
    renderer.on_terminal(None)

    trace = next(
        (line for line in renderer.permanent_lines if "generate" in line),
        None,
    )
    assert trace is not None, (
        f"interrupted phase left no trace; permanent={renderer.permanent_lines!r}"
    )
    assert "did not complete" in trace, (
        f"trace does not mark the phase incomplete: {trace!r}"
    )
    assert renderer._started is False


def test_completed_phase_leaves_no_incomplete_trace():
    # The complement: a phase that DID complete must not also get a "did not
    # complete" line on teardown (no false positives).
    console, _ = _terminal_console(80)
    renderer = LiveRenderer(console)

    renderer.begin()
    renderer.on_operation_started("generate", "dev")
    renderer.on_phase_started("generate")
    renderer.on_phase_completed("generate", 0.3, True)
    renderer.on_terminal(None)

    assert not any("did not complete" in line for line in renderer.permanent_lines), (
        f"completed phase wrongly surfaced as incomplete: {renderer.permanent_lines!r}"
    )


def test_error_teardown_does_not_print_incomplete_phase_trace():
    # The error path stays quiet: error_boundary owns the panel on a clean
    # stderr (§9.5), so an open phase must NOT be surfaced when on_error tears
    # the Live down — otherwise the panel talks over renderer noise.
    console, _ = _terminal_console(80)
    renderer = LiveRenderer(console)

    renderer.begin()
    renderer.on_operation_started("generate", "dev")
    renderer.on_phase_started("generate")
    renderer.on_error("LHP-VAL-021")

    assert renderer.outcome.errored is True
    assert renderer._started is False
    assert not any("did not complete" in line for line in renderer.permanent_lines), (
        f"error path printed an incomplete-phase trace: {renderer.permanent_lines!r}"
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

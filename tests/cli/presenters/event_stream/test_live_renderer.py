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
from typing import Callable, Iterator, List

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
from lhp.cli.presenters.event_stream._flavor_words import (
    _DEFAULT_PERIOD_S,
    _DEFAULT_SPINNER_PERIOD_S,
    SPINNER_FRAMES,
    flavor_spinner_frame_for,
    flavor_word_count,
    flavor_word_for,
)
from lhp.cli.presenters.event_stream._live_renderable import _LiveStatus
from lhp.cli.presenters.event_stream._status_line import (
    GLYPH_CHECK,
    GLYPH_CROSS,
    build_status_line,
)
from lhp.cli.presenters.event_stream.live_renderer import LiveRenderer

# Test module (not under cli/presenters/**), so it MAY import lhp.errors — used
# only to narrow the expected propagating exception. The renderer under test
# never imports lhp.errors (sole-bridge invariant, §9.5).
from lhp.errors import LHPError
from tests.cli.presenters.event_stream._fixtures import (
    clean_generate_stream,
    clean_plan_stream,
    clean_validate_stream,
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


def _has_flavor_lead(s: str) -> bool:
    """True if ``s`` carries any line-2 spinner frame (a circle quadrant).

    The flavor line's lead-in is the ONLY source of a circle quadrant in the
    region — line 1 uses braille dots, the check/cross/warn glyphs, and the bar
    blocks, none of which overlap :data:`SPINNER_FRAMES`. So "any quadrant
    present" is a frame-agnostic marker that the flavor line was rendered,
    regardless of which animation frame a given Live tick happened to sample.
    """
    return any(frame in s for frame in SPINNER_FRAMES)


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
# Fixed two-line region: line 1 status (unchanged), line 2 rotating flavor
# ---------------------------------------------------------------------------
def _region_lines(status: _LiveStatus, console: Console) -> list[str]:
    """Render the renderable through the console and return its content lines.

    This is what ``Live`` does each tick — invoke ``__rich_console__`` via the
    console. The trailing empty line that ``console.print`` appends after the
    final renderable is dropped, leaving only the region's own rows.
    """
    with console.capture() as cap:
        console.print(status, end="")
    lines = cap.get().split("\n")
    while lines and lines[-1] == "":
        lines.pop()
    return lines


def _fixed_clock(value: float) -> Callable[[], float]:
    """A clock callable that always returns ``value`` (no time progression)."""
    return lambda: value


@pytest.mark.parametrize("width", [30, 40, 200])
def test_region_is_exactly_two_lines_each_within_width(width):
    # The renderable must yield a FIXED two-line region (line 1 status, line 2
    # flavor), and each line must fit console.width independently so a mid-tick
    # resize can never wrap the region taller than two rows.
    console, _ = _terminal_console(width)
    status = _LiveStatus(ProgressSink(), console, clock=_fixed_clock(1000.0))
    status.phase = "generate"
    status._progress.on_total(18)
    for _ in range(12):
        status._progress.on_advance()

    lines = _region_lines(status, console)
    assert len(lines) == 2, f"expected exactly two lines, got {len(lines)}: {lines!r}"
    for i, line in enumerate(lines, start=1):
        # Forced-terminal Console pads each row to full width; the load-bearing
        # invariant is that nothing exceeds width (no wrap to a third row).
        assert len(line) <= width, (
            f"region line {i} exceeds width {width}: len={len(line)} {line!r}"
        )


@pytest.mark.parametrize("width", [30, 40, 200])
def test_line_one_is_byte_identical_to_standalone_status_line(width):
    # current_frame() (line 1) must be byte-identical to what build_status_line
    # produces directly for the same state — the two-line change must not alter
    # line 1 by a single character.
    console, _ = _terminal_console(width)
    clock_value = 1000.0
    status = _LiveStatus(ProgressSink(), console, clock=_fixed_clock(clock_value))
    status.phase = "generate"
    status.pipeline = "silver_orders"
    status._progress.on_total(18)
    for _ in range(12):
        status._progress.on_advance()

    elapsed = clock_value - status._start
    spinner_frame = status._spinner.render(elapsed)
    spinner = (
        spinner_frame.plain if hasattr(spinner_frame, "plain") else str(spinner_frame)
    )
    expected = build_status_line(
        spinner=spinner,
        phase="generate",
        done=12,
        total=18,
        pipeline="silver_orders",
        elapsed_s=elapsed,
        width=width,
    )
    expected.truncate(width, overflow="crop")

    assert status.current_frame().plain == expected.plain


def test_line_two_is_indented_dim_flavor_with_spinner_lead_glyph():
    # Line 2 is the flavor line: two-space indent, the animated circle-quadrant
    # spinner lead-in (one of ◐◓◑◒), then a flavor word — and rendered dim
    # (secondary motion, never primary data).
    console, _ = _terminal_console(80)
    status = _LiveStatus(ProgressSink(), console, clock=_fixed_clock(1000.0))

    flavor = status.current_flavor()
    plain = flavor.plain
    assert plain.startswith("  "), f"flavor line missing two-space indent: {plain!r}"
    assert plain[2] in SPINNER_FRAMES, (
        f"flavor line lead-in is not a circle quadrant: {plain!r}"
    )
    assert plain[3] == " ", f"flavor line missing space after spinner: {plain!r}"
    assert len(plain) > 4, "flavor line carries no word"
    # The whole flavor line is dim.
    assert flavor.style == "dim", f"flavor line not dim: style={flavor.style!r}"


def test_line_two_rotates_as_clock_advances():
    # The flavor word is a function of the elapsed clock: advancing the clock
    # across flavor periods must change line 2 (it is alive, not frozen).
    console, _ = _terminal_console(200)
    now = {"t": 1000.0}
    status = _LiveStatus(ProgressSink(), console, clock=lambda: now["t"])

    words = []
    for k in range(6):
        now["t"] = 1000.0 + k * 2.0  # one default flavor period per step
        words.append(status.current_flavor().plain)

    assert len(set(words)) >= 2, f"flavor line did not rotate across time: {words!r}"


def test_line_two_shows_specific_words_at_specific_clock_values():
    # Deterministic rotation under the injected clock: at known elapsed offsets
    # the flavor line carries SPECIFIC words (not merely "some rotation"). The
    # clock base is the renderable's _start; offsetting it by whole default
    # periods (2.0s) walks consecutive buckets through the fixed _ORDER
    # permutation, so each step's word is pinned, not just distinct.
    console, _ = _terminal_console(200)
    now = {"t": 0.0}
    # Pin BOTH per-run offsets to 0 so the rotation starts unshifted: this test
    # asserts SPECIFIC words/frames at specific elapsed values, which is only
    # well-defined at offset 0 (the production constructor draws a RANDOM start).
    status = _LiveStatus(
        ProgressSink(),
        console,
        clock=lambda: now["t"],
        spinner_offset=0,
        word_offset=0,
    )
    base = status._start  # captured from clock() at construction (== 0.0 here)

    # elapsed = k * _DEFAULT_PERIOD_S selects bucket k -> _FLAVOR_WORDS[_ORDER[k]].
    # These literals are the contract of _flavor_words._ORDER; a permutation edit
    # that silently reorders the rotation must fail HERE, not just "still rotates".
    expected_by_elapsed = {
        0.0: "pouring the foundation",  # bucket 0 -> _ORDER[0] == 7
        2.0: "tuning the manifolds",  # bucket 1 -> _ORDER[1] == 18
        4.0: "connecting pipes",  # bucket 2 -> _ORDER[2] == 2
        6.0: "unclogging the drains",  # bucket 3 -> _ORDER[3] == 25
    }
    for elapsed, word in expected_by_elapsed.items():
        now["t"] = base + elapsed
        flavor = status.current_flavor().plain
        # The word sits after the "  <spinner> " indent + animated lead-in; the
        # spinner frame is its own deterministic function of the same elapsed
        # clock, so pin the WHOLE line (indent + exact frame + exact word).
        spinner = flavor_spinner_frame_for(elapsed)
        assert spinner in SPINNER_FRAMES
        assert flavor == f"  {spinner} {word}", (
            f"at elapsed {elapsed}s expected flavor {f'  {spinner} {word}'!r}, "
            f"got {flavor!r}"
        )

    # The lead-in advances at its OWN (sub-second) spin period, distinctly from
    # the 2.0s word rotation: sampling across one spinner period changes the
    # leading glyph even while the word is unchanged.
    spin_frames = set()
    for i in range(len(SPINNER_FRAMES)):
        now["t"] = base + i * _DEFAULT_SPINNER_PERIOD_S
        spin_frames.add(status.current_flavor().plain[2])
    assert len(spin_frames) >= 2, (
        f"line-2 spinner did not advance at its spin period: {spin_frames!r}"
    )


# ---------------------------------------------------------------------------
# Per-run random START offset for line 2 (the "looks the same every run" fix):
# the renderable draws a per-RUN spinner/word phase ONCE at construction, so two
# runs begin on different quadrants/words. The draw is injectable so tests stay
# deterministic; production (offset=None) draws into [0, len) via random.
# ---------------------------------------------------------------------------
def test_injected_offsets_reproduce_a_known_line_two_sequence():
    # With BOTH offsets pinned, line 2 is a pure function of the injected clock:
    # at each whole-period step it must equal build_flavor_line's layout of the
    # offset-shifted frame + word. A non-zero offset proves the shift is applied
    # (the run does NOT start on quadrant 0 / word 0), yet stays deterministic.
    console, _ = _terminal_console(200)
    now = {"t": 0.0}
    spinner_offset, word_offset = 2, 5
    status = _LiveStatus(
        ProgressSink(),
        console,
        clock=lambda: now["t"],
        spinner_offset=spinner_offset,
        word_offset=word_offset,
    )
    base = status._start

    for k in range(6):
        elapsed = k * _DEFAULT_PERIOD_S
        now["t"] = base + elapsed
        expected_spinner = flavor_spinner_frame_for(elapsed, offset=spinner_offset)
        expected_word = flavor_word_for(elapsed, offset=word_offset)
        assert status.current_flavor().plain == f"  {expected_spinner} {expected_word}"

    # The very first frame is NOT the unshifted start (offset is really applied).
    now["t"] = base
    assert (
        status.current_flavor().plain != f"  {SPINNER_FRAMES[0]} {flavor_word_for(0.0)}"
    )


def test_injected_offsets_are_honored_verbatim():
    # An int offset is stored verbatim (not redrawn), so tests can pin a phase.
    console, _ = _terminal_console(80)
    status = _LiveStatus(
        ProgressSink(),
        console,
        clock=_fixed_clock(1000.0),
        spinner_offset=3,
        word_offset=11,
    )
    assert status._spinner_offset == 3
    assert status._word_offset == 11


def test_default_offsets_are_drawn_into_range_not_a_fixed_zero():
    # In production (offset=None) the renderable draws each offset ONCE into its
    # valid band: [0, len(SPINNER_FRAMES)) and [0, flavor_word_count()). We do
    # NOT assert two runs differ (that would be flaky); we assert the drawn
    # attributes are in range, which is the deterministic, non-flaky contract.
    console, _ = _terminal_console(80)
    for _ in range(50):  # many draws: each must land in range every time
        status = _LiveStatus(ProgressSink(), console, clock=_fixed_clock(1000.0))
        assert 0 <= status._spinner_offset < len(SPINNER_FRAMES)
        assert 0 <= status._word_offset < flavor_word_count()


def test_offsets_are_drawn_once_and_frozen_for_the_run():
    # The per-run offset is fixed at construction: re-reading across renders
    # never changes it (a per-FRAME draw would jitter the spinner and break the
    # clock-injected tests). Render at several clocks; the stored offsets hold.
    console, _ = _terminal_console(80)
    now = {"t": 0.0}
    status = _LiveStatus(ProgressSink(), console, clock=lambda: now["t"])
    first_spinner, first_word = status._spinner_offset, status._word_offset
    for k in range(8):
        now["t"] = k * _DEFAULT_SPINNER_PERIOD_S
        status.current_flavor()
        assert status._spinner_offset == first_spinner
        assert status._word_offset == first_word


def test_line_two_spinner_is_red_and_word_is_dim():
    # The spinner lead-in is the one RED accent on an otherwise dim line: the
    # span covering the spinner glyph (plain index 2) is styled "red", and the
    # span covering a word character is styled "dim". Inspect the Text spans.
    console, _ = _terminal_console(80)
    status = _LiveStatus(
        ProgressSink(),
        console,
        clock=_fixed_clock(1000.0),
        spinner_offset=0,
        word_offset=0,
    )
    flavor = status.current_flavor()
    plain = flavor.plain
    assert plain.startswith("  "), f"flavor line missing two-space indent: {plain!r}"
    assert plain[2] in SPINNER_FRAMES, f"flavor lead-in not a quadrant: {plain!r}"
    word_idx = 4  # first character of the word (after "  <spinner> ")

    def styles_at(text, i):
        return [str(span.style) for span in text.spans if span.start <= i < span.end]

    assert "red" in styles_at(flavor, 2), (
        f"spinner glyph not styled red; spans={flavor.spans!r}"
    )
    assert "dim" in styles_at(flavor, word_idx), (
        f"flavor word not styled dim; spans={flavor.spans!r}"
    )
    # The spinner is NOT also dim, and the word is NOT also red (clean split).
    assert "dim" not in styles_at(flavor, 2), f"spinner wrongly dim: {flavor.spans!r}"
    assert "red" not in styles_at(flavor, word_idx), (
        f"word wrongly red: {flavor.spans!r}"
    )


def test_line_one_unchanged_while_line_two_rotates():
    # Independence: as the clock advances and line 2 rotates, line 1's content
    # (phase + bar + counter) tracks only the sink, not the flavor rotation.
    console, _ = _terminal_console(200)
    now = {"t": 1000.0}
    status = _LiveStatus(ProgressSink(), console, clock=lambda: now["t"])
    status.phase = "generate"
    status._progress.on_total(4)
    status._progress.on_advance()  # 1/4, fixed for the whole test

    flavors = set()
    for k in range(6):
        now["t"] = 1000.0 + k * 2.0
        assert "1/4" in status.current_frame().plain, "line 1 counter drifted"
        flavors.add(status.current_flavor().plain)
    assert len(flavors) >= 2, f"line 2 did not rotate: {flavors!r}"


# ---------------------------------------------------------------------------
# Tiny-terminal resize safety: the fixed two-line region must degrade
# gracefully at a sub-2-row viewport (vertical_overflow="crop"), never crash
# and never scroll/clip the primary data off the top.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("height", [1, 2])
def test_region_crops_to_viewport_without_scroll_at_tiny_height(height):
    # This is exactly what Live does each tick: LiveRender(crop) slices the
    # region to options.size.height. At a 1-row viewport the two-line region
    # must collapse to its TOP row (line 1, the status) verbatim — not scroll,
    # not wrap to two rows, not get replaced by an ellipsis. So the bar/counter
    # (primary data) survive a terminal too short to hold the whole region.
    from rich.live_render import LiveRender

    buf = io.StringIO()
    console = Console(
        file=buf, force_terminal=True, width=80, height=height, color_system=None
    )
    status = _LiveStatus(ProgressSink(), console, clock=_fixed_clock(1000.0))
    status.phase = "generate"
    status._progress.on_total(3)
    status._progress.on_advance()

    live_render = LiveRender(status, vertical_overflow="crop")
    with console.capture() as cap:
        console.print(live_render, end="")
    rows = [r for r in cap.get().split("\n") if r.strip()]

    # Cropped to the viewport: never taller than `height` (no scroll, no wrap).
    assert len(rows) <= height, f"region exceeded {height}-row viewport: {rows!r}"
    # The TOP row that survives is line 1 — its bar + counter are intact (crop
    # keeps the most important row; it is not erased by an ellipsis or scrolled
    # off the top).
    assert "1/3" in rows[0], f"status line clipped at height={height}: {rows!r}"
    assert any(g in rows[0] for g in ("█", "░")), f"bar clipped: {rows!r}"


@pytest.mark.parametrize("height", [1, 2])
def test_full_drive_does_not_crash_at_tiny_height(height):
    # End-to-end through drive() at a 1-2 row terminal: starting the Live,
    # refreshing the two-line region per event, and tearing it down (which clears
    # BOTH live lines) must not raise even when the viewport cannot hold the
    # whole region.
    buf = io.StringIO()
    console = Console(
        file=buf, force_terminal=True, width=80, height=height, color_system=None
    )
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(clean_generate_stream(), sink), renderer)

    # The run still completes and the permanent stage line(s) are recorded.
    assert renderer.outcome.errored is False
    assert any(GLYPH_CHECK in line for line in renderer.permanent_lines), (
        f"no permanent stage line survived a {height}-row terminal: "
        f"{renderer.permanent_lines!r}"
    )


# ---------------------------------------------------------------------------
# Clean teardown wipes BOTH live lines (transient region), permanent intact
# ---------------------------------------------------------------------------
# Rich's Live.stop() restores the cursor (CSI ?25h) once, then erases the
# transient region by emitting one "cursor-up + erase-line" (CSI 1A, CSI 2K)
# pair PER region row. So the teardown tail after the cursor-show sequence
# carries exactly as many erase pairs as the region is tall — two for the fixed
# two-line region. A one-line region would emit a single pair; counting them is
# how we prove BOTH live lines are wiped, not just the bottom one.
_CURSOR_SHOW = "\x1b[?25h"
_ERASE_PAIR = "\x1b[1A\x1b[2K"


def _teardown_erase_pairs(raw: str) -> int:
    """Count cursor-up+erase pairs in the transient teardown tail of ``raw``.

    The tail is everything after the LAST cursor-show sequence Live emits on
    stop(); each ``CSI 1A CSI 2K`` there clears one region row. Returns 0 when
    no teardown occurred (no cursor-show emitted).
    """
    idx = raw.rfind(_CURSOR_SHOW)
    if idx == -1:
        return 0
    tail = raw[idx + len(_CURSOR_SHOW) :]
    return tail.count(_ERASE_PAIR)


def test_clean_teardown_wipes_both_live_lines_keeping_permanent_scrollback():
    # On a successful full drive, teardown must wipe the WHOLE transient
    # two-line region (line 1 status + line 2 flavor) so successes leave no
    # transient residue, while the permanent single-line scrollback survives.
    # We assert the teardown tail clears exactly two rows (both live lines) and
    # that the permanent stage lines are untouched.
    buf = io.StringIO()
    console = Console(
        file=buf, force_terminal=True, width=80, height=24, color_system=None
    )
    sink = ProgressSink()
    renderer = LiveRenderer(console, progress=sink)

    drive(_drive_with_progress(clean_generate_stream(), sink), renderer)

    raw = buf.getvalue()
    # The flavor line WAS rendered during the run (line 2 of the region)...
    assert _has_flavor_lead(raw), "flavor line never rendered in the live region"
    # ...and teardown clears BOTH region rows: exactly two erase pairs in the
    # teardown tail (a single-line region would clear only one).
    assert _teardown_erase_pairs(raw) == 2, (
        "teardown did not wipe both live lines (expected 2 cursor-up+erase pairs "
        f"in the teardown tail): {raw[-80:]!r}"
    )
    # The Live is fully torn down and the permanent scrollback survives the wipe.
    assert renderer._started is False
    assert renderer._live.is_started is False
    assert renderer.permanent_lines, "permanent scrollback was wiped with the region"
    assert any(GLYPH_CHECK in line for line in renderer.permanent_lines)
    # No permanent line is itself a flavor line — the flavor line is transient.
    assert not any(_has_flavor_lead(line) for line in renderer.permanent_lines), (
        f"flavor line leaked into permanent scrollback: {renderer.permanent_lines!r}"
    )


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
# Validate & diff paths also drive LiveRenderer: flavor line present on BOTH,
# and NEITHER surfaces a 'format' phase (format is generate-only).
# ---------------------------------------------------------------------------
def _drive_capturing(events, sink) -> tuple[LiveRenderer, str]:
    """Drive ``events`` through a fresh LiveRenderer on a forced-terminal buffer.

    Returns the renderer and the raw capture buffer, so a caller can both
    inspect the surfaced permanent lines and confirm the transient flavor line
    was rendered into the live region (its lead glyph appears in the raw stream).
    """
    buf = io.StringIO()
    console = Console(
        file=buf, force_terminal=True, width=120, height=24, color_system=None
    )
    renderer = LiveRenderer(console, progress=sink)
    drive(_drive_with_progress(events, sink), renderer)
    return renderer, buf.getvalue()


def test_validate_path_renders_flavor_line_and_no_format_phase():
    # The validate command drives the SAME LiveRenderer. Its phases are
    # discover/preflight/validate — there is NO 'format' phase (format is
    # generate-only). The two-line region (incl. the flavor line) must still be
    # rendered, and no surfaced stage line may name 'format'.
    sink = ProgressSink()
    renderer, raw = _drive_capturing(clean_validate_stream(), sink)

    # Flavor line (line 2) was rendered into the live region on the validate path.
    assert _has_flavor_lead(raw), (
        "flavor line absent on the validate path; the second region line did not render"
    )
    # The validate stage line is surfaced...
    assert any(
        "validate" in line and GLYPH_CHECK in line for line in renderer.permanent_lines
    ), f"no 'validate' stage line surfaced; permanent={renderer.permanent_lines!r}"
    # ...and NO 'format' phase is ever surfaced (validate has none).
    assert not any("format" in line for line in renderer.permanent_lines), (
        f"validate path surfaced a 'format' stage line: {renderer.permanent_lines!r}"
    )
    assert renderer.outcome.errored is False


def test_diff_plan_path_renders_flavor_line_and_no_format_phase():
    # The diff command drives the SAME LiveRenderer over the plan stream.
    # The plan stream's phases are discover/preflight/generate with NO 'format'
    # phase (the dry-run plan never writes/formats). The flavor line must still
    # render, and no surfaced stage line may name 'format'.
    sink = ProgressSink()
    renderer, raw = _drive_capturing(clean_plan_stream(), sink)

    # Flavor line (line 2) was rendered into the live region on the diff/plan path.
    assert _has_flavor_lead(raw), (
        "flavor line absent on the diff/plan path; the second region line did not render"
    )
    # The generate stage line is surfaced (plan runs a generate phase)...
    assert any(
        "generate" in line and GLYPH_CHECK in line for line in renderer.permanent_lines
    ), f"no 'generate' stage line surfaced; permanent={renderer.permanent_lines!r}"
    # ...and NO 'format' phase is ever surfaced on the plan/diff path.
    assert not any("format" in line for line in renderer.permanent_lines), (
        f"diff/plan path surfaced a 'format' stage line: {renderer.permanent_lines!r}"
    )
    assert renderer.outcome.errored is False


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

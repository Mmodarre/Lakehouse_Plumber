"""Structural tests for ``lhp.cli.live_panel``.

Snapshot testing is avoided because Rich's ``Progress`` calls
``get_time()`` on every render, making frames time-dependent.
"""

import logging
import threading
import time
from io import StringIO
from unittest.mock import patch

from rich.console import Console, Group
from rich.panel import Panel
from rich.progress import Progress
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

from lhp.cli.live_panel import (
    _LHP_WORDMARK,
    ActivityTail,
    HeaderContext,
    OverallProgress,
    PhaseTracker,
    PipelineRecord,
    _get_lhp_version,
    _LiveUpdateCoalescer,
    render_live_frame,
)


def _render_panel_to_text(panel: Panel, width: int = 120) -> str:
    console = Console(file=StringIO(), force_terminal=False, width=width)
    with console.capture() as cap:
        console.print(panel)
    return cap.get()


def _plain_of(child) -> str:
    if isinstance(child, Text):
        return child.plain
    return ""


def _find_renderable(node, target_type):
    if isinstance(node, target_type):
        return node
    if isinstance(node, Group):
        for child in node.renderables:
            found = _find_renderable(child, target_type)
            if found is not None:
                return found
    return None


def test_phase_tracker_start_then_complete_after_sleep_records_entry():
    pt = PhaseTracker()
    pt.start("X")
    time.sleep(0.3)
    pt.complete("X")
    assert len(pt.completed) == 1
    name, duration, success = pt.completed[0]
    assert name == "X"
    assert success is True
    assert duration >= 0.25
    assert pt.active is None


def test_phase_tracker_sub_threshold_complete_suppresses_when_opted_in():
    pt = PhaseTracker()
    pt.start("X")
    pt.complete("X", suppress_if_fast=True)
    assert pt.completed == []
    assert pt.active is None


def test_phase_tracker_default_records_sub_threshold_complete():
    pt = PhaseTracker()
    pt.start("X")
    pt.complete("X")
    assert len(pt.completed) == 1
    assert pt.completed[0][0] == "X"
    assert pt.active is None


def test_phase_tracker_failure_records_red_x_marker():
    pt = PhaseTracker()
    pt.start("X")
    time.sleep(0.3)
    pt.complete("X", success=False)
    assert len(pt.completed) == 1
    assert pt.completed[0][2] is False

    rendered = pt.render()
    assert isinstance(rendered, Group)
    found_cross = any("✗" in _plain_of(child) for child in rendered.renderables)
    assert found_cross, "expected red-cross glyph in rendered failure entry"


def test_phase_tracker_sequential_starts_preserve_order():
    pt = PhaseTracker()
    pt.start("A")
    time.sleep(0.3)
    pt.complete("A")
    pt.start("B")
    time.sleep(0.3)
    pt.complete("B")
    assert [entry[0] for entry in pt.completed] == ["A", "B"]
    assert all(entry[2] is True for entry in pt.completed)
    assert pt.active is None


def test_phase_tracker_render_active_phase_contains_spinner():
    pt = PhaseTracker()
    pt.start("Generation")
    rendered = pt.render()
    assert isinstance(rendered, Group)
    spinners = [c for c in rendered.renderables if isinstance(c, Spinner)]
    assert (
        len(spinners) >= 1
    ), "active phase must render as a Spinner so outer Live animates it"


def test_phase_tracker_complete_unknown_name_is_noop():
    pt = PhaseTracker()
    pt.complete("never_started")
    assert pt.completed == []
    assert pt.active is None


def test_phase_tracker_complete_unknown_name_emits_debug_log(caplog):
    # Silent returns on unknown phase names mask typos and double-complete bugs;
    # the no-op must still leave a DEBUG breadcrumb naming the offending phase.
    caplog.set_level(logging.DEBUG, logger="lhp.cli.live_panel")
    pt = PhaseTracker()
    pt.complete("not-a-phase")

    matching = [
        rec
        for rec in caplog.records
        if rec.levelno == logging.DEBUG
        and "not-a-phase" in rec.getMessage()
        and "unknown" in rec.getMessage().lower()
    ]
    assert matching, (
        "expected a DEBUG log record naming the unknown phase 'not-a-phase'; "
        f"got records: {[(r.levelname, r.getMessage()) for r in caplog.records]}"
    )


def test_activity_tail_max_entries_caps_history():
    tail = ActivityTail(max_entries=3)
    for i in range(5):
        tail.append(PipelineRecord(name=f"p{i}", success=True, files=1, duration_s=0.5))
    snapshot = list(tail._entries)
    assert len(snapshot) == 3
    assert [r.name for r in snapshot] == ["p2", "p3", "p4"]


def test_activity_tail_empty_render_returns_empty_text():
    tail = ActivityTail(max_entries=5)
    rendered = tail.render()
    assert isinstance(rendered, Text)
    assert rendered.plain == ""


def test_activity_tail_success_record_renders_check_glyph():
    tail = ActivityTail(max_entries=5)
    tail.append(PipelineRecord(name="p", success=True, files=2, duration_s=1.3))
    rendered = tail.render()
    assert isinstance(rendered, Group)
    matched = False
    for child in rendered.renderables:
        plain = _plain_of(child)
        if "✓" in plain and "p" in plain and "2 files" in plain and "1.3s" in plain:
            matched = True
            break
    assert matched, "expected success record to render check glyph with files+duration"


def test_activity_tail_failure_record_renders_cross_glyph_with_error_code():
    tail = ActivityTail(max_entries=5)
    tail.append(PipelineRecord(name="p", success=False, error_code="LHP-001"))
    rendered = tail.render()
    assert isinstance(rendered, Group)
    matched = False
    for child in rendered.renderables:
        plain = _plain_of(child)
        if "✗" in plain and "p" in plain and "LHP-001" in plain:
            matched = True
            break
    assert matched, "expected failure record to render cross glyph with error code"


def test_activity_tail_failure_record_without_error_code_falls_back_to_em_dash():
    tail = ActivityTail(max_entries=5)
    tail.append(PipelineRecord(name="p", success=False, error_code=None))
    rendered = tail.render()
    assert isinstance(rendered, Group)
    matched = any("—" in _plain_of(child) for child in rendered.renderables)
    assert matched, "expected missing error_code to fall back to '—'"


def test_activity_tail_render_snapshots_before_iterating():
    """render() must snapshot self._entries before iterating: the Rich Live
    refresh thread iterates while the main thread appends on each pipeline
    completion, and CPython raises ``RuntimeError: deque mutated during
    iteration`` without an explicit list() copy.
    """
    tail = ActivityTail(max_entries=10)
    for i in range(8):
        tail.append(
            PipelineRecord(name=f"seed{i}", success=True, files=1, duration_s=0.5)
        )

    stop = threading.Event()
    caught: list = []

    def producer() -> None:
        i = 0
        while not stop.is_set():
            tail.append(
                PipelineRecord(name=f"p{i}", success=True, files=1, duration_s=0.5)
            )
            i += 1
            time.sleep(0)  # yield to maximise interleaving

    thread = threading.Thread(target=producer, daemon=True)
    thread.start()
    try:
        for _ in range(2000):
            try:
                tail.render()
            except RuntimeError as exc:  # pragma: no cover - failure path
                caught.append(exc)
                break
    finally:
        stop.set()
        thread.join(timeout=2.0)

    assert not caught, (
        "render() must snapshot self._entries via list() before iterating; "
        f"a concurrent append raised: {caught[0]!r}"
    )
    assert len(tail._entries) > 0


def test_overall_progress_initializes_with_total():
    op = OverallProgress("foo", total=10)
    task = op._progress.tasks[0]
    assert task.total == 10
    assert task.description == "foo"


def test_overall_progress_advance_increments_completed():
    op = OverallProgress("foo", total=10)
    op.advance(2)
    assert op._progress.tasks[0].completed == 2


def test_overall_progress_set_total_updates_total():
    op = OverallProgress("foo", total=10)
    op.set_total(50)
    assert op._progress.tasks[0].total == 50


def test_overall_progress_render_returns_progress_instance():
    op = OverallProgress("foo", total=10)
    assert isinstance(op.render(), Progress)


def test_overall_progress_auto_refresh_is_disabled():
    # Rich stores ``auto_refresh`` on the Progress' internal Live, not on the
    # Progress itself; assert on the Live attribute.
    op = OverallProgress("foo", total=10)
    assert (
        op._progress.live.auto_refresh is False
    ), "auto_refresh must be False — outer Live drives all refreshes"


def test_overall_progress_task_clock_not_started_at_construction():
    """``add_task(start=False)`` gates elapsed-time tracking on ``start()``;
    constructing OverallProgress must not start the task clock.
    """
    op = OverallProgress("foo", total=10)
    assert (
        op._progress.tasks[0].start_time is None
    ), "task.start_time must be None until OverallProgress.start() is called"


def test_overall_progress_stop_before_start_leaves_clock_unstarted():
    """``stop()`` without a prior ``start()`` must be a no-op. Error-path
    callers (LHPConfigError-014 discovery, validate empty_no_op finally) hit
    the teardown without start(), and Rich's ``Progress.stop_task`` fallback
    would otherwise set ``task.start_time = current_time``.
    """
    op = OverallProgress("foo", total=10)
    op.stop()
    assert (
        op._progress.tasks[0].start_time is None
    ), "stop() before start() must not start the task clock"


def test_overall_progress_start_does_not_activate_internal_live():
    """``Progress.start()`` activates the Progress's internal ``rich.live.Live``,
    which the outer Live then composes into a Group with the parent renderable
    and paints the bar twice. Pin the invariant.
    """
    from rich.console import Console as _RichConsole

    console = _RichConsole(force_terminal=True, width=80)
    op = OverallProgress("foo", total=10, console=console)
    op.start()
    try:
        assert op._progress.live.is_started is False, (
            "OverallProgress.start() must NOT start the Progress's internal Live; "
            "doing so causes a duplicate bar beneath the parent Panel"
        )
        assert console._live_stack == [], (
            "The Progress's internal Live must not be pushed onto "
            "console._live_stack — Rich's topmost Live composes the whole "
            "stack into a Group on every refresh"
        )
    finally:
        op.stop()


def test_overall_progress_start_starts_the_task_clock():
    """``start()`` must set ``task.start_time`` so TimeElapsedColumn ticks."""
    op = OverallProgress("foo", total=10)
    op.start()
    assert (
        op._progress.tasks[0].start_time is not None
    ), "OverallProgress.start() must start the task clock (start_task)"


def test_overall_progress_stop_freezes_the_task_clock():
    """``stop()`` must set ``task.stop_time`` so TimeElapsedColumn freezes."""
    op = OverallProgress("foo", total=10)
    op.start()
    op.stop()
    assert (
        op._progress.tasks[0].stop_time is not None
    ), "OverallProgress.stop() must stop the task clock (stop_task)"


def test_overall_progress_stop_does_not_leave_live_on_stack():
    """While an outer Live is active and OverallProgress.start() has been
    called, ``console._live_stack`` must contain ONLY the outer Live. The
    snapshot is taken while the outer Live is still active — post-teardown
    the inner Live would be popped regardless of how it was pushed.
    """
    from rich.console import Console as _RichConsole
    from rich.live import Live

    console = _RichConsole(force_terminal=True, width=80)
    op = OverallProgress("foo", total=10, console=console)

    with Live("outer", console=console, auto_refresh=False) as outer_live:
        op.start()
        stack_snapshot = list(outer_live.console._live_stack)
        op.stop()

    assert len(stack_snapshot) == 1, (
        "While OverallProgress is active inside an outer Live, "
        "console._live_stack must contain ONLY the outer Live; "
        f"got stack of length {len(stack_snapshot)}: {stack_snapshot!r}. "
        "A length of 2 indicates OverallProgress.start() pushed the "
        "Progress's internal Live, which would paint a duplicate bar."
    )
    assert console._live_stack == []


def _make_frame(
    *,
    seed_phase: str | None = None,
    seed_record: PipelineRecord = None,
    console_width: int = 80,
) -> Panel:
    # Default ``console_width=80`` keeps the body as a single-column Group so
    # structural tests can assert on ``panel.renderable.renderables`` directly.
    pt = PhaseTracker()
    if seed_phase is not None:
        pt.start(seed_phase)
    op = OverallProgress("Pipelines", total=42)
    tail = ActivityTail(max_entries=5)
    if seed_record is not None:
        tail.append(seed_record)
    hdr = HeaderContext(command_name="generate", env="dev", total_pipelines=42)
    return render_live_frame(
        pt,
        op,
        tail,
        [],
        header_context=hdr,
        elapsed_text="12.3s",
        show_progress=True,
        console_width=console_width,
    )


def test_render_live_frame_returns_panel():
    panel = _make_frame()
    assert isinstance(panel, Panel)


def test_render_live_frame_title_contains_header_fields():
    panel = _make_frame()
    # Title is a rich.text.Text so a styled red " — N failed" suffix can be
    # appended in-line; use .plain to assert on the rendered string.
    title = panel.title.plain
    assert "generate" in title
    assert "dev" in title
    assert "42" in title
    assert "12.3s" in title


def test_render_live_frame_uses_dim_border_style():
    panel = _make_frame()
    assert panel.border_style == "dim"


def test_render_live_frame_body_is_group():
    panel = _make_frame()
    assert isinstance(panel.renderable, Group)


def test_render_live_frame_body_includes_progress_renderable():
    panel = _make_frame()
    body = panel.renderable
    progresses = [c for c in body.renderables if isinstance(c, Progress)]
    assert len(progresses) >= 1, "frame body must include the OverallProgress instance"


def test_render_live_frame_with_seeded_active_phase_contains_spinner():
    panel = _make_frame(seed_phase="Discovering")
    body = panel.renderable
    assert isinstance(body, Group)
    spinner = _find_renderable(body, Spinner)
    assert spinner is not None, "active phase must surface as a Spinner in the frame"


def test_render_live_frame_recent_label_appears_when_tail_has_entries():
    panel = _make_frame(
        seed_record=PipelineRecord(name="p", success=True, files=1, duration_s=0.5)
    )
    body = panel.renderable
    has_recent = any("Recent:" in _plain_of(child) for child in body.renderables)
    assert has_recent, "'Recent:' label must appear when activity tail has entries"


def test_render_live_frame_recent_label_absent_when_tail_empty():
    panel = _make_frame()
    body = panel.renderable
    has_recent = any(_plain_of(child) == "Recent:" for child in body.renderables)
    assert not has_recent, (
        "'Recent:' label must be absent when activity tail is empty "
        "(else-branch yields Text(''))"
    )


def test_render_live_frame_title_omits_failed_suffix_when_zero():
    panel = _make_frame()
    assert "failed" not in panel.title.plain


def test_render_live_frame_title_appends_failed_suffix_when_positive():
    pt = PhaseTracker()
    op = OverallProgress("Pipelines", total=42)
    tail = ActivityTail(max_entries=5)
    hdr = HeaderContext(command_name="generate", env="dev", total_pipelines=42)
    panel = render_live_frame(
        pt,
        op,
        tail,
        [],
        header_context=hdr,
        elapsed_text="12.3s",
        show_progress=True,
        failed_count=3,
    )
    assert "3 failed" in panel.title.plain


def test_render_live_frame_wide_terminal_body_is_grid_with_wordmark():
    """Wide terminals render the body as a two-column ``Table.grid`` so the
    wordmark can occupy the right column without displacing existing content.
    """
    panel = _make_frame(console_width=120)
    assert isinstance(
        panel.renderable, Table
    ), f"wide-terminal body must be a Table.grid; got {type(panel.renderable).__name__}"


def test_render_live_frame_wordmark_appears_in_body_at_wide_width():
    """Asserts on rendered text rather than internal structure so the test is
    robust to future layout tweaks.
    """
    panel = _make_frame(console_width=120)
    rendered = _render_panel_to_text(panel, width=120)
    first_row = _LHP_WORDMARK.splitlines()[0]
    assert first_row in rendered, (
        f"expected wordmark first row {first_row!r} in rendered panel; "
        f"rendered output was:\n{rendered}"
    )


def test_render_live_frame_wordmark_omitted_on_narrow_terminal():
    """Below :data:`_WORDMARK_MIN_PANEL_WIDTH` the body must remain a
    single-column Group so the progress bar and activity tail have full width.
    """
    panel = _make_frame(console_width=80)
    rendered = _render_panel_to_text(panel, width=80)
    first_row = _LHP_WORDMARK.splitlines()[0]
    assert first_row not in rendered
    assert isinstance(panel.renderable, Group)


def test_render_live_frame_wordmark_version_uses_package_metadata():
    """Both ``lhp --version`` and the wordmark version label must call
    ``importlib.metadata.version('lakehouse-plumber')`` so the two surfaces
    can never disagree within one CLI session.
    """
    from importlib.metadata import version as _pkg_version

    expected = _pkg_version("lakehouse-plumber")
    panel = _make_frame(console_width=120)
    rendered = _render_panel_to_text(panel, width=120)
    assert f"v{expected}" in rendered, (
        f"expected version label 'v{expected}' in rendered panel; "
        f"rendered output was:\n{rendered}"
    )


class _RecordingLive:
    """Test double for ``rich.live.Live`` — counts update() calls."""

    def __init__(self) -> None:
        self.updates: list = []

    def update(self, renderable) -> None:
        self.updates.append(renderable)


def test_live_update_coalescer_throttles_within_interval():
    live = _RecordingLive()
    render_calls = {"n": 0}

    def render_fn():
        render_calls["n"] += 1
        return f"frame-{render_calls['n']}"

    coalescer = _LiveUpdateCoalescer(live, render_fn, min_interval_s=10.0)
    coalescer.update()
    coalescer.update()
    coalescer.update()
    assert len(live.updates) == 1


def test_live_update_coalescer_force_always_passes():
    live = _RecordingLive()
    coalescer = _LiveUpdateCoalescer(live, lambda: "frame", min_interval_s=10.0)
    coalescer.update()
    coalescer.update(force=True)
    coalescer.update(force=True)
    assert len(live.updates) == 3


def test_live_update_coalescer_force_flushes_after_throttled_update():
    """``generate_command`` calls ``coalesced.update(force=True)`` once the
    submit loop drains so the final absorbed frame paints; without the
    forced flush the last sub-throttle state change would never repaint.
    """
    live = _RecordingLive()
    frames: list = []

    def render_fn():
        frame = object()
        frames.append(frame)
        return frame

    coalescer = _LiveUpdateCoalescer(live, render_fn, min_interval_s=10.0)
    coalescer.update()
    coalescer.update(force=True)

    assert len(live.updates) == 2, (
        "force=True must always flush — saw "
        f"{len(live.updates)} live.update call(s); expected 2 "
        "(one debounced, one forced)"
    )
    assert live.updates[0] is frames[0]
    assert live.updates[1] is frames[1]


def test_get_lhp_version_is_cached_per_process():
    """``_get_lhp_version`` is ``functools.cache``-decorated so the
    ``importlib.metadata.version`` call happens at most once per process —
    ``render_live_frame`` invokes it on every Rich Live refresh (~10 Hz).
    The body uses a lazy import, so patch the source module rather than a
    name re-exported on ``lhp.cli.live_panel``.
    """
    _get_lhp_version.cache_clear()
    with patch("importlib.metadata.version", return_value="1.2.3") as mock_version:
        first = _get_lhp_version()
        second = _get_lhp_version()
        third = _get_lhp_version()

    assert first == "1.2.3"
    assert second == "1.2.3"
    assert third == "1.2.3"
    assert mock_version.call_count == 1, (
        f"expected importlib.metadata.version to be called exactly once across "
        f"three _get_lhp_version() invocations; got {mock_version.call_count}"
    )
    _get_lhp_version.cache_clear()

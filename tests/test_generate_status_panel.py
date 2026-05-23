"""Structural tests for the generate command Rich status panel.

Snapshots are intentionally not used here because timing values
(``duration_s``) are non-deterministic. Tests assert structural
properties only: pipeline names appear, success/failure markers are
rendered with the right symbol, and the dry-run verb differs from the
non-dry-run verb.
"""

import logging
import time
from io import StringIO

from conftest import capture_lhp_console
from rich.console import Console, Group
from rich.progress import Progress
from rich.spinner import Spinner
from rich.text import Text

from lhp.cli.generate_summary import print_summary_table
from lhp.cli.live_panel import (
    ActivityTail,
    HeaderContext,
    OverallProgress,
    PhaseTracker,
    PipelineRecord,
    render_live_frame,
    rich_handler_attached,
)


def _has_spinner(node) -> bool:
    if isinstance(node, Spinner):
        return True
    if isinstance(node, Group):
        return any(_has_spinner(child) for child in node.renderables)
    return False


def _capture(
    records,
    dry_run: bool,
    *,
    failed: bool = False,
    show_all: bool = False,
    elapsed_s: float = 1.5,
) -> str:
    """Render the summary table at a fixed width without ANSI color and return the captured text."""
    with capture_lhp_console(80) as buf:
        print_summary_table(
            records,
            dry_run=dry_run,
            failed=failed,
            show_all=show_all,
            elapsed_s=elapsed_s,
        )
    return buf.getvalue()


def test_summary_table_includes_passing_pipelines() -> None:
    rec_pass = PipelineRecord("bronze_pipeline")
    rec_pass.success = True
    rec_pass.files = 5
    rec_pass.duration_s = 1.2
    rec_fail = PipelineRecord("silver_pipeline")
    rec_fail.success = False
    rec_fail.duration_s = 0.4
    rec_fail.error_code = "LHP-VAL-021"

    # The failures-only default would hide bronze_pipeline; opt into the
    # full table via ``show_all=True`` to assert on both ``✓`` and ``✗`` rows.
    out = _capture(
        {"bronze_pipeline": rec_pass, "silver_pipeline": rec_fail},
        dry_run=False,
        show_all=True,
    )

    assert "bronze_pipeline" in out
    assert "silver_pipeline" in out
    assert "✓" in out
    assert "✗" in out
    assert "Generated" in out
    assert "5" in out


def test_summary_table_dry_run_says_would_generate() -> None:
    rec = PipelineRecord("p")
    rec.success = True
    rec.files = 3
    rec.duration_s = 0.5

    out = _capture({"p": rec}, dry_run=True)

    assert "Would generate" in out
    # Non-dry-run verb must not leak in (after stripping the dry-run verb
    # that contains "generate").
    assert "Generated " not in out.replace("Would generate", "")


def test_summary_table_all_passing_total_matches_sum() -> None:
    rec_a = PipelineRecord("a")
    rec_a.success = True
    rec_a.files = 4
    rec_a.duration_s = 0.1
    rec_b = PipelineRecord("b")
    rec_b.success = True
    rec_b.files = 7
    rec_b.duration_s = 0.2

    out = _capture({"a": rec_a, "b": rec_b}, dry_run=False)

    assert "11" in out


def test_summary_table_footer_uses_real_wall_clock_elapsed() -> None:
    """Footer's elapsed seconds reflects ``elapsed_s`` kwarg, not ``sum(r.duration_s)``.

    The pre-fix bug computed the footer total as the sum of per-pipeline
    durations, which double-counts under parallelism. Sentinel 99.9s cannot
    match any sum of the per-row durations.
    """
    rec_a = PipelineRecord("a")
    rec_a.success = True
    rec_a.files = 2
    rec_a.duration_s = 0.1
    rec_b = PipelineRecord("b")
    rec_b.success = True
    rec_b.files = 3
    rec_b.duration_s = 0.2

    out = _capture({"a": rec_a, "b": rec_b}, dry_run=False, elapsed_s=99.9)

    assert (
        "99.9s" in out
    ), f"expected footer to render elapsed_s=99.9 verbatim; got:\n{out}"
    # Buggy sum would have been 0.3s — assert it isn't there so a future
    # regression that re-introduces ``sum(r.duration_s)`` is caught.
    assert "0.3s" not in out


def test_summary_table_failed_pipeline_omits_file_count() -> None:
    rec = PipelineRecord("doomed")
    rec.success = False
    rec.duration_s = 0.1
    rec.error_code = "LHP-IO-007"

    out = _capture({"doomed": rec}, dry_run=False)

    assert "doomed" in out
    assert "✗" in out
    assert "—" in out


def test_pipeline_record_defaults() -> None:
    """``PipelineRecord`` starts with no terminal state; ``success is None`` means in-flight."""
    rec = PipelineRecord("just_started")

    assert rec.name == "just_started"
    assert rec.success is None
    assert rec.files == 0
    assert rec.duration_s == 0.0
    assert rec.error_code is None


def test_summary_table_prints_on_full_failure() -> None:
    """Summary header + footer still print when every pipeline fails; footer says 'Failed to generate' (not 'Generated 0 files')."""
    rec_a = PipelineRecord("doomed_a")
    rec_a.success = False
    rec_a.duration_s = 0.4
    rec_a.error_code = "LHP-VAL-007"
    rec_b = PipelineRecord("doomed_b")
    rec_b.success = False
    rec_b.duration_s = 0.1
    rec_b.error_code = "LHP-IO-002"

    out = _capture(
        {"doomed_a": rec_a, "doomed_b": rec_b},
        dry_run=False,
        failed=True,
    )

    assert "Generation Summary" in out
    assert "doomed_a" in out
    assert "doomed_b" in out
    assert "✗" in out
    assert "Failed to generate" in out
    assert "0 of 2 pipelines succeeded" in out
    assert "Generated " not in out
    assert "Would generate" not in out


def test_summary_table_prints_on_partial_failure() -> None:
    """Partial failure footer keeps 'Generated N files' verb plus a failure-count parenthetical."""
    records = {}
    for i, name in enumerate(("ok_1", "ok_2", "ok_3")):
        rec = PipelineRecord(name)
        rec.success = True
        rec.files = i + 1  # 1, 2, 3 -> total 6
        rec.duration_s = 0.1
        records[name] = rec
    for name in ("bad_1", "bad_2"):
        rec = PipelineRecord(name)
        rec.success = False
        rec.duration_s = 0.1
        rec.error_code = "LHP-VAL-021"
        records[name] = rec

    out = _capture(records, dry_run=False, failed=True)

    assert "Generation Summary" in out
    assert "Generated " in out
    assert "2 of 5 pipelines failed" in out
    assert "6" in out


def test_summary_table_empty_records_prints_nothing() -> None:
    """records={} -> no output (early return).

    The summary call sits on the post-Live path that fires unconditionally,
    so it must handle the empty case gracefully (no empty Generation
    Summary table for a run that fails before any pipeline records are seeded).
    """
    out = _capture({}, dry_run=False, failed=True)

    assert "Generation Summary" not in out
    assert out == ""


def test_summary_table_partial_failure_dry_run_uses_would_generate() -> None:
    rec_ok = PipelineRecord("ok")
    rec_ok.success = True
    rec_ok.files = 4
    rec_ok.duration_s = 0.1
    rec_bad = PipelineRecord("bad")
    rec_bad.success = False
    rec_bad.duration_s = 0.1
    rec_bad.error_code = "LHP-VAL-021"

    out = _capture({"ok": rec_ok, "bad": rec_bad}, dry_run=True, failed=True)

    assert "Would generate" in out
    assert "1 of 2 pipelines failed" in out


def test_summary_table_total_failure_dry_run_still_says_failed_to_generate() -> None:
    """Total-failure footer is NOT softened by dry-run; failure is real even in a dry-run preview."""
    rec = PipelineRecord("doomed")
    rec.success = False
    rec.duration_s = 0.1
    rec.error_code = "LHP-VAL-007"

    out = _capture({"doomed": rec}, dry_run=True, failed=True)

    assert "Failed to generate" in out
    assert "Would generate" not in out


def test_phase_marker_records_sub_threshold_phases_by_default() -> None:
    """Default semantics: every ``complete()`` is recorded regardless of duration."""
    tracker = PhaseTracker()

    tracker.start("Generation")
    tracker.complete("Generation")
    assert len(tracker.completed) == 1
    name, _duration, success = tracker.completed[0]
    assert name == "Generation"
    assert success is True

    tracker.start("Generation")
    tracker.complete("Generation", success=False)
    assert len(tracker.completed) == 2
    assert tracker.completed[1][0] == "Generation"
    assert tracker.completed[1][2] is False


def test_phase_marker_suppress_if_fast_drops_sub_threshold_phases() -> None:
    """``suppress_if_fast=True`` drops sub-250ms phases; phases above the threshold are still recorded."""
    tracker = PhaseTracker()

    tracker.start("Transient")
    tracker.complete("Transient", suppress_if_fast=True)
    assert tracker.completed == []
    assert tracker.active is None


def test_phase_marker_failed_uses_red_x_marker() -> None:
    """``complete(name, success=False)`` renders a ``✗`` glyph and records ``success=False``."""
    tracker = PhaseTracker()
    tracker.start("Generation")
    tracker.complete("Generation", success=False)

    assert len(tracker.completed) == 1
    assert tracker.completed[0][0] == "Generation"
    assert tracker.completed[0][2] is False

    rendered = tracker.render()
    assert isinstance(rendered, Group)
    found_cross = False
    for child in rendered.renderables:
        if (
            isinstance(child, Text)
            and "✗" in child.plain
            and "Generation" in child.plain
        ):
            found_cross = True
            break
    assert (
        found_cross
    ), "expected ✗ glyph alongside 'Generation' in rendered failure entry"


def test_phase_marker_success_path_unchanged() -> None:
    """Above the 250ms threshold, a successful phase records ``(name, duration, True)``."""
    tracker = PhaseTracker()
    tracker.start("Discovering")
    time.sleep(0.3)
    tracker.complete("Discovering")

    assert len(tracker.completed) == 1
    name, duration, success = tracker.completed[0]
    assert name == "Discovering"
    assert success is True
    assert duration >= 0.25
    assert tracker.active is None


def test_render_live_frame_active_phase_contains_spinner() -> None:
    """Active phase surfaces as a ``Spinner`` in the rendered frame body alongside the ``OverallProgress``."""
    phase_tracker = PhaseTracker()
    phase_tracker.start("Generation")
    overall = OverallProgress("Generating", total=2)
    activity = ActivityTail()

    # ``console_width=80`` keeps the body as the single-column ``Group``
    # this test was written against. The wide-terminal two-column grid is
    # covered by separate tests in ``tests/cli/test_live_panel_primitives``.
    panel = render_live_frame(
        phase_tracker,
        overall,
        activity,
        [],
        header_context=HeaderContext(
            command_name="generate", env="dev", total_pipelines=2
        ),
        elapsed_text="00:03",
        show_progress=True,
        console_width=80,
    )

    body = panel.renderable
    assert isinstance(body, Group)
    assert _has_spinner(body) is True
    progresses = [c for c in body.renderables if isinstance(c, Progress)]
    assert (
        len(progresses) == 1
    ), "frame body must include the OverallProgress (Progress) instance"


def test_rich_handler_attached_restores_handlers_on_exit() -> None:
    """On exit, the root logger's handler list is identical to what it was before entry."""
    root_logger = logging.getLogger()
    err_console = Console(file=StringIO(), force_terminal=False, no_color=True)

    handlers_before = list(root_logger.handlers)

    with rich_handler_attached(err_console):
        assert root_logger.handlers != handlers_before or not handlers_before

    handlers_after = list(root_logger.handlers)
    assert handlers_after == handlers_before

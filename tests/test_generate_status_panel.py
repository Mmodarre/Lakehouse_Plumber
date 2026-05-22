"""Structural tests for the generate command Rich status panel.

Snapshots are intentionally not used here because timing values
(``duration_s``) are non-deterministic. Tests assert structural
properties only: pipeline names appear, success/failure markers are
rendered with the right symbol, and the dry-run verb differs from the
non-dry-run verb.
"""

import logging
from io import StringIO

from conftest import capture_lhp_console
from rich.console import Console
from rich.spinner import Spinner
from rich.text import Text

from lhp.cli.generate_summary import print_summary_table
from lhp.cli.live_panel import (
    PipelineRecord,
    append_phase_marker_line,
    render_status_group,
    rich_handler_attached,
)


def _capture(
    records,
    dry_run: bool,
    *,
    failed: bool = False,
    show_all: bool = False,
) -> str:
    """Render the summary table to a StringIO and return the captured text.

    Uses the shared ``capture_lhp_console`` helper to swap the lazy
    module-attribute ``lhp.cli.console.console`` for a deterministic
    in-memory Console. The autouse ``_isolate_lhp_console`` fixture
    (in tests/conftest.py) already isolates the global; this helper
    provides finer-grained capture without ANSI color.
    """
    with capture_lhp_console(80) as buf:
        print_summary_table(records, dry_run=dry_run, failed=failed, show_all=show_all)
    return buf.getvalue()


def _render_text(t: Text) -> str:
    """Render a Rich ``Text`` to plain string for substring assertions."""
    buf = StringIO()
    Console(file=buf, force_terminal=False, no_color=True, width=200).print(t)
    return buf.getvalue()


def test_summary_table_includes_passing_pipelines() -> None:
    """Passing pipelines render with ``✓`` and their file count."""
    rec_pass = PipelineRecord("bronze_pipeline")
    rec_pass.success = True
    rec_pass.files = 5
    rec_pass.duration_s = 1.2
    rec_fail = PipelineRecord("silver_pipeline")
    rec_fail.success = False
    rec_fail.duration_s = 0.4
    rec_fail.error_code = "LHP-VAL-021"

    # The failures-only default would hide bronze_pipeline; this test
    # asserts that the unfiltered table still renders both ``✓`` and
    # ``✗`` rows, so opt into the full table via ``show_all=True``.
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
    # Files count for the passing pipeline appears in the table column.
    assert "5" in out


def test_summary_table_dry_run_says_would_generate() -> None:
    """Dry-run summary uses 'Would generate', not 'Generated'."""
    rec = PipelineRecord("p")
    rec.success = True
    rec.files = 3
    rec.duration_s = 0.5

    out = _capture({"p": rec}, dry_run=True)

    assert "Would generate" in out
    # The non-dry-run verb must not leak in (excluding it after stripping
    # the dry-run verb that contains "generate").
    assert "Generated " not in out.replace("Would generate", "")


def test_summary_table_all_passing_total_matches_sum() -> None:
    """The 'Generated N files' total equals the sum of per-record files."""
    rec_a = PipelineRecord("a")
    rec_a.success = True
    rec_a.files = 4
    rec_a.duration_s = 0.1
    rec_b = PipelineRecord("b")
    rec_b.success = True
    rec_b.files = 7
    rec_b.duration_s = 0.2

    out = _capture({"a": rec_a, "b": rec_b}, dry_run=False)

    # Total = 4 + 7 = 11 (rendered as "11" in the summary line).
    assert "11" in out


def test_summary_table_failed_pipeline_omits_file_count() -> None:
    """Failed pipelines render '—' for the files column, not a digit."""
    rec = PipelineRecord("doomed")
    rec.success = False
    rec.duration_s = 0.1
    rec.error_code = "LHP-IO-007"

    out = _capture({"doomed": rec}, dry_run=False)

    assert "doomed" in out
    assert "✗" in out
    # Em-dash replaces the file count for the failed row.
    assert "—" in out


def test_pipeline_record_defaults() -> None:
    """``PipelineRecord`` starts with no terminal state.

    The Live status panel relies on ``success is None`` to mean
    'in flight' for the 'N of M done' counter.
    """
    rec = PipelineRecord("just_started")

    assert rec.name == "just_started"
    assert rec.success is None
    assert rec.files == 0
    assert rec.duration_s == 0.0
    assert rec.error_code is None


def test_summary_table_prints_on_full_failure() -> None:
    """When all pipelines fail, the summary header + footer still print.

    Phase 4 contract: the summary table is rendered AFTER the Live frame
    closes, so it must still appear when the orchestrator reports a
    total failure. The footer changes to a 'Failed to generate' line so
    the user does not see a misleading 'Generated 0 files' on a full
    failure run.
    """
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
    # The success-path footer must not appear on a total failure.
    assert "Generated " not in out
    assert "Would generate" not in out


def test_summary_table_prints_on_partial_failure() -> None:
    """3 success + 2 failure -> footer reports both counts.

    Partial failure keeps the standard 'Generated N files' verb but
    appends a parenthetical that surfaces the failure count so the
    user can scan the footer alone to see the run was not fully clean.
    """
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
    # Total success files = 6 (1+2+3).
    assert "6" in out


def test_summary_table_empty_records_prints_nothing() -> None:
    """records={} -> no output (early return).

    A run that fails before any pipeline records are seeded must not
    print an empty Generation Summary table. The summary call sits on
    the post-Live path that fires unconditionally, so it must handle
    the empty case gracefully.
    """
    out = _capture({}, dry_run=False, failed=True)

    assert "Generation Summary" not in out
    assert out == ""


def test_summary_table_partial_failure_dry_run_uses_would_generate() -> None:
    """Partial-failure footer respects the dry-run verb."""
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
    """Total-failure footer is NOT softened by dry-run.

    There is no useful 'would have failed' framing — the failure is
    real even in a dry-run preview, so the footer stays unchanged.
    """
    rec = PipelineRecord("doomed")
    rec.success = False
    rec.duration_s = 0.1
    rec.error_code = "LHP-VAL-007"

    out = _capture({"doomed": rec}, dry_run=True, failed=True)

    assert "Failed to generate" in out
    assert "Would generate" not in out


def test_phase_marker_force_overrides_threshold() -> None:
    """force=True emits a phase line even below the 250ms threshold.

    Without ``force=True``, a 0.05s phase is suppressed by the threshold
    to avoid Live-panel flicker. A failing Generation phase, however,
    needs a diagnostic line regardless of how fast it failed.
    """
    phase_lines: list = []

    # Sanity: without force, a sub-threshold duration is suppressed.
    append_phase_marker_line(phase_lines, "Generation", 0.05)
    assert phase_lines == []

    append_phase_marker_line(
        phase_lines,
        "Generation",
        0.05,
        force=True,
        success=False,
    )
    assert len(phase_lines) == 1
    rendered = _render_text(phase_lines[0])
    assert "Generation" in rendered
    assert "✗" in rendered


def test_phase_marker_failed_uses_red_x_marker() -> None:
    """The success=False kwarg flows through into the rendered text.

    The failing-phase contract is that the marker glyph is ``✗`` and the
    style hint is ``bold red``. The style hint is a Rich tuple on the
    Text span, not a literal substring of the rendered output (we strip
    ANSI for assertion stability), so we inspect the Text spans.
    """
    phase_lines: list = []
    append_phase_marker_line(
        phase_lines,
        "Generation",
        0.05,
        force=True,
        success=False,
    )

    assert len(phase_lines) == 1
    text = phase_lines[0]
    # Rich Text stores per-span styling; inspect the spans for the
    # marker glyph + style.
    plain = text.plain
    assert "Generation" in plain
    assert "✗" in plain
    # Find the span carrying the failure marker and assert its style.
    marker_styles = [
        span.style for span in text.spans if "✗" in plain[span.start : span.end]
    ]
    assert marker_styles, "expected at least one span covering the ✗ marker"
    assert any("bold red" in str(s) for s in marker_styles)


def test_phase_marker_success_path_unchanged() -> None:
    """The default (no kwargs) behavior matches the pre-Phase-4 semantics.

    Backward-compat: success phase markers still use the ✓ glyph and
    the 250ms threshold still suppresses fast successes.
    """
    phase_lines: list = []

    # Sub-threshold success: suppressed.
    append_phase_marker_line(phase_lines, "Discovering", 0.05)
    assert phase_lines == []

    # Above threshold: emitted with the default ✓.
    append_phase_marker_line(phase_lines, "Discovering", 0.5)
    assert len(phase_lines) == 1
    rendered = _render_text(phase_lines[0])
    assert "Discovering" in rendered
    assert "✓" in rendered


def test_render_status_group_with_records_renders_spinner() -> None:
    """Non-empty records with completed < total -> spinner is present.

    Two records, neither completed; the function must emit exactly one
    spinner row so the user sees the "0 of 2 pipelines done" status.
    """
    records = {
        "p1": PipelineRecord("p1"),
        "p2": PipelineRecord("p2"),
    }
    group = render_status_group(records, [], [], elapsed_text="00:03")

    spinners = [r for r in group.renderables if isinstance(r, Spinner)]
    assert len(spinners) == 1


def test_rich_handler_attached_restores_handlers_on_exit() -> None:
    """The context manager restores the root logger's handlers on exit.

    The contract: at the moment ``with rich_handler_attached(...)``
    exits, the root logger's handler list is identical to what it was
    before entry. A regression here would tear the next CLI command's
    logging configuration.
    """
    root_logger = logging.getLogger()
    err_console = Console(file=StringIO(), force_terminal=False, no_color=True)

    handlers_before = list(root_logger.handlers)

    with rich_handler_attached(err_console):
        # Inside the context: the RichHandler is attached and any pre-
        # existing StreamHandler (non-FileHandler) was removed. The
        # invariant we care about is restoration on exit, so we don't
        # over-assert the in-context shape.
        assert root_logger.handlers != handlers_before or not handlers_before

    handlers_after = list(root_logger.handlers)
    assert handlers_after == handlers_before

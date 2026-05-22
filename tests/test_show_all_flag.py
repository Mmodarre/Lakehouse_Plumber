"""Tests for the ``--show-all`` flag and failures-only summary default.

Phase E of the CLI UX hardening plan moves the post-run summary table
from "always print every pipeline" to "print failed pipelines only,
plus a single-line footer". The full-table behavior is opted into by
``--show-all`` / ``-a``.

These tests exercise the table functions directly with constructed
``PipelineRecord`` dicts; no full CLI integration is needed because the
filtering and footer-only paths live in ``generate_summary.py`` and
``validate_summary.py``.
"""

from typing import Dict

from conftest import capture_lhp_console

from lhp.cli.generate_summary import print_summary_table
from lhp.cli.live_panel import PipelineRecord
from lhp.cli.validate_summary import print_validate_summary_table


def _capture_generate(
    records: Dict[str, PipelineRecord],
    *,
    dry_run: bool = False,
    failed: bool = False,
    show_all: bool = False,
    warning_count: int = 0,
) -> str:
    """Render the generate summary table to an in-memory console.

    Uses the shared ``capture_lhp_console`` helper from conftest. The
    autouse ``_isolate_lhp_console`` fixture already isolates the global
    console singleton; this helper provides finer-grained capture at a
    fixed width so assertions can match plain text without ANSI.
    """
    with capture_lhp_console(200) as buf:
        print_summary_table(
            records,
            dry_run=dry_run,
            failed=failed,
            show_all=show_all,
            warning_count=warning_count,
        )
    return buf.getvalue()


def _capture_validate(
    records: Dict[str, PipelineRecord],
    *,
    failed: bool = False,
    show_all: bool = False,
    warning_count: int = 0,
) -> str:
    """Render the validate summary table to an in-memory console."""
    with capture_lhp_console(200) as buf:
        print_validate_summary_table(
            records,
            failed=failed,
            show_all=show_all,
            warning_count=warning_count,
        )
    return buf.getvalue()


def _success_records(n: int) -> Dict[str, PipelineRecord]:
    """Build n successful PipelineRecords for the generate path."""
    return {
        f"p{i:03d}": PipelineRecord(
            name=f"p{i:03d}",
            success=True,
            files=40 + i,
            duration_s=1.5,
        )
        for i in range(n)
    }


def _mixed_records(*, success: int, failed: int) -> Dict[str, PipelineRecord]:
    """Build mixed success/failure PipelineRecords for the generate path."""
    out: Dict[str, PipelineRecord] = {}
    for i in range(success):
        out[f"ok{i:03d}"] = PipelineRecord(
            name=f"ok{i:03d}",
            success=True,
            files=10,
            duration_s=0.8,
        )
    for i in range(failed):
        out[f"bad{i:03d}"] = PipelineRecord(
            name=f"bad{i:03d}",
            success=False,
            files=0,
            duration_s=0.3,
            error_code="LHP-VAL-007",
        )
    return out


def _validate_success_records(n: int) -> Dict[str, PipelineRecord]:
    """Build n successful PipelineRecords for the validate path."""
    return {
        f"p{i:03d}": PipelineRecord(
            name=f"p{i:03d}",
            success=True,
            errors_count=0,
            warnings_count=0,
        )
        for i in range(n)
    }


def _validate_mixed_records(*, success: int, failed: int) -> Dict[str, PipelineRecord]:
    """Build mixed success/failure PipelineRecords for the validate path."""
    out: Dict[str, PipelineRecord] = {}
    for i in range(success):
        out[f"ok{i:03d}"] = PipelineRecord(
            name=f"ok{i:03d}",
            success=True,
            errors_count=0,
            warnings_count=0,
        )
    for i in range(failed):
        out[f"bad{i:03d}"] = PipelineRecord(
            name=f"bad{i:03d}",
            success=False,
            errors_count=1,
            warnings_count=0,
        )
    return out


# --- Generate: default failures-only behavior -----------------------------


def test_generate_default_full_success_no_table():
    """Default ``show_all=False`` with 100 successes prints footer only.

    The Generation Summary title must NOT appear (no table), but the
    success footer with the aggregate file count must appear with the
    new ``all N pipelines passed`` phrasing.
    """
    out = _capture_generate(_success_records(100), failed=False)

    assert "Generation Summary" not in out
    assert "p000" not in out
    assert "p099" not in out
    assert "Generated" in out
    assert "all 100 pipelines passed" in out


def test_generate_default_partial_failure_shows_failed_rows_only():
    """Default ``show_all=False`` with 3 failures of 100 shows 3 rows.

    Successful pipelines must not appear in the table; the table title
    and the partial-failure footer wording must both be present.
    """
    records = _mixed_records(success=97, failed=3)
    out = _capture_generate(records, failed=True)

    assert "Generation Summary" in out
    # Failed rows are present.
    assert "bad000" in out
    assert "bad001" in out
    assert "bad002" in out
    # Successful rows are NOT present in the table.
    assert "ok000" not in out
    assert "ok050" not in out
    assert "ok096" not in out
    # Partial-failure footer.
    assert "3 of 100 pipelines failed" in out


def test_generate_show_all_full_success_shows_all_rows():
    """``show_all=True`` with 100 successes prints the full 100-row table."""
    out = _capture_generate(_success_records(100), failed=False, show_all=True)

    assert "Generation Summary" in out
    # First, middle, and last rows all present.
    assert "p000" in out
    assert "p050" in out
    assert "p099" in out
    assert "Generated" in out


# --- Validate: default failures-only behavior -----------------------------


def test_validate_default_full_success_no_table():
    """Default ``show_all=False`` with 100 passes prints footer only."""
    out = _capture_validate(_validate_success_records(100), failed=False)

    assert "Validation Summary" not in out
    assert "p000" not in out
    assert "p099" not in out
    assert "Validated 100 pipelines" in out
    assert "all passed" in out


def test_validate_partial_failure_failures_only():
    """Default ``show_all=False`` with mixed results shows failed rows only."""
    records = _validate_mixed_records(success=97, failed=3)
    out = _capture_validate(records, failed=True)

    assert "Validation Summary" in out
    # Failed rows are present.
    assert "bad000" in out
    assert "bad001" in out
    assert "bad002" in out
    # Successful rows are NOT present in the table.
    assert "ok000" not in out
    assert "ok096" not in out
    # Partial-failure footer mentions counts.
    assert "97" in out
    assert "passed" in out
    assert "3" in out
    assert "failed" in out


# --- Warning suffix on the footer ----------------------------------------


def test_generate_summary_footer_warning_count_full_success():
    """``warning_count=1`` on generate full-success footer-only path.

    Pluralization is pinned at ``1 warning`` (singular); a regression to
    ``1 warnings`` is caught by the negative assertion.
    """
    out = _capture_generate(_success_records(100), warning_count=1)
    assert "all 100 pipelines passed" in out
    assert "1 warning" in out
    assert "1 warnings" not in out  # singular form required


def test_generate_summary_footer_warning_count_partial_failure():
    """``warning_count=2`` on generate partial-failure table + footer."""
    out = _capture_generate(
        _mixed_records(success=97, failed=3),
        failed=True,
        warning_count=2,
    )
    assert "3 of 100 pipelines failed" in out
    assert "2 warnings" in out


def test_validate_summary_footer_warning_count_full_success():
    """``warning_count=1`` on validate full-success footer-only path.

    Pluralization is pinned at ``1 warning`` (singular).
    """
    out = _capture_validate(_validate_success_records(100), warning_count=1)
    assert "Validated 100 pipelines" in out
    assert "all passed" in out
    assert "1 warning" in out
    assert "1 warnings" not in out


def test_validate_summary_footer_warning_count_partial_failure():
    """``warning_count=3`` on validate partial-failure table + footer."""
    out = _capture_validate(
        _validate_mixed_records(success=97, failed=3),
        failed=True,
        warning_count=3,
    )
    assert "97" in out and "3" in out  # success/failure counts
    assert "3 warnings" in out


def test_generate_footer_includes_show_all_hint_when_total_gt_one():
    """Multi-pipeline success footer appends the ``-a`` discoverability hint.

    Footer-only path with ``show_all=False`` and more than one pipeline
    should surface the dim ``(use -a to list)`` hint so the user
    discovers the flag that opts into the per-pipeline table.
    """
    out = _capture_generate(_success_records(5))
    assert "all 5 pipelines passed" in out
    assert "(use -a to list)" in out


def test_generate_footer_omits_show_all_hint_when_single_pipeline():
    """Single-pipeline footer omits the hint -- there is nothing to list."""
    out = _capture_generate(_success_records(1))
    assert "all 1 pipeline passed" in out
    assert "(use -a to list)" not in out


def test_generate_footer_omits_show_all_hint_when_show_all_true():
    """``show_all=True`` already renders the table, so the hint is omitted.

    When ``show_all`` is True the table is rendered and the footer-only
    path is not taken, so the hint must never appear in this output.
    """
    out = _capture_generate(_success_records(5), show_all=True)
    assert "(use -a to list)" not in out


def test_validate_footer_includes_show_all_hint_when_total_gt_one():
    """Multi-pipeline validate footer appends the ``-a`` discoverability hint."""
    out = _capture_validate(_validate_success_records(5))
    assert "Validated 5 pipelines" in out
    assert "(use -a to list)" in out


def test_validate_footer_omits_show_all_hint_when_single_pipeline():
    """Single-pipeline validate footer omits the hint."""
    out = _capture_validate(_validate_success_records(1))
    assert "Validated 1 pipeline" in out
    assert "(use -a to list)" not in out


def test_validate_footer_omits_show_all_hint_when_show_all_true():
    """``show_all=True`` renders the validate table; the hint is omitted."""
    out = _capture_validate(_validate_success_records(5), show_all=True)
    assert "(use -a to list)" not in out

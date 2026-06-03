"""Tests for the ``--show-all`` flag and failures-only summary default.

Default behavior: print failed pipelines only plus a single-line footer.
Full-table opted into by ``--show-all`` / ``-a``.

Tests exercise the table functions directly with constructed
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
    elapsed_s: float = 1.5,
) -> str:
    with capture_lhp_console(200) as buf:
        print_summary_table(
            records,
            dry_run=dry_run,
            failed=failed,
            show_all=show_all,
            warning_count=warning_count,
            elapsed_s=elapsed_s,
        )
    return buf.getvalue()


def _capture_validate(
    records: Dict[str, PipelineRecord],
    *,
    failed: bool = False,
    show_all: bool = False,
    warning_count: int = 0,
) -> str:
    with capture_lhp_console(200) as buf:
        print_validate_summary_table(
            records,
            failed=failed,
            show_all=show_all,
            warning_count=warning_count,
        )
    return buf.getvalue()


def _success_records(n: int) -> Dict[str, PipelineRecord]:
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


def test_generate_default_full_success_no_table():
    """Default ``show_all=False`` with 100 successes prints footer only (no Generation Summary table)."""
    out = _capture_generate(_success_records(100), failed=False)

    assert "Generation Summary" not in out
    assert "p000" not in out
    assert "p099" not in out
    assert "Generated" in out
    assert "all 100 pipelines passed" in out


def test_generate_orchestrator_failure_after_full_success():
    """``failed=True`` with every record succeeding renders a non-success footer.

    Post-pipeline finalization raised (e.g. monitoring artifact write failed
    after every pipeline wrote its files). Footer must say pipelines completed
    but the run failed in post-processing, not ``Generated N files``.
    """
    out = _capture_generate(_success_records(5), failed=True)

    assert "Generated" not in out
    assert "Would generate" not in out
    assert "completed but" in out
    assert "post-processing" in out
    assert "All 5 pipelines" in out


def test_generate_default_partial_failure_shows_failed_rows_only():
    records = _mixed_records(success=97, failed=3)
    out = _capture_generate(records, failed=True)

    assert "Generation Summary" in out
    assert "bad000" in out
    assert "bad001" in out
    assert "bad002" in out
    assert "ok000" not in out
    assert "ok050" not in out
    assert "ok096" not in out
    assert "3 of 100 pipelines failed" in out


def test_generate_show_all_full_success_shows_all_rows():
    out = _capture_generate(_success_records(100), failed=False, show_all=True)

    assert "Generation Summary" in out
    assert "p000" in out
    assert "p050" in out
    assert "p099" in out
    assert "Generated" in out


def test_validate_default_full_success_no_table():
    out = _capture_validate(_validate_success_records(100), failed=False)

    assert "Validation Summary" not in out
    assert "p000" not in out
    assert "p099" not in out
    assert "Validated 100 pipelines" in out
    assert "all passed" in out


def test_validate_partial_failure_failures_only():
    records = _validate_mixed_records(success=97, failed=3)
    out = _capture_validate(records, failed=True)

    assert "Validation Summary" in out
    assert "bad000" in out
    assert "bad001" in out
    assert "bad002" in out
    assert "ok000" not in out
    assert "ok096" not in out
    assert "97" in out
    assert "passed" in out
    assert "3" in out
    assert "failed" in out


def test_generate_summary_footer_warning_count_full_success():
    out = _capture_generate(_success_records(100), warning_count=1)
    assert "all 100 pipelines passed" in out
    assert "1 warning" in out
    assert "1 warnings" not in out  # singular form required


def test_generate_summary_footer_warning_count_partial_failure():
    out = _capture_generate(
        _mixed_records(success=97, failed=3),
        failed=True,
        warning_count=2,
    )
    assert "3 of 100 pipelines failed" in out
    assert "2 warnings" in out


def test_validate_summary_footer_warning_count_full_success():
    out = _capture_validate(_validate_success_records(100), warning_count=1)
    assert "Validated 100 pipelines" in out
    assert "all passed" in out
    assert "1 warning" in out
    assert "1 warnings" not in out


def test_validate_summary_footer_warning_count_partial_failure():
    out = _capture_validate(
        _validate_mixed_records(success=97, failed=3),
        failed=True,
        warning_count=3,
    )
    assert "97" in out and "3" in out
    assert "3 warnings" in out


def test_generate_footer_includes_show_all_hint_when_total_gt_one():
    out = _capture_generate(_success_records(5))
    assert "all 5 pipelines passed" in out
    assert "(use -a to list)" in out


def test_generate_footer_omits_show_all_hint_when_single_pipeline():
    out = _capture_generate(_success_records(1))
    assert "all 1 pipeline passed" in out
    assert "(use -a to list)" not in out


def test_generate_footer_omits_show_all_hint_when_show_all_true():
    out = _capture_generate(_success_records(5), show_all=True)
    assert "(use -a to list)" not in out


def test_validate_footer_includes_show_all_hint_when_total_gt_one():
    out = _capture_validate(_validate_success_records(5))
    assert "Validated 5 pipelines" in out
    assert "(use -a to list)" in out


def test_validate_footer_omits_show_all_hint_when_single_pipeline():
    out = _capture_validate(_validate_success_records(1))
    assert "Validated 1 pipeline" in out
    assert "(use -a to list)" not in out


def test_validate_footer_omits_show_all_hint_when_show_all_true():
    out = _capture_validate(_validate_success_records(5), show_all=True)
    assert "(use -a to list)" not in out

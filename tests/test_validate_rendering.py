"""Snapshot test for ``lhp validate`` post-Live summary table rendering.

Phase D moved per-pipeline rendering OUT of the Live frame: the inline
``_display_pipeline_validation_results`` per-completion table is gone,
replaced by a single ``print_validate_summary_table`` call after Live
exits. This test pins the Pipeline / Errors / Warnings columns and the
status-icon contract; per-failure rich Panels are covered separately
by ``test_lhperror_rendering`` (the LHPError ``__rich__`` snapshot).
"""

from io import StringIO

from rich.console import Console as RichConsole

import lhp.cli.console as _lhp_console_module
from lhp.cli.live_panel import PipelineRecord
from lhp.cli.validate_summary import print_validate_summary_table


def _render_table(
    records: dict[str, PipelineRecord],
    *,
    failed: bool = False,
    show_all: bool = False,
) -> str:
    """Render ``print_validate_summary_table`` to a fixed-width Console."""
    buf = StringIO()
    fake = RichConsole(file=buf, force_terminal=False, no_color=True, width=80)
    old = _lhp_console_module.console
    try:
        _lhp_console_module.console = fake
        print_validate_summary_table(records, failed=failed, show_all=show_all)
    finally:
        _lhp_console_module.console = old
    return buf.getvalue()


def test_validate_summary_all_pass_renders_green_check():
    """All-pass batch renders ✓ marker for every row."""
    records = {
        "bronze_pipeline": PipelineRecord(
            name="bronze_pipeline", success=True, errors_count=0, warnings_count=0
        )
    }
    # The default failures-only contract suppresses the per-pipeline
    # table on a clean pass; this test verifies the per-row rendering
    # itself, so opt into the full table via ``show_all=True``.
    out = _render_table(records, show_all=True)
    assert "✓" in out
    assert "bronze_pipeline" in out
    assert "Pipelines validated" in out


def test_validate_summary_failure_renders_rich_table(snapshot):
    """Failing batch renders the summary table with error/warning counts."""
    records = {
        "silver_pipeline": PipelineRecord(
            name="silver_pipeline",
            success=False,
            errors_count=2,
            warnings_count=1,
            error_code="LHP-VAL-021",
        ),
        "gold_pipeline": PipelineRecord(
            name="gold_pipeline",
            success=True,
            errors_count=0,
            warnings_count=0,
        ),
    }
    # The snapshot captures the unfiltered table (both passing and
    # failing rows). The post-Phase-E default filters to failures only,
    # so opt into the full table via ``show_all=True`` to preserve the
    # original rendering contract this snapshot was written for.
    assert _render_table(records, failed=True, show_all=True) == snapshot

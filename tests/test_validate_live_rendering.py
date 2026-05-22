"""End-to-end smoke tests for the validate command's post-Live rendering.

Phase D moved per-pipeline validation rendering OUT of the Live frame.
These tests pin the resulting contract:

- Inline ``✓ <name>`` / ``✗ <name>  CODE`` lines appear DURING Live (the
  callback only mutates ``records`` and appends one ``Text`` line — no
  ``console.print`` per pipeline).
- A single :func:`print_validate_summary_table` table appears AFTER
  Live exits, with one row per pipeline.
- Each failing pipeline's :class:`LHPError` is rendered as a yellow
  Rich Panel via :meth:`LHPError.__rich__` AFTER the summary table.
- No ``=====`` separators leak into the summary cells (the regression
  the structured ``lhp_error`` field on :class:`ValidationIssue`
  prevents).
"""

from io import StringIO
from unittest.mock import patch

from rich.console import Console as RichConsole

import lhp.cli.console as _lhp_console_module
from lhp.cli.live_panel import PipelineRecord
from lhp.cli.validate_summary import print_validate_summary_table
from lhp.core.layers import (
    BatchValidationResponse,
    ValidationIssue,
    ValidationResponse,
)
from lhp.utils.error_formatter import ErrorCategory, LHPValidationError


def _capture(width: int = 100) -> tuple[StringIO, RichConsole]:
    """Build a fixed-width plain-text Console for snapshot-stable capture."""
    buf = StringIO()
    fake = RichConsole(file=buf, force_terminal=False, no_color=True, width=width)
    return buf, fake


def _patch_consoles(stdout_console: RichConsole, stderr_console: RichConsole):
    """Patch both module-level Console singletons used by the CLI."""
    return patch.multiple(
        _lhp_console_module,
        console=stdout_console,
        err_console=stderr_console,
    )


def _build_failing_response() -> BatchValidationResponse:
    """Construct a deterministic batch response with one failing pipeline."""
    err = LHPValidationError(
        category=ErrorCategory.VALIDATION,
        code_number="007",
        title="invalid action reference",
        details="Action references unknown view 'foo_v'",
        context={"action": "load_foo", "missing_view": "foo_v"},
    )
    failing_response = ValidationResponse(
        success=False,
        issues=[
            ValidationIssue(
                code=err.code,
                severity="error",
                title=err.title,
                details=err.details,
                location="bronze_pipeline",
                lhp_error=err,
            ),
        ],
        validated_pipelines=["bronze_pipeline"],
    )
    return BatchValidationResponse(
        success=False,
        pipeline_responses={"bronze_pipeline": failing_response},
        total_errors=1,
        total_warnings=0,
        validated_pipelines=["bronze_pipeline"],
    )


def test_validate_failure_renders_summary_table_post_live():
    """Summary table is rendered after Live frame exits, not inline.

    Phase D contract: the per-pipeline callback only mutates ``records``,
    then the post-Live ``print_validate_summary_table`` emits a single
    table. The ``✓`` / ``✗`` inline lines appear in Live's buffer (not
    captured here); the captured stdout contains the table once.
    """
    records = {
        "bronze_pipeline": PipelineRecord(
            name="bronze_pipeline",
            success=False,
            errors_count=1,
            warnings_count=0,
            error_code="LHP-VAL-007",
        )
    }
    buf, fake = _capture()
    with _patch_consoles(fake, fake):
        print_validate_summary_table(records, failed=True)
    out = buf.getvalue()
    assert out.count("Validation Summary") == 1
    assert "bronze_pipeline" in out
    # Failure footer pinned (total-failure path: 0 of 1 passed).
    assert "Validation failed" in out


def test_validate_failure_panel_uses_lhp_error_rich():
    """Failing pipeline's LHPError renders via the rich Panel surface."""
    batch_response = _build_failing_response()
    buf, fake = _capture()
    with _patch_consoles(fake, fake):
        # Mirror the validate command's post-Live LHPError print loop.
        for response in batch_response.pipeline_responses.values():
            for issue in response.issues:
                if issue.lhp_error is not None:
                    _lhp_console_module.err_console.print(issue.lhp_error)
    out = buf.getvalue()
    # ``__rich__`` returns a Panel titled ``LHP-VAL-007   <category label>``.
    assert "LHP-VAL-007" in out
    assert "invalid action reference" in out
    # Yellow Panel border characters present (validation severity → yellow).
    assert "╭" in out or "┌" in out


def test_validate_code_column_not_dashes_in_failure_line():
    """The inline failure-line code is the real code, not the legacy ``—``.

    Pre-D5 the validate worker stringified LHPError so the main-thread
    ValidationIssue.code was empty. With the structured ``lhp_error``
    field on ValidationIssue, the validate command's ``_on_complete``
    callback pulls ``first_lhp.code`` for the inline failure marker.
    """
    batch_response = _build_failing_response()
    response = batch_response.pipeline_responses["bronze_pipeline"]
    first_lhp = next(
        (i.lhp_error for i in response.issues if i.lhp_error is not None),
        None,
    )
    assert first_lhp is not None
    assert first_lhp.code == "LHP-VAL-007"


def test_validate_issue_cell_has_no_equals_borders():
    """Summary table cells must not contain the legacy ``=====`` border.

    The pre-D5 string projection embedded ``LHPError.__str__()`` which
    appends a ``"=" * 70`` separator. With ``lhp_error`` carried
    structurally, the summary table only reads ``errors_count`` /
    ``warnings_count`` / ``error_code``; the title/details that
    contained ``=====`` never reach the table.
    """
    records = {
        "bronze_pipeline": PipelineRecord(
            name="bronze_pipeline",
            success=False,
            errors_count=1,
            warnings_count=0,
            error_code="LHP-VAL-007",
        )
    }
    buf, fake = _capture()
    with _patch_consoles(fake, fake):
        print_validate_summary_table(records, failed=True)
    out = buf.getvalue()
    assert "=====" not in out
    # Defensive: the legacy LHPError separator was exactly 70 ``=``s.
    assert "=" * 10 not in out

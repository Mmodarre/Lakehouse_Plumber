"""Tests for the post-run summary presenter (generate / validate).

Covers the counts banner ("generated" vs "validated", warning + failed
counts), the failure attribution block (pipeline-level vs
pipeline/flowgroup/file enrichment), the success-path hint omission, and
the ``--show-details`` per-failure panel expansion.
"""

import io

import pytest

from lhp.api.responses import (
    BatchGenerationResponse,
    BatchValidationResponse,
    GenerationResponse,
    ValidationResponse,
)
from lhp.api.views import ValidationIssueView
from lhp.cli.presenters.event_stream._model import (
    FailureLine,
    RenderOptions,
    RunHeader,
    RunOutcome,
)
from lhp.cli.presenters.summary_presenter import print_run_summary

rich = pytest.importorskip("rich")
from rich.console import Console  # noqa: E402


def capture_err_console(width: int = 100):
    """Return a ``(console, buffer)`` pair capturing a no-color stderr Console.

    Mirrors ``tests.conftest.capture_lhp_console`` but for an ad-hoc
    stderr console passed straight into ``print_run_summary`` — no
    singleton swap needed, the presenter takes ``err_console`` as a
    parameter. Read rendered text via ``buffer.getvalue()``.
    """
    buf = io.StringIO()
    console = Console(
        file=buf, stderr=True, force_terminal=False, no_color=True, width=width
    )
    return console, buf


# --------------------------------------------------------------------------- #
# Fixtures: terminal responses + outcomes                                      #
# --------------------------------------------------------------------------- #


def _validation_issue(
    *,
    code="LHP-VAL-021",
    title="Missing target",
    details="The write action has no target.",
    pipeline="bronze",
    flowgroup="ingest",
    file="ingest.yaml",
    suggestions=("Add a write_target block",),
    doc_link="https://example.test/errors#LHP-VAL-021",
):
    from pathlib import Path

    # The structured path mirrors ``ErrorCategory`` onto ``category``; derive
    # it from the code's middle segment (``LHP-<CAT>-NNN``) so the fixture is
    # faithful to how a real ``ValidationIssueView`` is populated.
    category = code.split("-")[1] if code.count("-") >= 2 else "VAL"
    return ValidationIssueView(
        code=code,
        category=category,
        severity="error",
        title=title,
        details=details,
        pipeline_name=pipeline,
        flowgroup_name=flowgroup,
        file_path=Path(file) if file else None,
        suggestions=tuple(suggestions),
        context={"action": "write_customers"},
        doc_link=doc_link,
    )


def _validate_outcome():
    """A validate run: 2 validated, 1 warning, 2 failed (with flowgroup+file)."""
    issue_a = _validation_issue(
        code="LHP-VAL-021", pipeline="bronze", flowgroup="ingest", file="ingest.yaml"
    )
    issue_b = _validation_issue(
        code="LHP-VAL-005",
        title="Duplicate action name",
        pipeline="silver",
        flowgroup="enrich",
        file="enrich.yaml",
    )
    response = BatchValidationResponse(
        success=False,
        pipeline_responses={
            "bronze": ValidationResponse(
                success=False, issues=(issue_a,), validated_pipelines=("bronze",)
            ),
            "silver": ValidationResponse(
                success=False, issues=(issue_b,), validated_pipelines=("silver",)
            ),
            "gold": ValidationResponse(
                success=True, issues=(), validated_pipelines=("gold",)
            ),
            "platinum": ValidationResponse(
                success=True, issues=(), validated_pipelines=("platinum",)
            ),
        },
        total_errors=2,
        total_warnings=1,
        validated_pipelines=("bronze", "silver", "gold", "platinum"),
    )
    failures = (
        FailureLine(
            pipeline="bronze",
            code="LHP-VAL-021",
            message="Missing target",
            flowgroup="ingest",
            file="ingest.yaml",
        ),
        FailureLine(
            pipeline="silver",
            code="LHP-VAL-005",
            message="Duplicate action name",
            flowgroup="enrich",
            file="enrich.yaml",
        ),
    )
    from lhp.cli.presenters.event_stream._model import WarningLine

    warnings = (
        WarningLine(code="LHP-DEP-001", message="deprecated syntax", file=None),
    )
    return RunOutcome(
        response=response, warnings=warnings, failures=failures, errored=False
    )


def _generate_outcome():
    """A generate run: 1 generated, 0 warnings, 1 pipeline-level failure."""
    issue = _validation_issue(
        code="LHP-CFG-014",
        title="No pipelines found",
        details="The environment resolved zero flowgroups.",
        pipeline="bronze",
        flowgroup=None,
        file=None,
    )
    response = BatchGenerationResponse(
        success=False,
        pipeline_responses={
            "bronze": GenerationResponse(
                success=False,
                generated_filenames=(),
                files_written=0,
                total_flowgroups=0,
                output_location=None,
                performance_info={},
                error_message="No pipelines found",
                error_code="LHP-CFG-014",
                error=issue,
            ),
            "silver": GenerationResponse(
                success=True,
                generated_filenames=("a.py",),
                files_written=1,
                total_flowgroups=1,
                output_location=None,
                performance_info={},
            ),
        },
        total_files_written=1,
        aggregate_generated_filenames=("a.py",),
        output_location=None,
    )
    failures = (
        FailureLine(
            pipeline="bronze",
            code="LHP-CFG-014",
            message="No pipelines found",
        ),
    )
    return RunOutcome(response=response, warnings=(), failures=failures, errored=False)


def _success_outcome():
    response = BatchGenerationResponse(
        success=True,
        pipeline_responses={
            "bronze": GenerationResponse(
                success=True,
                generated_filenames=("a.py",),
                files_written=1,
                total_flowgroups=1,
                output_location=None,
                performance_info={},
            ),
        },
        total_files_written=1,
        aggregate_generated_filenames=("a.py",),
        output_location=None,
    )
    return RunOutcome(response=response, warnings=(), failures=(), errored=False)


# --------------------------------------------------------------------------- #
# Counts banner                                                                #
# --------------------------------------------------------------------------- #


def test_generate_counts_line_uses_generated_and_failed():
    outcome = _generate_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=4.43,
        options=RenderOptions(),
        err_console=console,
    )
    text = buf.getvalue()
    # Generate now names the unit and carries a file count derived from the
    # response (1 success -> singular "pipeline"; total_files_written=1 -> "file").
    assert "1 pipeline generated" in text
    assert "1 file" in text
    assert "validated" not in text
    assert "1 failed" in text
    assert "4.4s" in text


def test_generate_counts_line_pluralizes_pipelines_and_files():
    # Counts are read off the terminal response (per-pipeline success +
    # total_files_written): the header carries no count at all, so the banner
    # can only have come from the response.
    response = BatchGenerationResponse(
        success=True,
        pipeline_responses={
            "bronze": GenerationResponse(
                success=True,
                generated_filenames=("a.py",),
                files_written=1,
                total_flowgroups=1,
                output_location=None,
                performance_info={},
            ),
            "silver": GenerationResponse(
                success=True,
                generated_filenames=("b.py", "c.py"),
                files_written=2,
                total_flowgroups=2,
                output_location=None,
                performance_info={},
            ),
        },
        total_files_written=3,
        aggregate_generated_filenames=("a.py", "b.py", "c.py"),
        output_location=None,
    )
    outcome = RunOutcome(response=response, warnings=(), failures=(), errored=False)
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=1.0, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    # 2 successes -> "2 pipelines generated"; total_files_written=3 -> "3 files".
    assert "2 pipelines generated" in text
    assert "3 files" in text


def test_validate_counts_line_uses_validated_and_warning_and_failed():
    outcome = _validate_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=2.0,
        options=RenderOptions(),
        err_console=console,
    )
    text = buf.getvalue()
    assert "2 validated" in text
    assert "generated" not in text
    assert "1 warning" in text
    assert "2 failed" in text


def test_warning_plural_two_warnings():
    outcome = _validate_outcome()
    from lhp.cli.presenters.event_stream._model import WarningLine

    outcome = RunOutcome(
        response=outcome.response,
        warnings=(
            WarningLine(code="A", message="one", file=None),
            WarningLine(code="B", message="two", file=None),
        ),
        failures=outcome.failures,
        errored=False,
    )
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=2.0, options=RenderOptions(), err_console=console
    )
    assert "2 warnings" in buf.getvalue()


# --------------------------------------------------------------------------- #
# Failure attribution block                                                    #
# --------------------------------------------------------------------------- #


def test_validate_failures_include_flowgroup_and_file_segments():
    outcome = _validate_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=2.0, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    # pipeline / flowgroup  file  CODE  msg
    assert "bronze" in text
    assert "ingest" in text
    assert "ingest.yaml" in text
    assert "LHP-VAL-021" in text
    assert "silver" in text
    assert "enrich" in text
    assert "enrich.yaml" in text
    assert "LHP-VAL-005" in text


def test_generate_failures_are_pipeline_level_no_flowgroup_segment():
    outcome = _generate_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console(width=200)
    print_run_summary(
        outcome, header, elapsed_s=1.0, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    assert "bronze" in text
    assert "LHP-CFG-014" in text
    # Pipeline-level failure carries no flowgroup/file -> no " / " separator
    # and no ".yaml" segment in the failure block.
    assert " / " not in text
    assert ".yaml" not in text


# --------------------------------------------------------------------------- #
# Preflight failure (batch-level, empty-pipeline)                              #
# --------------------------------------------------------------------------- #


def _preflight_failure_outcome():
    """A validate run halted by a folded preflight (e.g. CFG-023).

    Mirrors the non-raising validate path: ``_build_validation_batch_from_issues``
    yields a failed ``BatchValidationResponse`` with NO per-pipeline entries but
    a batch-level ``error_code`` / ``error_message``, and ``merge_terminal_validation``
    synthesizes exactly one empty-pipeline ``FailureLine`` from it (see
    ``event_stream/_outcome.py``). This is the shape the summary must surface as
    an explicit stop rather than as a missing stage.
    """
    response = BatchValidationResponse(
        success=False,
        pipeline_responses={},
        total_errors=1,
        total_warnings=0,
        validated_pipelines=(),
        error_message="Error [LHP-CFG-023]: Project preflight failed",
        error_code="LHP-CFG-023",
    )
    failures = (
        FailureLine(
            pipeline="",
            code="LHP-CFG-023",
            message="Error [LHP-CFG-023]: Project preflight failed",
        ),
    )
    return RunOutcome(response=response, warnings=(), failures=failures, errored=False)


def test_preflight_failure_is_surfaced_explicitly():
    outcome = _preflight_failure_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=1.2, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    # Reads as an explicit error that stopped the run, not a missing stage.
    assert "preflight failed" in text
    assert "later stages not run" in text
    # The code still surfaces, and the banner tallies the one failure.
    assert "LHP-CFG-023" in text
    assert "1 failed" in text
    # Still points the user at the next step.
    assert "lhp validate --env prod --show-details" in text


def test_preflight_failure_omits_bare_empty_pipeline_line():
    # The synthetic FailureLine has an empty pipeline; the explicit-stop line
    # MUST be printed instead of the bare _failure_line (which would render the
    # empty pipeline + lone code as if a stage were simply missing).
    outcome = _preflight_failure_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console(width=200)
    print_run_summary(
        outcome, header, elapsed_s=1.2, options=RenderOptions(), err_console=console
    )
    lines = [ln for ln in buf.getvalue().splitlines() if ln.strip()]
    # The failure line is the dedicated preflight line — it leads with the code
    # immediately followed by the explicit phrase (no empty-pipeline prefix).
    failure_lines = [ln for ln in lines if "LHP-CFG-023" in ln]
    assert len(failure_lines) == 1
    assert failure_lines[0].lstrip().startswith("LHP-CFG-023  preflight failed")


def test_preflight_failure_explicit_under_show_details():
    # --show-details must NOT fall back to render_issue_panel here: the batch
    # carries no per-pipeline ValidationIssueView to expand, so the explicit
    # stop line is the right surface regardless of the flag.
    outcome = _preflight_failure_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=1.2,
        options=RenderOptions(show_details=True),
        err_console=console,
    )
    text = buf.getvalue()
    assert "preflight failed" in text
    assert "later stages not run" in text


# --------------------------------------------------------------------------- #
# Next-step hint                                                               #
# --------------------------------------------------------------------------- #


def test_success_prints_no_next_step_hint():
    outcome = _success_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=0.5, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    assert "1 pipeline generated" in text
    assert "Re-run" not in text
    assert "--show-details" not in text


def test_failures_print_next_step_hint():
    outcome = _validate_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=2.0, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    assert "lhp validate --env prod --show-details" in text


# --------------------------------------------------------------------------- #
# show_details -> per-failure panels                                           #
# --------------------------------------------------------------------------- #


def test_show_details_prints_panel_per_failure():
    outcome = _validate_outcome()
    header = RunHeader(command="validate", env="prod")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=2.0,
        options=RenderOptions(show_details=True),
        err_console=console,
    )
    text = buf.getvalue()
    # error_panel-shaped: title in panel header, plus Suggestions / Context /
    # More info sections from the ValidationIssueView fields.
    assert "LHP-VAL-021" in text
    assert "Validation Error" in text  # category label in the panel title
    assert "Suggestions" in text
    assert "Add a write_target block" in text
    assert "Context" in text
    assert "More info:" in text
    # Both failures expanded.
    assert "LHP-VAL-005" in text
    assert "Duplicate action name" in text


def test_show_details_generate_pipeline_level_panel():
    outcome = _generate_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=1.0,
        options=RenderOptions(show_details=True),
        err_console=console,
    )
    text = buf.getvalue()
    assert "LHP-CFG-014" in text
    assert "No pipelines found" in text
    assert "Configuration Error" in text  # category label from "CFG"


# --------------------------------------------------------------------------- #
# Warning rollup (collapsed default view)                                      #
# --------------------------------------------------------------------------- #


def _warnings_outcome():
    """A clean generate run carrying 3 warnings across 2 codes (DEPR-001 ×2)."""
    from lhp.cli.presenters.event_stream._model import WarningLine

    response = BatchGenerationResponse(
        success=True,
        pipeline_responses={
            "bronze": GenerationResponse(
                success=True,
                generated_filenames=("a.py",),
                files_written=1,
                total_flowgroups=1,
                output_location=None,
                performance_info={},
            ),
        },
        total_files_written=1,
        aggregate_generated_filenames=("a.py",),
        output_location=None,
    )
    warnings = (
        WarningLine(
            code="LHP-DEPR-001", message="Deprecated bare {token} syntax", file="a.yaml"
        ),
        WarningLine(
            code="LHP-DEPR-001", message="Deprecated bare {token} syntax", file="b.yaml"
        ),
        WarningLine(
            code="LHP-DEPR-002", message="deprecated field 'database'", file="c.yaml"
        ),
    )
    return RunOutcome(response=response, warnings=warnings, failures=(), errored=False)


def test_warning_rollup_lists_codes_when_details_collapsed():
    outcome = _warnings_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome, header, elapsed_s=1.0, options=RenderOptions(), err_console=console
    )
    text = buf.getvalue()
    # Header: total + distinct-code count + the opt-in hint.
    assert "3 warnings (2 types)" in text
    assert "--show-details" in text
    assert "to list" in text
    # One row per distinct code, each with its ×count tail (DEPR-001 collapsed
    # two files into ×2).
    assert "LHP-DEPR-001" in text
    assert "×2" in text
    assert "LHP-DEPR-002" in text
    assert "×1" in text


def test_warning_rollup_absent_with_show_details():
    # With --show-details the renderer already streamed the full per-file
    # lines, so the summary must NOT also print the collapsed rollup.
    outcome = _warnings_outcome()
    header = RunHeader(command="generate", env="dev")
    console, buf = capture_err_console()
    print_run_summary(
        outcome,
        header,
        elapsed_s=1.0,
        options=RenderOptions(show_details=True),
        err_console=console,
    )
    text = buf.getvalue()
    # The banner still tallies the warnings...
    assert "3 warnings" in text
    # ...but no per-code rollup rows (no ×count tail, no opt-in hint).
    assert "×" not in text
    assert "to list" not in text

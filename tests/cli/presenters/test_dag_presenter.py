"""Tests for the ``dag`` command's dependency-analysis presenter.

Covers the extraction-warnings section: the count header, the 10-line
detail cap with the ``... and N more`` overflow line, the single trailing
``depends_on`` hint, location formatting (file:line / file-only / none),
and full suppression of the section for a warning-free result.
"""

import io

import pytest

from lhp.api.responses import DependencyAnalysisResult, DependencyWarningView
from lhp.cli.presenters.dag_presenter import render_analysis

rich = pytest.importorskip("rich")
from rich.console import Console  # noqa: E402


def capture_err_console(width: int = 200):
    """Return a ``(console, buffer)`` pair capturing a no-color stderr Console.

    Mirrors the summary-presenter test helper: ``render_analysis`` takes the
    console as a parameter (the command passes its stderr console), so no
    singleton swap is needed. The wide width keeps detail lines unwrapped.
    """
    buf = io.StringIO()
    console = Console(
        file=buf, stderr=True, force_terminal=False, no_color=True, width=width
    )
    return console, buf


def _warning(
    index: int,
    *,
    file_path=None,
    line=None,
) -> DependencyWarningView:
    return DependencyWarningView(
        code="LHP-DEP-002",
        message=f"could not resolve source table for read {index}",
        flowgroup=f"fg{index}",
        action=f"load_{index}",
        suggestion="Add an explicit depends_on declaration",
        file_path=file_path,
        line=line,
    )


def _result(warnings=()) -> DependencyAnalysisResult:
    return DependencyAnalysisResult(
        pipeline_dependencies={"bronze": ()},
        execution_stages=(("bronze",),),
        circular_dependencies=(),
        external_sources=(),
        total_pipelines=1,
        total_external_sources=0,
        warnings=tuple(warnings),
    )


def test_twelve_warnings_render_ten_details_overflow_and_single_hint() -> None:
    """12 warnings -> count header, exactly 10 detail lines, '... and 2 more',
    and exactly one trailing depends_on hint — all on the stderr console."""
    warnings = [
        _warning(i, file_path=f"pipelines/fg{i}.yaml", line=i + 1) for i in range(12)
    ]
    console, buf = capture_err_console()

    render_analysis(_result(warnings), console=console)

    output = buf.getvalue()
    lines = output.splitlines()

    assert "12 dependency extraction warning(s):" in output

    detail_lines = [ln for ln in lines if ln.strip().startswith("LHP-DEP-")]
    assert len(detail_lines) == 10
    # First and last rendered details; the 11th/12th warnings are cut.
    assert "LHP-DEP-002 fg0.load_0 (pipelines/fg0.yaml:1): " in detail_lines[0]
    assert "LHP-DEP-002 fg9.load_9 (pipelines/fg9.yaml:10): " in detail_lines[9]
    assert "fg10.load_10" not in output
    assert "fg11.load_11" not in output

    assert output.count("... and 2 more (see JSON output)") == 1

    hint = "Declare explicit 'depends_on' for reads LHP cannot resolve."
    assert output.count(hint) == 1
    # The hint is the last line of the warnings section (and of the render).
    assert lines[-1].strip() == hint


def test_warning_free_result_renders_no_warnings_section() -> None:
    """Zero warnings -> no header, no detail lines, no hint — nothing at all."""
    console, buf = capture_err_console()

    render_analysis(_result(()), console=console)

    output = buf.getvalue()
    assert "dependency extraction warning" not in output
    assert "LHP-DEP-" not in output
    assert "Declare explicit 'depends_on'" not in output
    assert "(see JSON output)" not in output


def test_warning_location_formats_adapt_to_available_fields() -> None:
    """file+line -> (file:line); file only -> (file); neither -> no parens."""
    warnings = [
        _warning(0, file_path="pipelines/fg0.yaml", line=7),
        _warning(1, file_path="pipelines/fg1.yaml"),
        _warning(2),
    ]
    console, buf = capture_err_console()

    render_analysis(_result(warnings), console=console)

    output = buf.getvalue()
    assert (
        "LHP-DEP-002 fg0.load_0 (pipelines/fg0.yaml:7): "
        "could not resolve source table for read 0" in output
    )
    assert (
        "LHP-DEP-002 fg1.load_1 (pipelines/fg1.yaml): "
        "could not resolve source table for read 1" in output
    )
    # No location -> the parenthesised part is omitted entirely.
    assert "LHP-DEP-002 fg2.load_2: could not resolve source table for read 2" in output
    assert "fg2.load_2 (" not in output
    # No overflow line for 3 warnings.
    assert "(see JSON output)" not in output

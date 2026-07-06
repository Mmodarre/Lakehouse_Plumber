"""Tests for the ``dag`` command's dependency-analysis presenter.

Covers the extraction-warnings section: the count header (sites +
affected actions), the grouped per-site table (code / location / reason /
capped affected list / depends_on edit paths), the 50-site cap with an
overflow row, the single trailing ``depends_on`` hint, and full
suppression of the section for a warning-free result. The tests capture a
non-TTY console, where ``render_listing_table`` emits tab-separated rows
verbatim.
"""

import io

import pytest

from lhp.api.responses import (
    AffectedActionView,
    DependencyAnalysisResult,
    DependencyWarningView,
)
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


def test_grouped_sites_render_rows_overflow_and_single_hint() -> None:
    """52 sites -> count header, exactly 50 table rows, one '+2 more site(s)'
    overflow row, and exactly one trailing depends_on hint."""
    warnings = [
        _warning(i, file_path=f"pipelines/fg{i}.yaml", line=i + 1) for i in range(52)
    ]
    console, buf = capture_err_console()

    render_analysis(_result(warnings), console=console)

    output = buf.getvalue()
    lines = output.splitlines()

    assert "52 unresolved read site(s) affecting 52 action(s):" in output

    site_rows = [ln for ln in lines if ln.startswith("LHP-DEP-")]
    assert len(site_rows) == 50
    # First and last rendered sites; the 51st/52nd are cut behind the overflow row.
    assert site_rows[0].startswith("LHP-DEP-002\tpipelines/fg0.yaml:1\t")
    assert site_rows[49].startswith("LHP-DEP-002\tpipelines/fg49.yaml:50\t")
    assert "fg50.load_50" not in output
    assert "fg51.load_51" not in output

    assert output.count("+2 more site(s)") == 1
    assert output.count("see JSON output") == 1

    hint = "Declare explicit 'depends_on' for reads LHP cannot resolve."
    assert output.count(hint) == 1
    # The hint is the last line of the warnings section (and of the render).
    assert lines[-1].strip() == hint


def test_warning_free_result_renders_no_warnings_section() -> None:
    """Zero warnings -> no header, no table, no hint — nothing at all."""
    console, buf = capture_err_console()

    render_analysis(_result(()), console=console)

    output = buf.getvalue()
    assert "unresolved read site" not in output
    assert "LHP-DEP-" not in output
    assert "Declare explicit 'depends_on'" not in output
    assert "Extraction warnings" not in output


def test_warning_location_formats_adapt_to_available_fields() -> None:
    """file+line -> file:line; file only -> file; neither -> empty cell."""
    warnings = [
        _warning(0, file_path="pipelines/fg0.yaml", line=7),
        _warning(1, file_path="pipelines/fg1.yaml"),
        _warning(2),
    ]
    console, buf = capture_err_console()

    render_analysis(_result(warnings), console=console)

    output = buf.getvalue()
    assert (
        "LHP-DEP-002\tpipelines/fg0.yaml:7\t"
        "could not resolve source table for read 0\t1: fg0.load_0\t-" in output
    )
    assert (
        "LHP-DEP-002\tpipelines/fg1.yaml\t"
        "could not resolve source table for read 1\t1: fg1.load_1\t-" in output
    )
    # No location -> empty location cell.
    assert (
        "LHP-DEP-002\t\t"
        "could not resolve source table for read 2\t1: fg2.load_2\t-" in output
    )
    # No overflow row for 3 sites.
    assert "more site(s)" not in output


def test_aggregated_site_renders_capped_affected_list_and_edit_paths() -> None:
    """One site affecting five actions renders the count, the first three
    flowgroup.action pairs, a '+2 more' marker, and the distinct edit paths."""
    affected = tuple(
        AffectedActionView(
            flowgroup=f"fg{i}",
            action=f"load_{i}",
            edit_yaml_path=f"pipelines/fg{i}.yaml",
        )
        for i in range(5)
    )
    warning = DependencyWarningView(
        code="LHP-DEP-002",
        message="could not resolve source table for read 0",
        flowgroup="fg0",
        action="load_0",
        suggestion="Add an explicit depends_on declaration",
        file_path="py_functions/helper.py",
        line=4,
        edit_yaml_path="pipelines/fg0.yaml",
        affected_actions=affected,
        affected_count=5,
    )
    console, buf = capture_err_console(width=400)

    render_analysis(_result([warning]), console=console)

    output = buf.getvalue()
    assert "1 unresolved read site(s) affecting 5 action(s):" in output
    assert "5: fg0.load_0, fg1.load_1, fg2.load_2, +2 more" in output
    # Edit-path cell caps at three distinct YAML paths.
    assert "pipelines/fg0.yaml, pipelines/fg1.yaml, pipelines/fg2.yaml" in output
    assert "pipelines/fg3.yaml" not in output.split("5: fg0.load_0")[1]

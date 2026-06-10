"""Unit tests for the plan-vs-disk diff presenter.

The presenter is a pure dict / set comparison plus Rich rendering: it folds
a hand-built :class:`GenerationPlan` into a ``{path: content}`` map and diffs
it against an on-disk snapshot. These tests cover all four cases (identical,
modified, plan-only, disk-only) and assert both the rendered ``~`` / ``+`` /
``-`` lines and the returned bool.
"""

from __future__ import annotations

import io
from pathlib import Path

import pytest
from rich.console import Console

from lhp.api import GenerationPlan, PlannedFileView
from lhp.cli.presenters.diff_presenter import render_diff


def _plan(*paths_and_contents: tuple[str, str]) -> GenerationPlan:
    """Build a GenerationPlan from ``(path, content)`` pairs."""
    files = tuple(
        PlannedFileView(
            path=Path(path),
            content=content,
            pipeline="p",
            kind="flowgroup",
        )
        for path, content in paths_and_contents
    )
    return GenerationPlan(
        files=files,
        output_location=None,
        pipeline_count=1,
        file_count=len(files),
    )


def _render(plan: GenerationPlan, on_disk, *, show_details: bool = False):
    """Render to an in-memory plain-text console; return (changed, text)."""
    buf = io.StringIO()
    console = Console(file=buf, force_terminal=False, no_color=True, width=200)
    changed = render_diff(plan, on_disk, console=console, show_details=show_details)
    return changed, buf.getvalue()


@pytest.mark.unit
def test_all_four_cases_render_expected_lines():
    """identical + modified + plan-only + disk-only in one diff."""
    plan = _plan(
        ("out/identical.py", "same\n"),  # identical -> no line
        ("out/modified.py", "new content\n"),  # modified
        ("out/create.py", "fresh\n"),  # plan-only -> would-create
    )
    on_disk = {
        "out/identical.py": "same\n",
        "out/modified.py": "old content\n",
        "out/orphan.py": "stale\n",  # disk-only -> orphan
    }

    changed, text = _render(plan, on_disk)

    assert changed is True
    # would-create line for the plan-only path
    assert "+ would-create  out/create.py" in text
    # modified line for the content-differing intersection path
    assert "~ modified  out/modified.py" in text
    # orphan line for the disk-only path
    assert "- orphan  out/orphan.py" in text
    # identical path produces no line at all
    assert "out/identical.py" not in text
    # without show_details, no unified-diff body leaks in
    assert "old content" not in text
    assert "new content" not in text


@pytest.mark.unit
def test_identical_returns_false_and_renders_no_change_markers():
    """Plan == disk: returns False, emits no ~/+/- lines."""
    plan = _plan(("out/a.py", "x\n"), ("out/b.py", "y\n"))
    on_disk = {"out/a.py": "x\n", "out/b.py": "y\n"}

    changed, text = _render(plan, on_disk)

    assert changed is False
    assert "~" not in text
    assert "+ would-create" not in text
    assert "- orphan" not in text


@pytest.mark.unit
def test_modified_only():
    """Single content difference -> one ~ line, changed True."""
    plan = _plan(("out/a.py", "after\n"))
    on_disk = {"out/a.py": "before\n"}

    changed, text = _render(plan, on_disk)

    assert changed is True
    assert "~ modified  out/a.py" in text
    assert "+ would-create" not in text
    assert "- orphan" not in text


@pytest.mark.unit
def test_plan_only_would_create():
    """Empty disk -> every planned file is would-create."""
    plan = _plan(("out/a.py", "a\n"), ("out/b.py", "b\n"))

    changed, text = _render(plan, {})

    assert changed is True
    assert "+ would-create  out/a.py" in text
    assert "+ would-create  out/b.py" in text
    assert "~" not in text


@pytest.mark.unit
def test_disk_only_orphan():
    """Empty plan -> every on-disk file is an orphan."""
    plan = _plan()
    on_disk = {"out/old.py": "gone\n"}

    changed, text = _render(plan, on_disk)

    assert changed is True
    assert "- orphan  out/old.py" in text
    assert "+ would-create" not in text


@pytest.mark.unit
def test_show_details_emits_unified_diff_for_modified():
    """With show_details, the modified file's unified diff body is rendered."""
    plan = _plan(("out/a.py", "line one\nline TWO\n"))
    on_disk = {"out/a.py": "line one\nline two\n"}

    changed, text = _render(plan, on_disk, show_details=True)

    assert changed is True
    assert "~ modified  out/a.py" in text
    # unified-diff header references both sides
    assert "out/a.py (on disk)" in text
    assert "out/a.py (planned)" in text
    # the changed lines appear with +/- prefixes
    assert "-line two" in text
    assert "+line TWO" in text

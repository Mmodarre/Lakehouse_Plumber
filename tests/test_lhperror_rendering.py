"""Snapshot tests for LHPError Rich Panel rendering.

The snapshot pins the structural contract of the Rich Panel output. Substring
assertions in other test files cover error-code presence for individual
commands; this file is the single source of truth for whole-panel rendering.
"""

from io import StringIO

from rich.console import Console

from lhp.cli.error_panel import render_error_panel
from lhp.errors import ErrorCategory, LHPError


def _render(error: LHPError) -> str:
    """Render an LHPError to plain text using a fixed-width no-color Console."""
    buf = StringIO()
    Console(file=buf, force_terminal=False, no_color=True, width=80).print(
        render_error_panel(error)
    )
    return buf.getvalue()


def test_lhperror_validation_panel(snapshot):
    err = LHPError(
        category=ErrorCategory.VALIDATION,
        code_number="021",
        title="Missing required field 'source'",
        details="Action 'load_customer' must specify a 'source' field.",
        context={"action": "load_customer", "flowgroup": "customer_bronze"},
        suggestions=["Add 'source: customer_raw' to the action"],
        example="source: customer_raw  # the upstream view name",
    )
    assert _render(err) == snapshot


def test_lhperror_config_panel(snapshot):
    err = LHPError(
        category=ErrorCategory.CONFIG,
        code_number="011",
        title="Not in a LakehousePlumber project directory",
        details="No lhp.yaml found in the current directory or any parent.",
        suggestions=["Run 'lhp init <project_name>' to create a new project"],
    )
    assert _render(err) == snapshot


def test_lhperror_io_panel(snapshot):
    err = LHPError(
        category=ErrorCategory.IO,
        code_number="007",
        title="File already exists",
        details="lhp.yaml already exists in the target directory.",
        context={"path": "/tmp/example/lhp.yaml"},
        suggestions=["Choose a different project name or remove the existing file"],
    )
    assert _render(err) == snapshot

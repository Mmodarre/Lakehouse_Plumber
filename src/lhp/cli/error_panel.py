"""Rich Panel rendering for LHPError.

This module is the **single** place that renders ``LHPError`` to Rich.
``cli.error_boundary`` and ``cli.commands.validate_command`` call
``render_error_panel(error)`` and pass the result to a Rich Console.

Why not on ``LHPError`` itself? See TARGET §6: the errors package
``lhp.errors`` must not depend on Rich, so the visual presentation lives
on the CLI side of the boundary.
"""

from __future__ import annotations

from typing import Any, Dict

from rich.panel import Panel
from rich.text import Text

from ..errors import ErrorCategory, LHPError


def _template_data(error: LHPError) -> Dict[str, Any]:
    return {
        "code": error.code,
        "category_label": error._category_label(),
        "title": error.title,
        "details": error.details,
        "context": dict(error.context) if error.context else {},
        "suggestions": list(error.suggestions or []),
        "example": error.example or None,
        "doc_link": error.doc_link or None,
    }


def _border_style(category: ErrorCategory) -> str:
    return {
        ErrorCategory.VALIDATION: "yellow",
        ErrorCategory.CONFIG: "red",
        ErrorCategory.IO: "red",
        ErrorCategory.DEPENDENCY: "red",
        ErrorCategory.ACTION: "red",
        ErrorCategory.CLOUDFILES: "red",
        ErrorCategory.GENERAL: "red",
    }.get(category, "red")


def render_error_panel(error: LHPError) -> Panel:
    """Render an LHPError as a Rich Panel for stderr console output.

    Panel structure is pinned by the snapshot test in ``tests/test_lhperror_rendering.py``.
    """
    d = _template_data(error)
    body = Text()
    body.append(d["title"] + "\n\n", style="bold")
    if d["details"]:
        body.append(d["details"] + "\n")
    if d["context"]:
        body.append("\n")
        body.append("Context\n", style="bold dim")
        for key, value in d["context"].items():
            body.append(f"  {key}: {value}\n", style="dim")
    if d["suggestions"]:
        body.append("\n")
        body.append("Suggestions\n", style="bold dim")
        for suggestion in d["suggestions"]:
            body.append("  -> ")
            body.append(f"{suggestion}\n")
    if d["example"]:
        body.append("\n")
        body.append("Example\n", style="bold dim")
        for line in d["example"].strip().split("\n"):
            body.append(f"  {line}\n")
    if d["doc_link"]:
        body.append("\n")
        body.append(f"More info: {d['doc_link']}", style="dim underline")
    return Panel(
        body,
        title=f"{d['code']}   {d['category_label']}",
        border_style=_border_style(error.category),
        title_align="left",
        padding=(1, 2),
    )

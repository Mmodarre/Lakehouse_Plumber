"""Rich Panel rendering for LHPError and the DTO projection of it.

This module is the **single** place that renders LHP error payloads to
Rich. ``cli.error_boundary`` and ``cli.commands.validate_command`` call
``render_error_panel(error)`` and pass the result to a Rich Console;
the event-stream summary presenter (``cli/presenters/summary_presenter.py``)
calls ``render_issue_panel(issue)`` with a frozen ``ValidationIssueView``
DTO, so it never has to import ``lhp.errors`` (sole-bridge invariant,
constitution §5.2 / §9.5).

Why not on ``LHPError`` itself? See TARGET §6: the errors package
``lhp.errors`` must not depend on Rich, so the visual presentation lives
on the CLI side of the boundary.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional

from rich.panel import Panel
from rich.text import Text

from ..errors import ErrorCategory, LHPError

# Category value (e.g. ``"VAL"``) -> human-facing panel label. Keyed by the
# ``ErrorCategory`` *value* string so the DTO path (which carries the
# category as a plain ``str``) and the live-exception path share one source
# of truth.
_CATEGORY_LABELS: Dict[str, str] = {
    ErrorCategory.VALIDATION.value: "Validation Error",
    ErrorCategory.CONFIG.value: "Configuration Error",
    ErrorCategory.IO.value: "I/O Error",
    ErrorCategory.DEPENDENCY.value: "Dependency Error",
    ErrorCategory.ACTION.value: "Action Error",
    ErrorCategory.CLOUDFILES.value: "CloudFiles Error",
    ErrorCategory.GENERAL.value: "Error",
}

# Category value -> Rich border style. VALIDATION is yellow (advisory);
# everything else is red (hard failure).
_CATEGORY_BORDERS: Dict[str, str] = {
    ErrorCategory.VALIDATION.value: "yellow",
}


def _category_label(category_value: str) -> str:
    return _CATEGORY_LABELS.get(category_value, "Error")


def _category_border(category_value: str) -> str:
    return _CATEGORY_BORDERS.get(category_value, "red")


def _build_panel(
    *,
    code: str,
    category_value: str,
    title: str,
    details: Optional[str],
    context: Mapping[str, Any],
    suggestions: List[str],
    example: Optional[str],
    doc_link: Optional[str],
) -> Panel:
    """Assemble the shared error-panel body from primitive fields.

    The single ``Text``-building routine behind both
    :func:`render_error_panel` (live :class:`LHPError`) and
    :func:`render_issue_panel` (:class:`ValidationIssueView` DTO). Panel
    structure is pinned by the snapshot test in
    ``tests/test_lhperror_rendering.py``.
    """
    body = Text()
    body.append(title + "\n\n", style="bold")
    if details:
        body.append(details + "\n")
    if context:
        body.append("\n")
        body.append("Context\n", style="bold dim")
        for key, value in context.items():
            body.append(f"  {key}: {value}\n", style="dim")
    if suggestions:
        body.append("\n")
        body.append("Suggestions\n", style="bold dim")
        for suggestion in suggestions:
            body.append("  -> ")
            body.append(f"{suggestion}\n")
    if example:
        body.append("\n")
        body.append("Example\n", style="bold dim")
        for line in example.strip().split("\n"):
            body.append(f"  {line}\n")
    if doc_link:
        body.append("\n")
        body.append(f"More info: {doc_link}", style="dim underline")
    return Panel(
        body,
        title=f"{code}   {_category_label(category_value)}",
        border_style=_category_border(category_value),
        title_align="left",
        padding=(1, 2),
    )


def render_error_panel(error: LHPError) -> Panel:
    """Render an LHPError as a Rich Panel for stderr console output.

    Panel structure is pinned by the snapshot test in ``tests/test_lhperror_rendering.py``.
    """
    return _build_panel(
        code=error.code,
        category_value=error.category.value,
        title=error.title,
        details=error.details,
        context=dict(error.context) if error.context else {},
        suggestions=list(error.suggestions or []),
        example=error.example or None,
        doc_link=error.doc_link or None,
    )


def render_issue_panel(issue: Any) -> Panel:
    """Render a ``ValidationIssueView``-shaped DTO as a Rich Panel.

    Duck-typed: ``issue`` only has to expose the flat fields a frozen
    :class:`lhp.api.views.ValidationIssueView` carries — ``code``,
    ``category``, ``title``, ``details``, ``context``, ``suggestions``,
    ``doc_link`` (``ValidationIssueView`` has no ``example`` field, so it
    is always omitted). Lets the summary presenter render a full
    error-style panel per validation issue without importing
    ``lhp.errors`` or reconstructing an :class:`LHPError` (§9.5).
    """
    return _build_panel(
        code=getattr(issue, "code", "") or "",
        category_value=getattr(issue, "category", "") or "",
        title=getattr(issue, "title", "") or "",
        details=getattr(issue, "details", None),
        context=dict(getattr(issue, "context", {}) or {}),
        suggestions=list(getattr(issue, "suggestions", ()) or []),
        example=getattr(issue, "example", None),
        doc_link=getattr(issue, "doc_link", None),
    )

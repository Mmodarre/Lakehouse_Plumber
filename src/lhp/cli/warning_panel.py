"""Rich Panel renderer for the Rich-free :class:`WarningCollector`.

This module is the CLI-side counterpart to the data class at
:mod:`lhp.api.callbacks`. The split — DTO in ``lhp.api``, Rich rendering
in ``lhp.cli`` — exists because constitution §9.6 forbids any ``rich.*``
import outside ``lhp.cli``, and TARGET §6 places callback DTOs in
``lhp.api`` so domain code can inject them without reaching into the CLI.

Callers use the function as::

    panel = render_warning_panel(wc)
    if panel is not None:
        console.print(panel)

which preserves the no-op-on-empty behavior of the old in-class
``render(console)`` method without forcing the caller to special-case
the empty collector.
"""
from __future__ import annotations

from rich.panel import Panel
from rich.text import Text

from lhp.api import WarningCollector


def render_warning_panel(collector: WarningCollector) -> Panel | None:
    """Build the end-of-run Rich Panel for a populated ``WarningCollector``.

    Returns ``None`` when the collector is empty, so the caller can skip
    the ``console.print`` call entirely. The title heuristic preserves
    the long-standing CLI behavior: a single deprecation entry yields
    ``"⚠ Deprecation Warning"``; anything else yields ``"⚠ Warnings"``.
    """
    entries = collector.as_list()
    if not entries:
        return None
    body = Text()
    for i, (category, message) in enumerate(entries):
        if i > 0:
            body.append("\n\n")
        body.append(f"[{category}] ", style="bold yellow")
        body.append(message)
    is_single_deprecation = len(entries) == 1 and entries[0][0] == "deprecation"
    return Panel(
        body,
        title="⚠ Deprecation Warning" if is_single_deprecation else "⚠ Warnings",
        border_style="yellow",
        title_align="left",
        padding=(1, 2),
    )

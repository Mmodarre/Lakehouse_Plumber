"""Rich Panel renderer for the Rich-free :class:`WarningCollector`.

CLI-side counterpart to :mod:`lhp.api.callbacks`: constitution §9.6
forbids any ``rich.*`` import outside ``lhp.cli``.
"""

from __future__ import annotations

from rich.panel import Panel
from rich.text import Text

from lhp.api import WarningCollector


def render_warning_panel(collector: WarningCollector) -> Panel | None:
    """Build the end-of-run Rich Panel, or ``None`` if collector is empty.

    Returning ``None`` lets the caller skip ``console.print`` without
    special-casing the empty collector.
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

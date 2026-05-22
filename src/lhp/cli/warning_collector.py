"""Per-run accumulator for non-fatal warnings rendered as a Rich Panel.

The collector deduplicates warnings by ``(category, message)``. Both
``lhp generate`` and ``lhp validate`` instantiate one per run, pass it
through to the orchestrator/helpers that detect warnings (currently
only the deprecated bare-``{token}`` substitution syntax), and render
a single yellow-bordered panel at end-of-run.

This replaces the previous global ``_DEPRECATED_BARE_TOKEN_WARNED``
module-level flag in :mod:`lhp.utils.substitution`: per-instance
state with explicit ownership beats process-wide mutation, and the
collector's dedup makes the bool flag sufficient (the panel only
needs to know whether to surface the warning, not how many times the
bare token was seen).
"""

from dataclasses import dataclass, field
from typing import List, Set, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.text import Text


@dataclass
class WarningCollector:
    """Per-run accumulator for non-fatal warnings.

    Deduplicates by ``(category, message)``. Renders a single Rich Panel
    with yellow border containing all unique warnings at end-of-run.
    """

    _warnings: List[Tuple[str, str]] = field(default_factory=list)
    _seen: Set[Tuple[str, str]] = field(default_factory=set)

    def add(self, category: str, message: str) -> None:
        """Record a warning, dedup'd by ``(category, message)``."""
        key = (category, message)
        if key in self._seen:
            return
        self._seen.add(key)
        self._warnings.append(key)

    @property
    def count(self) -> int:
        """Number of unique warnings collected so far."""
        return len(self._warnings)

    def render(self, console: Console) -> None:
        """Render all collected warnings as a single Rich Panel.

        No-op when nothing has been collected. The title shifts to
        ``Deprecation Warning`` when the sole entry is a deprecation,
        which is the common case today; multi-warning runs use the
        generic ``Warnings`` title.
        """
        if not self._warnings:
            return
        body = Text()
        for i, (category, message) in enumerate(self._warnings):
            if i > 0:
                body.append("\n\n")
            body.append(f"[{category}] ", style="bold yellow")
            body.append(message)
        is_single_deprecation = (
            len(self._warnings) == 1 and self._warnings[0][0] == "deprecation"
        )
        console.print(
            Panel(
                body,
                title=(
                    "⚠ Deprecation Warning" if is_single_deprecation else "⚠ Warnings"
                ),
                border_style="yellow",
                title_align="left",
                padding=(1, 2),
            )
        )

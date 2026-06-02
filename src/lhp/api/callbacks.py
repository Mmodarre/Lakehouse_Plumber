"""Rich-free public DTOs for per-run callback state.

Imports nothing from ``rich``: constitution §9.6 forbids Rich imports
outside ``lhp.cli``. Rendering lives in :mod:`lhp.cli.warning_panel`.

:stability: provisional
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set, Tuple

from lhp.core._interfaces import BaseWarningCollector


@dataclass
class WarningCollector(BaseWarningCollector):
    """Per-run accumulator for non-fatal warnings, deduplicated by key.

    Dedup is by ``(category, message)`` so helpers can call :meth:`add`
    defensively. Picklable: state is held in stdlib containers only.

    :stability: provisional
    """

    _warnings: List[Tuple[str, str]] = field(default_factory=list)
    _seen: Set[Tuple[str, str]] = field(default_factory=set)

    def add(self, category: str, message: str) -> None:
        key = (category, message)
        if key in self._seen:
            return
        self._seen.add(key)
        self._warnings.append(key)

    @property
    def count(self) -> int:
        return len(self._warnings)

    def as_list(self) -> tuple[tuple[str, str], ...]:
        """Frozen view — tuple, not list, so renderers cannot mutate state."""
        return tuple(self._warnings)

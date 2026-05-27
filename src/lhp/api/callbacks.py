"""Rich-free public DTOs for per-run callback state.

This module carries the data-side of the warning-collection contract:
a mutable, dedup'd accumulator that domain code (``lhp.core.coordination``,
``lhp.api.facade``) writes into. Rendering belongs to the CLI layer
(see :mod:`lhp.cli.warning_panel` for the Rich Panel renderer), so this
module imports nothing from ``rich`` — that is the whole point of the
split, and it is enforced by constitution §9.6 (no Rich library imports
outside ``lhp.cli``). Consumers that want a frozen view of the collected
warnings call :meth:`WarningCollector.as_list`.

:stability: provisional
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Set, Tuple


@dataclass
class WarningCollector:
    """Per-run accumulator for non-fatal warnings, deduplicated by key.

    The collector is the shared state between domain code (which records
    warnings via :meth:`add`) and the CLI renderer (which reads via
    :meth:`as_list` / :attr:`count`). One instance is created per
    ``lhp generate`` / ``lhp validate`` invocation and injected into the
    orchestrator, facade, and any helper that may produce a warning.

    Deduplication is by ``(category, message)`` so that helpers can call
    :meth:`add` defensively without surfacing duplicate panel entries
    for the same warning fired from multiple call sites. Instances are
    picklable: state is held in stdlib containers only.

    Rendering is NOT a responsibility of this class. The Rich Panel
    builder is :func:`lhp.cli.warning_panel.render_warning_panel`, which
    consumes ``as_list()`` and ``count``.

    :stability: provisional
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

    def as_list(self) -> tuple[tuple[str, str], ...]:
        """Return a frozen view of collected warnings for renderers.

        Returns a tuple-of-tuples (not a list) so that renderers cannot
        mutate the collector's internal state through the returned view.
        Order matches insertion order of unique entries.
        """
        return tuple(self._warnings)

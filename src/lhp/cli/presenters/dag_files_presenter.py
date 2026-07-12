"""Written-file listing for the ``dag`` command.

Renders the file paths produced by
:meth:`lhp.api.DependencyFacade.save_dependency_outputs` (a frozen
:class:`lhp.api.DependencyOutputsResult`). The command passes its **stdout**
console here so the paths are the pipeable primary data stream
(``lhp dag --format json | xargs cat``); the analysis view goes to stderr via
:mod:`dag_presenter`.

Sole-bridge invariant (constitution §5.2 / §9.5): this module renders rich
but MUST NOT import ``lhp.errors`` — it only formats a frozen DTO.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ._layout import ColumnSpec, render_empty_state, render_listing_table

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import DependencyOutputsResult

logger = logging.getLogger(__name__)


def render_outputs(
    result: "DependencyOutputsResult",
    *,
    console: "Console",
) -> None:
    """Render the generated-file paths in ``result`` to ``console`` (stdout).

    One row per entry: the format name, an optional multi-job label
    (``_master`` for the master orchestration file, empty for single-file
    outputs), and the absolute path. Renders an empty-state notice when no
    files were written.
    """
    logger.debug("Rendering %d dependency output file(s)", len(result.entries))

    if not result.entries:
        render_empty_state(
            "No dependency files were written.",
            "Pass --format with one of dot, json, text, job (or all).",
            sink=console,
        )
        return

    rows = [
        (entry.format_name.upper(), entry.label or "-", str(entry.path))
        for entry in result.entries
    ]
    render_listing_table(
        "Generated files",
        [
            ColumnSpec("Format", style="bold"),
            ColumnSpec("Label", style="dim"),
            ColumnSpec("Path"),
        ],
        rows,
        sink=console,
    )

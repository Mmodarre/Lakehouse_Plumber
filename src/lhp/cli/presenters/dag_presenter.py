"""Dependency-analysis view for the ``dag`` command.

Renders the in-memory :class:`lhp.api.DependencyAnalysisResult` — execution
stages (parallel groups), the per-pipeline dependency adjacency, cycle status,
and external sources — to the supplied console. The command passes its
**stderr** console here: this is diagnostic/status output, not the primary
data stream (file paths go to stdout via :mod:`dag_files_presenter`).

Sole-bridge invariant (constitution §5.2 / §9.5): this module renders rich
but MUST NOT import ``lhp.errors`` — it only formats a frozen DTO.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from rich.text import Text

from ._layout import ColumnSpec, render_listing_table

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import DependencyAnalysisResult

logger = logging.getLogger(__name__)


def render_analysis(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """Render the dependency-analysis summary for ``result`` to ``console``.

    Sections render in this order: a counts header, the execution-stage
    table (one row per pipeline; parallel stages are flagged), the dependency
    adjacency, cycle status, external sources, and extraction warnings. Each
    section is suppressed when it carries no data so a clean acyclic graph
    stays terse.
    """
    logger.debug(
        "Rendering dependency analysis: %d pipelines, %d stages, cycles=%s",
        result.total_pipelines,
        len(result.execution_stages),
        result.has_cycles,
    )

    console.print(Text("Dependency analysis", style="bold dim"))
    console.print(f"  Pipelines analyzed: {result.total_pipelines}")
    console.print(f"  Execution stages: {len(result.execution_stages)}")
    console.print(f"  External sources: {result.total_external_sources}")

    _render_execution_stages(result, console=console)
    _render_dependencies(result, console=console)
    _render_cycles(result, console=console)
    _render_external_sources(result, console=console)
    _render_warnings(result, console=console)


def _render_execution_stages(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """Render the stage-ordered execution table; parallel stages are flagged."""
    if not result.execution_stages:
        console.print(Text("No execution order could be determined.", style="yellow"))
        return

    rows: list[tuple[str, str, str]] = []
    for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
        parallel = "parallel" if len(stage_pipelines) > 1 else ""
        for name in stage_pipelines:
            rows.append((str(stage_idx), name, parallel))

    render_listing_table(
        "Execution order",
        [
            ColumnSpec("Stage", justify="right"),
            ColumnSpec("Pipeline"),
            ColumnSpec("Notes", style="dim"),
        ],
        rows,
        sink=console,
    )


def _render_dependencies(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """Render the per-pipeline dependency adjacency (``pipeline -> deps``)."""
    if not result.pipeline_dependencies:
        return

    rows = [
        (pipeline, ", ".join(deps) if deps else "-")
        for pipeline, deps in sorted(result.pipeline_dependencies.items())
    ]
    render_listing_table(
        "Dependencies",
        [
            ColumnSpec("Pipeline", style="bold"),
            ColumnSpec("Depends on", style="dim"),
        ],
        rows,
        sink=console,
    )


def _render_cycles(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """Render circular-dependency cycles when present (silent otherwise)."""
    if not result.circular_dependencies:
        return

    console.print(
        Text.assemble(
            ("Circular dependencies: ", "bold yellow"),
            str(len(result.circular_dependencies)),
        )
    )
    for cycle in result.circular_dependencies:
        console.print(f"  {' -> '.join(cycle)}")
    console.print(Text("  These must be resolved before execution.", style="yellow"))


def _render_external_sources(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """List external sources (up to five inline) when any were detected."""
    if not result.external_sources:
        return

    console.print(Text("External sources", style="bold dim"))
    if len(result.external_sources) <= 5:
        for source in result.external_sources:
            console.print(f"  {source}")
    else:
        console.print(
            f"  {result.total_external_sources} sources "
            "(see generated files for the full list)."
        )


_MAX_WARNING_LINES = 10


def _render_warnings(
    result: "DependencyAnalysisResult",
    *,
    console: "Console",
) -> None:
    """Render extraction warnings when present (silent otherwise).

    Shows a count header, up to :data:`_MAX_WARNING_LINES` detail lines
    (``code flowgroup.action (file:line): message`` — the location part is
    omitted or shortened when the warning carries no file/line), an overflow
    line pointing at the JSON output, and one trailing ``depends_on`` hint.
    """
    if not result.warnings:
        return

    console.print(
        Text(
            f"{len(result.warnings)} dependency extraction warning(s):",
            style="bold yellow",
        )
    )
    for warning in result.warnings[:_MAX_WARNING_LINES]:
        location = ""
        if warning.file_path and warning.line is not None:
            location = f" ({warning.file_path}:{warning.line})"
        elif warning.file_path:
            location = f" ({warning.file_path})"
        console.print(
            Text(
                f"  {warning.code} {warning.flowgroup}.{warning.action}"
                f"{location}: {warning.message}"
            )
        )
    overflow = len(result.warnings) - _MAX_WARNING_LINES
    if overflow > 0:
        console.print(Text(f"  ... and {overflow} more (see JSON output)", style="dim"))
    console.print(
        Text(
            "  Declare explicit 'depends_on' for reads LHP cannot resolve.",
            style="yellow",
        )
    )

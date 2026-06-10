"""Rendering for the ``inspect-wheel`` command family.

Renders the two frozen wheel DTOs produced by the inspection facade:

- :class:`lhp.api.WheelContentsView` — the modules packaged inside a built
  wheel (one table row per ``.py`` module: archive name + uncompressed size).
- :class:`lhp.api.WheelExtractionResult` — the files written to disk by a
  wheel-extraction run (the output dir + the list of written paths).

Sole-bridge invariant (constitution §5.2 / §9.5): this module is the only
bridge between the public wheel DTOs and ``rich`` rendering. It MUST NOT import
the error package or any domain module (core / bundle) — it only formats frozen
DTOs through the shared ``_layout`` primitives. The DTO type imports come from
the public api surface alone.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from ._layout import ColumnSpec, render_empty_state, render_listing_table

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import WheelContentsView, WheelExtractionResult

logger = logging.getLogger(__name__)


def render_inspection(
    result: "WheelContentsView",
    *,
    console: "Console",
) -> None:
    """Render the modules packaged inside ``result``'s wheel to ``console``.

    One row per module: the archive name and its uncompressed size in bytes.
    The table title identifies the wheel (and its ``pipeline`` / ``env``
    context when present) so the heading survives non-TTY capture. Renders a
    friendly empty-state notice — rather than a blank table — when the wheel
    contains no ``.py`` modules.
    """
    logger.debug(
        "Rendering %d wheel module(s) for %s",
        result.module_count,
        result.wheel_path,
    )

    if not result.modules:
        render_empty_state(
            f"No .py modules found in {result.wheel_path.name}.",
            "The wheel packages no Python modules under its top-level package.",
            sink=console,
        )
        return

    rows = [(module.arcname, str(module.size_bytes)) for module in result.modules]
    render_listing_table(
        _inspection_title(result),
        [
            ColumnSpec("Module", style="bold"),
            ColumnSpec("Size (bytes)", justify="right"),
        ],
        rows,
        total_label="modules",
        sink=console,
    )


def render_extraction(
    result: "WheelExtractionResult",
    *,
    console: "Console",
) -> None:
    """Render the files extracted from ``result``'s wheel to ``console``.

    One row per written path under ``result.output_dir``. The table title
    names the source wheel and the output directory; the footer reports the
    total file count. Renders an empty-state notice when no files were
    written.
    """
    logger.debug(
        "Rendering %d extracted file(s) from %s into %s",
        result.written_count,
        result.wheel_path,
        result.output_dir,
    )

    if not result.written_paths:
        render_empty_state(
            f"No files extracted from {result.wheel_path.name}.",
            f"Nothing was written under {result.output_dir}.",
            sink=console,
        )
        return

    rows = [(str(path),) for path in result.written_paths]
    render_listing_table(
        f"Extracted {result.wheel_path.name} -> {result.output_dir}",
        [ColumnSpec("Written path")],
        rows,
        total_label="files",
        sink=console,
    )


def _inspection_title(result: "WheelContentsView") -> str:
    """Build the inspection table title, appending pipeline / env if present."""
    title = f"Modules in {result.wheel_path.name}"
    context = [
        f"{label}={value}"
        for label, value in (("pipeline", result.pipeline), ("env", result.env))
        if value
    ]
    if context:
        title += f" ({', '.join(context)})"
    return title

"""Shared static-command rendering helpers for the LHP CLI.

Helpers branch internally on ``sink.is_terminal``; callers never check TTY
state or terminal width. Terminal-mode styling follows STYLE.md section 6
(``border_style="dim"``, ``header_style="bold dim"``). Non-TTY rendering is
plain text — tab-separated rows, no ANSI, no box-drawing.

The ``sink`` default is resolved lazily at call time so test monkey-patching
of ``lhp.cli.console.console`` is honored.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional, Sequence

from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from lhp.cli.console import console as _default_console

# Import (don't re-declare) so the launcher banner, the Live frame, and this
# header share artwork and color.
from lhp.cli.live_panel import (
    _BRAND_ORANGE,
    _LHP_WORDMARK,
    _WORDMARK_COL_WIDTH,
    _WORDMARK_MIN_PANEL_WIDTH,
)


@dataclass(frozen=True)
class ColumnSpec:
    """Declarative column metadata for :func:`render_listing_table`.

    Fields map 1:1 to ``rich.table.Table.add_column`` kwargs. ``style``
    applies to the column body only; header style is fixed by STYLE.md
    on the ``Table`` itself.
    """

    header: str
    style: Optional[str] = None
    justify: str = "left"


def render_listing_table(
    title: str,
    columns: Sequence[ColumnSpec],
    rows: Iterable[Sequence[str]],
    *,
    total_label: Optional[str] = None,
    sink: Optional[Console] = None,
) -> None:
    """Render a titled table of rows, with an optional total footer.

    Non-TTY mode writes tab-separated rows directly to ``sink.file`` —
    ``Console.print`` would expand embedded ``\\t`` into spaces, breaking
    downstream consumers that split on tab. Tests that capture output via
    ``Console(file=io.StringIO(), force_terminal=False)`` see the bytes
    verbatim; ``record=True`` does NOT capture raw ``file.write`` calls.
    """
    sink = sink or _default_console
    materialized_rows = [list(row) for row in rows]

    if sink.is_terminal:
        table = Table(
            title=title,
            border_style="dim",
            header_style="bold dim",
        )
        for spec in columns:
            table.add_column(
                spec.header,
                style=spec.style,
                justify=spec.justify,  # type: ignore[arg-type]
            )
        for row in materialized_rows:
            table.add_row(*(str(cell) for cell in row))
        sink.print(table)
        if total_label is not None:
            sink.print(f"Total {total_label}: {len(materialized_rows)}")
        return

    lines: list[str] = [title, "\t".join(spec.header for spec in columns)]
    for row in materialized_rows:
        lines.append("\t".join(str(cell) for cell in row))
    if total_label is not None:
        lines.append(f"Total {total_label}: {len(materialized_rows)}")
    sink.file.write("\n".join(lines) + "\n")


def render_empty_state(
    title: str,
    hint: str,
    *,
    sink: Optional[Console] = None,
) -> None:
    """Render a two-part empty-state notice (title plus hint).

    Non-TTY mode writes directly to ``sink.file`` (see
    :func:`render_listing_table` for the reasoning).
    """
    sink = sink or _default_console

    if sink.is_terminal:
        body = Group(
            Text(title),
            Text(hint, style="dim"),
        )
        sink.print(Panel(body, border_style="dim"))
        return

    sink.file.write(f"{title}\n{hint}\n")


def render_command_header(
    command_name: str,
    *,
    subtitle: Optional[str] = None,
    sink: Optional[Console] = None,
) -> None:
    """Render the top-of-command banner (command name + LHP wordmark).

    Silent no-op on non-TTY sinks or terminals narrower than
    ``_WORDMARK_MIN_PANEL_WIDTH`` (per STYLE.md section 7).
    """
    sink = sink or _default_console

    if not sink.is_terminal:
        return
    if sink.width < _WORDMARK_MIN_PANEL_WIDTH:
        return

    left: object = Text(command_name, style="bold dim")
    if subtitle:
        left = Group(left, Text(subtitle, style="dim"))

    # Without the explicit right-column width, Rich's column solver
    # squeezes the multi-line wordmark and the lines wrap onto themselves.
    grid = Table.grid(expand=True, padding=(0, 1))
    grid.add_column(ratio=1)
    grid.add_column(width=_WORDMARK_COL_WIDTH)
    grid.add_row(left, Text(_LHP_WORDMARK, style=f"bold {_BRAND_ORANGE}"))

    sink.print(Panel(grid, border_style="dim"))

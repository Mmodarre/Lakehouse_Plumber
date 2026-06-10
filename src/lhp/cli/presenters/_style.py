"""Shared Rich style primitives for the LHP CLI presenter layer.

Brand color, the LHP wordmark, and the small style-table helpers that the
launcher banner, the Live frame, and the static command headers all share.
"""

from typing import List, Tuple

from rich.table import Table

_BRAND_ORANGE = "#E07A2C"

_LHP_WORDMARK = """\
██╗     ██╗  ██╗██████╗
██║     ██║  ██║██╔══██╗
██║     ███████║██████╔╝
██║     ██╔══██║██╔═══╝
███████╗██║  ██║██║
╚══════╝╚═╝  ╚═╝╚═╝     """

# Widest wordmark row + 1-char margin each side; fixes the right column
# width so the wordmark cannot wrap.
_WORDMARK_COL_WIDTH = 26

# Wordmark column + 60-char minimum left column + ~4 chars of panel
# border/grid padding. Below this, render the single-column fallback.
_WORDMARK_MIN_PANEL_WIDTH = _WORDMARK_COL_WIDTH + 60 + 4


def make_summary_table(title: str) -> Table:
    """Returns a Rich ``Table`` with status-icon and Pipeline columns; callers append their own data columns."""
    table = Table(
        title=title,
        title_style="bold",
        show_header=True,
        header_style="bold dim",
        border_style="dim",
    )
    table.add_column("", width=2)
    table.add_column("Pipeline")
    return table


def warning_suffix_parts(warning_count: int) -> List[Tuple[str, str]]:
    """Return Rich ``Text.assemble`` parts for the warning-count suffix.

    Empty list when ``warning_count`` is zero so callers can splat
    unconditionally.
    """
    if warning_count == 0:
        return []
    noun = "warning" if warning_count == 1 else "warnings"
    return [
        ("; ", "default"),
        (f"{warning_count} {noun}", "bold yellow"),
    ]

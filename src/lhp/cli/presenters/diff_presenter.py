"""Plan-vs-disk diff block for the ``lhp generate --dry-run`` command.

Renders the change set a generation run *would* apply, computed as a pure
dict / set comparison between the planned files and what is currently on
disk. Three change classes, each on its own ``~`` / ``+`` / ``-`` line:

- ``~ modified``  — present in both, planned content differs from on-disk.
- ``+ would-create`` — planned only; no file exists at that path yet.
- ``- orphan`` — on disk only; the plan no longer emits it.

With ``show_details`` each modified file expands to a ``difflib`` unified
diff (on-disk as the *before*, planned as the *after*).

This is a leaf presenter (constitution §6.3 / §9.11): it owns no facade,
no project root, and no path / format / domain logic. The caller folds the
facade's :class:`lhp.api.GenerationPlan` and the on-disk snapshot, hands
both in, and this module does nothing but compare and render. It MUST NOT
import ``lhp.errors`` — diff rendering carries no domain-error formatting.
"""

from __future__ import annotations

import difflib
import logging
from typing import TYPE_CHECKING, Dict, List, Mapping, Tuple

from rich.text import Text

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import GenerationPlan

logger = logging.getLogger(__name__)

_MODIFIED_STYLE = "yellow"
_CREATE_STYLE = "green"
_ORPHAN_STYLE = "red"


def _planned_content_by_path(plan: "GenerationPlan") -> Dict[str, str]:
    """Fold ``plan.files`` (a tuple of views) into a ``{path: content}`` map.

    ``plan.files`` is a ``Tuple[PlannedFileView, ...]`` rather than a mapping
    so the public contract never depends on a ``Path``-keyed dict (§4.8); the
    diff needs the mapping shape, so we build it here from ``str(pf.path)``.
    """
    return {str(planned.path): planned.content for planned in plan.files}


def _classify(
    planned: Mapping[str, str], on_disk: Mapping[str, str]
) -> Tuple[List[str], List[str], List[str]]:
    """Partition paths into (modified, would-create, orphan), each sorted.

    Pure set / dict comparison — no filesystem access, no path normalisation.
    ``modified`` is the intersection where content differs; ``would_create``
    is planned-only; ``orphan`` is disk-only.
    """
    planned_paths = set(planned)
    disk_paths = set(on_disk)

    modified = sorted(
        path for path in planned_paths & disk_paths if planned[path] != on_disk[path]
    )
    would_create = sorted(planned_paths - disk_paths)
    orphan = sorted(disk_paths - planned_paths)
    return modified, would_create, orphan


def _change_line(marker: str, path: str, label: str, style: str) -> Text:
    """One ``<marker> <label>  <path>`` row, e.g. ``~ modified  out/a.py``."""
    text = Text()
    text.append(f"{marker} ", style=f"bold {style}")
    text.append(label, style=style)
    text.append("  ")
    text.append(path, style="dim")
    return text


def _unified_diff(path: str, before: str, after: str) -> Text:
    """A coloured ``difflib`` unified diff for one modified file.

    ``before`` is the on-disk content, ``after`` the planned content, so a
    removed line (``-``) is what disk has today and an added line (``+``) is
    what the run would write.
    """
    text = Text()
    diff_lines = difflib.unified_diff(
        before.splitlines(),
        after.splitlines(),
        fromfile=f"{path} (on disk)",
        tofile=f"{path} (planned)",
        lineterm="",
    )
    for line in diff_lines:
        if line.startswith("+") and not line.startswith("+++"):
            text.append(line + "\n", style=_CREATE_STYLE)
        elif line.startswith("-") and not line.startswith("---"):
            text.append(line + "\n", style=_ORPHAN_STYLE)
        elif line.startswith("@@"):
            text.append(line + "\n", style="cyan")
        else:
            text.append(line + "\n", style="dim")
    return text


def render_diff(
    plan: "GenerationPlan",
    on_disk: Mapping[str, str],
    *,
    console: "Console",
    show_details: bool,
) -> bool:
    """Render the plan-vs-disk diff and report whether anything changed.

    Folds ``plan.files`` into a ``{path: content}`` map, compares it against
    ``on_disk`` (the same ``{path: content}`` shape, read by the caller), and
    prints one ``~`` / ``+`` / ``-`` line per changed path. When
    ``show_details`` is set, every modified file is followed by its unified
    diff. Returns ``True`` iff the diff is non-empty (any modified,
    would-create, or orphan path), ``False`` when plan and disk match exactly.
    """
    planned = _planned_content_by_path(plan)
    modified, would_create, orphan = _classify(planned, on_disk)

    if not (modified or would_create or orphan):
        console.print(Text("No changes — output is up to date.", style="dim"))
        return False

    for path in would_create:
        console.print(_change_line("+", path, "would-create", _CREATE_STYLE))
    for path in modified:
        console.print(_change_line("~", path, "modified", _MODIFIED_STYLE))
        if show_details:
            console.print(_unified_diff(path, on_disk[path], planned[path]))
    for path in orphan:
        console.print(_change_line("-", path, "orphan", _ORPHAN_STYLE))

    return True

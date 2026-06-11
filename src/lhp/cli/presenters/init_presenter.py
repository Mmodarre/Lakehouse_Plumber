"""Presenter for the ``lhp init`` success outcome.

Renders the post-scaffold report — the created directory tree, the example
files seeded into the project, and the copy-pasteable next-step commands
(``cd``, edit YAML, ``lhp validate``, ``lhp generate``, and — for a bundle
project — ``databricks bundle deploy``). TTY sinks get a Rich tree plus a
boxed next-steps panel; non-TTY sinks get plain newline-delimited lines so
the output stays grep-able and free of box-drawing.

CLI presenter layer (constitution §2.7): renders Rich from a frozen
:class:`lhp.api.InitProjectResult` DTO and MUST NOT import ``lhp.errors``
(sole-bridge invariant §5.2 / §9.5) — ``init`` surfaces its one domain
failure by raising ``LHPError`` from the command and letting the error
boundary render it.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, List

from rich.console import Group
from rich.panel import Panel
from rich.text import Text
from rich.tree import Tree

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api import InitProjectResult

logger = logging.getLogger(__name__)


def _relativize(path: Path, base: Path) -> str:
    """Format ``path`` relative to ``base`` with a fallback to absolute.

    The bootstrap returns absolute paths; rendering them relative to the
    target directory keeps the report readable regardless of where ``lhp
    init`` was invoked from.
    """
    try:
        return str(path.relative_to(base))
    except ValueError:
        return str(path)


def _next_step_commands(result: "InitProjectResult") -> List[str]:
    """The ordered next-step commands shown after a successful scaffold.

    A bundle project ends with ``databricks bundle deploy``; a plain
    project stops after ``lhp generate``. ``cd`` is only meaningful when
    the scaffold created a new directory rather than initializing the cwd.
    """
    target = result.target_dir
    steps: List[str] = []
    if target in result.created_dirs:
        steps.append(f"cd {target.name}")
    steps.extend(
        [
            "# Edit substitutions/<env>.yaml and add a pipeline under pipelines/",
            "lhp validate --env dev",
            "lhp generate --env dev",
        ]
    )
    if result.bundle_enabled:
        steps.append("databricks bundle deploy")
    return steps


def _relative_entries(result: "InitProjectResult") -> tuple[list[str], list[str]]:
    """Return (sorted dir names, sorted file names) relative to the target.

    The target directory itself is dropped from the directory list so the
    report shows only the scaffolded sub-structure, not the project root.
    """
    target = result.target_dir
    dir_names = sorted(
        _relativize(d, target) for d in result.created_dirs if d != target
    )
    file_names = sorted(_relativize(f, target) for f in result.created_files)
    return dir_names, file_names


def _render_plain(
    result: "InitProjectResult",
    dir_names: list[str],
    file_names: list[str],
    *,
    console: "Console",
) -> None:
    """Plain newline-delimited report for non-TTY sinks."""
    label = "Databricks Asset Bundle project" if result.bundle_enabled else "project"
    lines: List[str] = [f"Initialized LHP {label} at {result.target_dir}"]
    for d in dir_names:
        lines.append(f"  created dir:  {d}")
    for f in file_names:
        lines.append(f"  created file: {f}")
    if result.git_initialized:
        lines.append("Initialized git repository: .git/")
    lines.append("Next steps:")
    for cmd in _next_step_commands(result):
        lines.append(f"  {cmd}")
    console.file.write("\n".join(lines) + "\n")


def _render_rich(
    result: "InitProjectResult",
    dir_names: list[str],
    file_names: list[str],
    *,
    console: "Console",
) -> None:
    """Rich tree + next-steps panel for TTY sinks."""
    label = "Databricks Asset Bundle project" if result.bundle_enabled else "project"
    tree = Tree(
        Text.assemble(
            ("✓ ", "bold green"),
            (f"Initialized LHP {label}: ", "bold"),
            (result.target_dir.name, f"bold {'#E07A2C'}"),
        )
    )
    for d in dir_names:
        tree.add(Text(f"{d}/", style="dim"))
    for f in file_names:
        tree.add(Text(f))
    console.print(tree)
    if result.git_initialized:
        console.print(
            Text.assemble(("✓ ", "bold green"), ("Initialized git repository", "dim"))
        )

    steps = [Text("Next steps:", style="bold")]
    steps.extend(Text(f"  {cmd}") for cmd in _next_step_commands(result))
    console.print(Panel(Group(*steps), border_style="dim"))


def render_init_result(result: "InitProjectResult", *, console: "Console") -> None:
    """Render the outcome of a successful ``lhp init`` to ``console``.

    Branches on ``console.is_terminal``: a Rich tree plus a boxed
    next-steps panel for TTY sinks, plain newline-delimited lines
    otherwise. Called only on success — failures are raised as
    ``LHPError`` by the command and rendered by the error boundary.
    """
    dir_names, file_names = _relative_entries(result)
    logger.debug(
        f"Rendering init result: {len(dir_names)} dirs, {len(file_names)} files, "
        f"bundle={result.bundle_enabled}"
    )
    if console.is_terminal:
        _render_rich(result, dir_names, file_names, console=console)
    else:
        _render_plain(result, dir_names, file_names, console=console)

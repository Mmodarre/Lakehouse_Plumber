"""Lazy command loading for the ``lhp`` top-level group.

Importing every command module at startup pulls the whole facade/domain
import graph into ``lhp --version`` and ``lhp --help`` paths, which the
startup refactor (Task U2) eliminates. :class:`LazyGroup` defers each
command's module import until that command is actually resolved, while
keeping rich_click's grouped help panels intact.

Mechanics:

- A ``name -> "module:attr"`` map (``COMMAND_IMPORTS``) replaces the eager
  ``from .commands.X import Y`` block and the matching ``cli.add_command``
  calls in ``cli/main.py``.
- :meth:`LazyGroup.get_command` imports the module on demand, registers the
  resolved command into ``self.commands``, and then delegates to
  ``RichGroup.get_command`` so rich_click's alias mapping still runs.
- :meth:`LazyGroup.list_commands` merges the lazy names with any eagerly
  registered ones, sorted (identical ordering to ``click.Group``).

Panel/help-grouping is preserved because rich_click's help renderer builds
panels from the *resolved* command objects (it calls ``list_commands`` then
``get_command`` for each, reading ``panel``/``hidden`` off the real object).
``lhp --help`` therefore still imports every command module — help legitimately
needs each command's metadata — but ``--version`` and single-command dispatch
no longer pay for commands they never touch.
"""

from __future__ import annotations

import importlib
from typing import Any, Optional

import click
from rich_click import RichGroup

# command name (the click ``name=``) -> "<module path>:<command attribute>".
# Ordering here is documentary only; ``list_commands`` sorts for display, so
# the help listing is alphabetical exactly as the eager registry produced.
COMMAND_IMPORTS: dict[str, str] = {
    "generate": "lhp.cli.commands.generate_command:generate",
    "validate": "lhp.cli.commands.validate_command:validate_command",
    "dag": "lhp.cli.commands.dag_command:dag",
    "list": "lhp.cli.commands.list_command:list_group",
    "substitutions": "lhp.cli.commands.substitutions_command:substitutions_command",
    "diff": "lhp.cli.commands.diff_command:diff_command",
    "init": "lhp.cli.commands.init_command:init",
    "skill": "lhp.cli.commands.skill_command:skill",
    # Hidden backward-compatibility alias for the renamed ``dag`` command.
    "deps": "lhp.cli.commands.dag_command:deps",
}


class LazyGroup(RichGroup):
    """A :class:`rich_click.RichGroup` that imports command modules on demand.

    Subclasses ``RichGroup`` (not plain ``click.Group``) so the grouped
    command panels, styling, and alias handling that rich_click layers on
    top of Click are preserved.
    """

    def __init__(
        self,
        *args: Any,
        lazy_commands: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        # name -> "module:attr" for commands not yet imported. Resolved
        # entries are moved into ``self.commands`` (Click's eager registry)
        # by ``get_command`` and dropped from here so each module imports once.
        self._lazy_commands: dict[str, str] = dict(lazy_commands or {})

    def list_commands(self, ctx: click.Context) -> list[str]:
        """Return all command names — lazy and eager — sorted for display.

        Includes hidden commands (e.g. ``deps``); rich_click filters
        ``hidden`` at render time, matching ``click.Group`` behavior.
        """
        names = set(self.commands) | set(self._lazy_commands)
        return sorted(names)

    def get_command(self, ctx: click.Context, cmd_name: str) -> Optional[click.Command]:
        """Resolve ``cmd_name``, importing its module the first time.

        On first resolution the lazy entry is imported, registered into
        ``self.commands``, and removed from the lazy map. Resolution then
        delegates to ``RichGroup.get_command`` so rich_click's alias mapping
        is applied for both lazy and eager commands.
        """
        if cmd_name not in self.commands and cmd_name in self._lazy_commands:
            import_path = self._lazy_commands.pop(cmd_name)
            module_path, _, attr = import_path.partition(":")
            module = importlib.import_module(module_path)
            command = getattr(module, attr)
            self.add_command(command, cmd_name)
        return super().get_command(ctx, cmd_name)

"""LakehousePlumber CLI — main entry point and command registry.

Thin assembly layer (constitution §2.7 / §9.11): defines the top-level ``lhp``
group, wires verbosity/logging from the group options, and registers each
command lazily via ``LazyGroup`` (commands in ``cli/commands/`` import only on
first use). No business logic and no domain imports live here — only
``click``/``rich_click``, stdlib, and the CLI-internal logging/version helpers
and the lazy command map (§5).
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import click

from ._lazy_group import COMMAND_IMPORTS, LazyGroup
from ._version import get_version
from .logging_config import configure_logging

logger = logging.getLogger(__name__)


def _find_project_root() -> Optional[Path]:
    """Best-effort walk for the ``lhp.yaml`` project marker.

    Returns the directory containing ``lhp.yaml`` (walking up from the cwd), or
    ``None`` when not inside a project. Unlike the facade's
    ``resolve_project_root``, this never raises: the group callback runs for
    every invocation — including ``--version`` and ``-h`` outside a project —
    and must only locate a log directory, not assert project membership.
    """
    current = Path.cwd().resolve()
    for path in [current, *list(current.parents)]:
        if (path / "lhp.yaml").exists():
            return path
    return None


def _print_version(ctx: click.Context, _param: click.Parameter, value: bool) -> None:
    """Eager ``--version`` callback: print the version and exit 0.

    Runs before the group body so ``lhp --version`` works outside a project
    and without a subcommand.
    """
    if not value or ctx.resilient_parsing:
        return
    click.echo(get_version())
    ctx.exit(0)


@click.group(
    cls=LazyGroup,
    lazy_commands=COMMAND_IMPORTS,
    context_settings={"help_option_names": ["-h", "--help"]},
)
@click.option(
    "--version",
    is_flag=True,
    is_eager=True,
    expose_value=False,
    callback=_print_version,
    help="Show the LHP version and exit.",
)
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Increase console log verbosity (repeatable). Does not affect output.",
)
@click.option(
    "--log-file",
    is_flag=True,
    help="Write a detailed DEBUG log to .lhp/logs/lhp.log (off by default).",
)
@click.option(
    "--no-progress",
    is_flag=True,
    help="Disable live progress displays for commands that show one.",
)
@click.option("--perf", is_flag=True, hidden=True)
@click.pass_context
def cli(
    ctx: click.Context,
    verbose: int,
    log_file: bool,
    no_progress: bool,
    perf: bool,
) -> None:
    """LakehousePlumber — generate Lakeflow pipelines from YAML configs."""
    project_root = _find_project_root()
    log_file_path = configure_logging(bool(verbose), project_root, log_to_file=log_file)

    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["log_file"] = log_file_path
    ctx.obj["no_progress"] = no_progress
    ctx.obj["perf"] = perf

    if perf and project_root is not None:
        from ..utils.performance_timer import enable_perf_timing

        enable_perf_timing(project_root)


# Commands are registered lazily: ``COMMAND_IMPORTS`` (in ``_lazy_group``)
# maps each name to its module, and ``LazyGroup`` imports it on first use.


if __name__ == "__main__":
    cli()

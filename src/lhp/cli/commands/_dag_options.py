"""Shared option wiring for the ``dag`` command and its ``deps`` alias.

Kept in a dedicated module so the single option stack lives in one place: the
``deps`` alias forwards into ``dag`` via ``ctx.forward``, which requires both
commands to expose identical parameter names. This is presentation glue (Click
option declarations), not business logic (§9.11).
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import click

_VALID_FORMATS = ("dot", "json", "text", "job")


def parse_formats(ctx: click.Context, param: click.Parameter, value: str) -> list[str]:
    """Click callback for ``--format``: parse the comma-separated value.

    ``all`` expands to every concrete format; an unknown token raises
    ``click.BadParameter``, which Click surfaces as exit code 2 (ahead of the
    boundary-wrapped command body).
    """
    formats = [token.strip().lower() for token in value.split(",") if token.strip()]
    invalid = [fmt for fmt in formats if fmt not in (*_VALID_FORMATS, "all")]
    if invalid:
        raise click.BadParameter(
            f"invalid format(s): {', '.join(invalid)}. "
            f"choose from {', '.join((*_VALID_FORMATS, 'all'))}"
        )
    return list(_VALID_FORMATS) if "all" in formats else formats


def dag_options(func: Callable) -> Callable:
    """Apply the ``dag`` / ``deps`` option set (one stack, applied to both)."""
    stack = [
        click.option(
            "-p",
            "--pipeline",
            "pipeline",
            default=None,
            help="Restrict the analysis to one pipeline.",
        ),
        click.option("--blueprint", default=None),
        click.option(
            "--trust-depends-on",
            is_flag=True,
            help=(
                "Treat a non-empty depends_on as the action's authoritative "
                "source set: skip SQL/Python body extraction for it (fast "
                "path for fully-declared projects)."
            ),
        ),
        # Deprecated no-op: blueprints are always fully expanded during
        # analysis. Accepted-but-ignored so existing invocations keep
        # working; the command prints a deprecation notice when passed.
        click.option("--expand-blueprints", is_flag=True, hidden=True),
        click.option(
            "-jc",
            "--job-config",
            "job_config",
            default=None,
            type=click.Path(dir_okay=False, path_type=Path),
        ),
        click.option("-j", "--job-name", default=None),
        click.option("-b", "--bundle-output", is_flag=True),
        click.option(
            "-o",
            "--output",
            "output_dir",
            default=".lhp/dependencies/",
            type=click.Path(file_okay=False, path_type=Path),
        ),
        click.option(
            "--format", "output_format", default="all", callback=parse_formats
        ),
        click.option(
            "--no-cache",
            is_flag=True,
            help="Disable the persistent parse cache for this run.",
        ),
        click.option(
            "--max-workers",
            type=click.IntRange(min=1),
            default=None,
            help="Max worker processes (default ~80%% of CPUs; 1 = sequential).",
        ),
    ]
    for decorate in stack:
        func = decorate(func)
    return func

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
        click.option("--blueprint", default=None),
        click.option("--expand-blueprints", is_flag=True),
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
    ]
    for decorate in stack:
        func = decorate(func)
    return func

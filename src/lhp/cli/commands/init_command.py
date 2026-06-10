"""``lhp init`` — scaffold a new LakehousePlumber project in the cwd.

Thin CLI shim (constitution §2.7 / §9.11): parse args, run the LHP-IO-007
pre-check, delegate scaffolding to ``LakehousePlumberBootstrap``, then hand
the result to the presenter. No business logic lives here. Domain failures
surface by raising ``LHPError`` so :func:`cli_error_boundary` renders the
panel and maps it to a POSIX exit code.
"""

from __future__ import annotations

import logging
from pathlib import Path

import rich_click as click
from rich_click import RichCommand

from lhp.api import LakehousePlumberBootstrap
from lhp.errors import ErrorCategory, ErrorFactory, LHPError, codes

from .. import console as _console_module
from ..error_boundary import cli_error_boundary
from ..presenters.init_presenter import render_init_result

logger = logging.getLogger(__name__)


@click.command(cls=RichCommand, name="init")
@click.argument("name")
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Skip Databricks Asset Bundle setup (bundle is enabled by default).",
)
@cli_error_boundary("init")
def init(name: str, no_bundle: bool) -> None:
    """Initialize a new LakehousePlumber project in the current directory.

    NAME is baked into template substitutions (bundle name, lhp.yaml). All
    files are created in the current working directory.
    """
    project_path = Path.cwd()
    bundle = not no_bundle
    logger.info(f"Initializing project '{name}' in {project_path}, bundle={bundle}")

    # Refuse to clobber an existing project before any filesystem mutation.
    if (project_path / "lhp.yaml").exists():
        raise ErrorFactory.io_error(
            codes.IO_007,
            title="LHP project already exists",
            details="An lhp.yaml file already exists in this directory.",
            suggestions=[
                "Use a different directory to create a new project",
                "Remove the existing lhp.yaml if you want to reinitialize",
            ],
            context={"Directory": str(project_path)},
        )

    result = LakehousePlumberBootstrap().init_project(
        project_path, bundle=bundle, project_name=name
    )
    if not result.success:
        _raise_for_failure(result.error_code, result.error_message, project_path)

    logger.info(f"Project '{name}' initialized successfully")
    render_init_result(result, console=_console_module.console)


def _raise_for_failure(
    error_code: str | None, error_message: str | None, target_dir: Path
) -> None:
    """Re-raise a bootstrap failure as ``LHPError`` for the error boundary.

    A structured ``error_code`` (e.g. ``LHP-IO-007``) is round-tripped so
    the boundary emits the same exit code an inline raise would; failures
    without a code fall back to the GENERAL category.
    """
    category, code_number = ErrorCategory.GENERAL, "000"
    parts = (error_code or "").split("-")
    if len(parts) == 3:
        try:
            category = ErrorCategory(parts[1])
        except ValueError:
            category = ErrorCategory.GENERAL
        code_number = parts[2]
    raise LHPError(
        category=category,
        code_number=code_number,
        title=error_message or "Project initialization failed.",
        details=f"Target directory: {target_dir}",
        suggestions=[
            "Use a different directory to create a new project",
            "Remove the conflicting files if you want to scaffold here",
        ],
        context={"Directory": str(target_dir)},
    )

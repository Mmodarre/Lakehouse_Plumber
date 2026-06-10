"""``dag`` command: pipeline dependency analysis + output generation.

Parses options, calls the inspection facade twice (analyze + save outputs),
renders the analysis to stderr and the written paths to stdout, and lets
``cli_error_boundary`` map any domain error to an exit code. A hidden ``deps``
alias forwards here with a ``DeprecationWarning`` for backward compatibility.
The shared option stack lives in ``_dag_options`` so the two stay in sync.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Optional, Sequence

import click
from rich_click import RichCommand

from lhp.cli import console as _console_module
from lhp.cli._app_context import build_facade, resolve_project_root
from lhp.cli.commands._dag_options import dag_options
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.presenters import dag_files_presenter, dag_presenter


@click.command(cls=RichCommand, name="dag")
@dag_options
@cli_error_boundary("dag")
def dag(
    output_format: Sequence[str],
    output_dir: Path,
    bundle_output: bool,
    job_name: Optional[str],
    job_config: Optional[Path],
    expand_blueprints: bool,
    blueprint: Optional[str],
) -> None:
    """Analyze pipeline dependencies and write dependency outputs."""
    project_root = resolve_project_root()
    facade = build_facade(project_root)

    analysis = facade.inspection.analyze_dependencies(
        expand_blueprints=expand_blueprints, blueprint_filter=blueprint
    )
    dag_presenter.render_analysis(analysis, console=_console_module.err_console)

    outputs = facade.inspection.save_dependency_outputs(
        formats=output_format,
        output_dir=output_dir,
        expand_blueprints=expand_blueprints,
        blueprint_filter=blueprint,
        job_name=job_name,
        job_config_path=str(job_config) if job_config else None,
        bundle_output=bundle_output,
    )
    dag_files_presenter.render_outputs(outputs, console=_console_module.console)


@click.command(cls=RichCommand, name="deps", hidden=True)
@dag_options
@click.pass_context
def deps(ctx: click.Context, **kwargs: object) -> None:
    """Deprecated alias for ``dag`` (forwards with a DeprecationWarning)."""
    _console_module.err_console.print(
        "[dim]'lhp deps' is deprecated; use 'lhp dag' instead.[/dim]"
    )
    warnings.warn(
        "'lhp deps' is deprecated; use 'lhp dag' instead.",
        DeprecationWarning,
        stacklevel=2,
    )
    ctx.forward(dag)

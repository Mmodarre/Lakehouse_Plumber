"""``dag`` command: pipeline dependency analysis + output generation.

Parses options, calls the dependency facade twice (analyze + save outputs),
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
    pipeline: Optional[str],
    blueprint: Optional[str],
    trust_depends_on: bool,
    no_cache: bool,
    max_workers: Optional[int],
) -> None:
    """Analyze pipeline dependencies and write dependency outputs."""
    if expand_blueprints:
        _console_module.err_console.print(
            "[dim]'--expand-blueprints' is deprecated and has no effect; "
            "blueprints are always fully expanded.[/dim]"
        )
        warnings.warn(
            "'--expand-blueprints' is deprecated and has no effect; "
            "blueprints are always fully expanded.",
            DeprecationWarning,
            stacklevel=2,
        )

    project_root = resolve_project_root()
    facade = build_facade(project_root, max_workers=max_workers, no_cache=no_cache)

    # Both facade calls route through the service's memoized
    # analyze_project, so the project is discovered and analyzed once.
    analysis = facade.dependency.analyze_dependencies(
        pipeline_filter=pipeline,
        blueprint_filter=blueprint,
        trust_depends_on=trust_depends_on,
    )
    dag_presenter.render_analysis(analysis, console=_console_module.err_console)

    outputs = facade.dependency.save_dependency_outputs(
        formats=output_format,
        output_dir=output_dir,
        pipeline_filter=pipeline,
        blueprint_filter=blueprint,
        trust_depends_on=trust_depends_on,
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

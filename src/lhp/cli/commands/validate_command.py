"""``lhp validate`` command — validate pipeline configurations for an env.

Thin CLI shell (constitution §9.11 / TARGET_ARCHITECTURE §7) mirroring the
generate command: parse options, build the facade, stream the validation events
through the renderer, print the summary, map the outcome to an exit code.

Unlike generate, validate REPORTS findings rather than raising — every issue is
folded into the terminal ``BatchValidationResponse`` and ``render`` merges those
into ``RunOutcome.failures`` so ``exit_for_outcome`` is correct. An empty project
yields a clean ``ValidationCompleted`` and exits 0.
"""

from __future__ import annotations

import logging
from time import perf_counter

import click
from rich_click import RichCommand

from lhp.api import should_enable_bundle_support
from lhp.cli import console as _console_module
from lhp.cli._app_context import (
    build_facade,
    derive_pipeline_fields,
    exit_for_outcome,
    resolve_project_root,
)
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.presenters.event_stream._model import RenderOptions, RunHeader
from lhp.cli.presenters.event_stream.renderer_factory import render
from lhp.cli.presenters.summary_presenter import print_run_summary

logger = logging.getLogger(__name__)

_WORKERS_HELP = "Max worker processes (default ~80%% of CPUs; 1 = sequential)."


@click.command(cls=RichCommand, name="validate")
@click.option(
    "-e", "--env", default="dev", show_default=True, help="Target environment."
)
@click.option("-s", "--show-details", is_flag=True, help="Expand failures into panels.")
@click.option("--strict", is_flag=True, help="Treat warnings as failures.")
@click.option("--no-progress", is_flag=True, help="Disable the live progress display.")
@click.option("--no-bundle", is_flag=True, help="Disable bundle support.")
@click.option(
    "--include-tests", is_flag=True, help="Include test actions in validation."
)
@click.option("-p", "--pipeline", default=None, help="Validate a single pipeline only.")
@click.option(
    "-pc", "--pipeline-config", default=None, help="Custom pipeline config path."
)
@click.option(
    "--max-workers", type=click.IntRange(min=1), default=None, help=_WORKERS_HELP
)
@cli_error_boundary("validate")
def validate_command(
    env: str,
    show_details: bool,
    strict: bool,
    no_progress: bool,
    no_bundle: bool,
    include_tests: bool,
    pipeline: str | None,
    pipeline_config: str | None,
    max_workers: int | None,
) -> None:
    """Validate pipeline configurations for ENV."""
    logger.debug(f"Validate request: env={env}, pipeline={pipeline}")
    project_root = resolve_project_root()
    facade = build_facade(
        project_root, pipeline_config=pipeline_config, max_workers=max_workers
    )
    bundle_enabled = should_enable_bundle_support(project_root, cli_no_bundle=no_bundle)

    fields = derive_pipeline_fields(facade, pipeline)
    header = RunHeader("validate", env, 1 if pipeline else len(fields))
    events = facade.validation.validate_pipelines(
        env=env,
        pipeline_filter=pipeline,
        pipeline_fields=fields,
        include_tests=include_tests,
        bundle_enabled=bundle_enabled,
        max_workers=max_workers,
    )
    options = RenderOptions(show_details=show_details, strict=strict)
    started = perf_counter()
    outcome = render(events, header, options=options, no_progress=no_progress)
    print_run_summary(
        outcome,
        header,
        elapsed_s=perf_counter() - started,
        options=options,
        err_console=_console_module.err_console,
    )
    exit_for_outcome(outcome, strict=strict)

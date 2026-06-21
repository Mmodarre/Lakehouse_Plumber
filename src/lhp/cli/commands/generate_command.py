"""``lhp generate`` command — single-stream pipeline code generation.

Thin CLI shell (§9.11 / TARGET_ARCHITECTURE §7): parse options, build the
facade, drain the generation event stream through the renderer, print the
summary, and exit with the outcome-implied code. No business logic here — all
phases run behind the facade. Status/summary to stderr; stdout stays empty.
"""

from __future__ import annotations

import logging
from time import perf_counter
from typing import Optional

import click
from rich_click import RichCommand

from lhp.api import ProgressSink, should_enable_bundle_support
from lhp.cli import console as _console_module
from lhp.cli._app_context import (
    build_facade,
    exit_for_outcome,
    resolve_project_root,
)
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.presenters.event_stream._model import RenderOptions, RunHeader
from lhp.cli.presenters.event_stream.renderer_factory import render
from lhp.cli.presenters.summary_presenter import print_run_summary

logger = logging.getLogger(__name__)


@click.command(cls=RichCommand, name="generate")
@click.option("-e", "--env", required=True, help="Target environment.")
@click.option(
    "-s",
    "--show-details",
    is_flag=True,
    help="Show the per-pipeline completion trail and expand failures to panels.",
)
@click.option("--strict", is_flag=True, help="Treat warnings as failures.")
@click.option("--no-progress", is_flag=True, help="Disable the live display.")
@click.option("--no-bundle", is_flag=True, help="Skip Asset Bundle sync.")
@click.option(
    "--no-contracts", is_flag=True, help="Skip ODCS data-contract translation."
)
@click.option("--include-tests", is_flag=True, help="Include test actions.")
@click.option("--no-format", is_flag=True, help="Skip code formatting.")
@click.option("-p", "--pipeline", default=None, help="Only the named pipeline.")
@click.option("-o", "--output", default=None, help="Output dir override.")
@click.option("-pc", "--pipeline-config", default=None, help="pipeline_config.yaml.")
@click.option("--max-workers", type=int, default=None, help="Max parallel workers.")
@click.option("--force", is_flag=True, hidden=True, help="Deprecated no-op.")
@cli_error_boundary("generate")
def generate(
    env: str,
    show_details: bool,
    strict: bool,
    no_progress: bool,
    no_bundle: bool,
    no_contracts: bool,
    include_tests: bool,
    no_format: bool,
    pipeline: Optional[str],
    output: Optional[str],
    pipeline_config: Optional[str],
    max_workers: Optional[int],
    force: bool,
) -> None:
    """Generate Databricks pipeline code for ENV from the project's flowgroups."""
    # Group-level ``lhp --no-progress generate`` falls through here: OR the
    # per-command flag with the group value stored in ``ctx.obj`` (main.py).
    no_progress = no_progress or bool(
        (click.get_current_context().obj or {}).get("no_progress")
    )
    if force:
        msg = "[dim]--force is deprecated and a no-op; every run regenerates.[/dim]"
        _console_module.err_console.print(msg)

    project_root = resolve_project_root()
    facade = build_facade(
        project_root,
        pipeline_config=pipeline_config,
        max_workers=max_workers,
        translate_contracts=not no_contracts,
    )

    bundle_enabled = should_enable_bundle_support(project_root, cli_no_bundle=no_bundle)
    output_dir = project_root / (output if output is not None else f"generated/{env}")

    header = RunHeader("generate", env)

    # One ProgressSink shared by the facade (which advances it per-flowgroup)
    # and the renderer (which reads it to animate the flowgroup bar).
    progress = ProgressSink()
    events = facade.generation.generate_pipelines(
        env=env,
        output_dir=output_dir,
        bundle_enabled=bundle_enabled,
        include_tests=include_tests,
        apply_formatting=(False if no_format else None),
        pipeline_filter=pipeline,
        max_workers=max_workers,
        progress=progress,
    )

    options = RenderOptions(show_details=show_details, strict=strict)
    start = perf_counter()
    outcome = render(
        events, header, options=options, no_progress=no_progress, progress=progress
    )
    print_run_summary(
        outcome,
        header,
        elapsed_s=perf_counter() - start,
        options=options,
        err_console=_console_module.err_console,
    )
    exit_for_outcome(outcome, strict=strict)

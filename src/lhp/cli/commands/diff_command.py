"""``lhp diff`` command — plan-vs-disk change set for a generation run.

Thin CLI shell (§9.11): plan every flowgroup through the shared event-stream
``render``, read the on-disk ``generated/<env>`` tree (the only filesystem
logic here) into a ``{path: content}`` map, and diff via ``diff_presenter``.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, Optional

import click
from rich_click import RichCommand

from lhp.api import GenerationPlan, ProgressSink

from .. import console as _console_module
from .._app_context import build_facade, resolve_project_root
from ..error_boundary import cli_error_boundary
from ..exit_codes import ExitCode
from ..presenters import diff_presenter
from ..presenters.event_stream._model import RenderOptions, RunHeader
from ..presenters.event_stream.renderer_factory import render

logger = logging.getLogger(__name__)


def _read_on_disk_tree(output_location: Optional[Path]) -> Dict[str, str]:
    """Read ``output_location`` into a ``{posix-relative-path: content}`` map.

    Keyed like the plan's ``str(path)`` fold (UTF-8) so the presenter's pure
    diff lines them up; a missing/unset location yields an empty map.
    """
    if output_location is None or not output_location.is_dir():
        return {}
    on_disk: Dict[str, str] = {}
    for path in output_location.rglob("*"):
        if path.is_file():
            rel = path.relative_to(output_location).as_posix()
            on_disk[rel] = path.read_text(encoding="utf-8")
    return on_disk


@click.command(cls=RichCommand, name="diff")
@click.option(
    "-e", "--env", default="dev", show_default=True, help="Environment to diff."
)
@click.option(
    "-s", "--show-details", is_flag=True, help="Show a unified diff per file."
)
@click.option("--no-progress", is_flag=True, help="Disable the live progress display.")
@click.option("--include-tests", is_flag=True, help="Include generated test hooks.")
@click.option("-p", "--pipeline", default=None, help="Diff only this pipeline.")
@click.option("--exit-code", is_flag=True, help="Exit non-zero on a non-empty diff.")
@cli_error_boundary("diff")
def diff_command(
    env: str,
    show_details: bool,
    no_progress: bool,
    include_tests: bool,
    pipeline: Optional[str],
    exit_code: bool,
) -> None:
    """Show what ``lhp generate --env ENV`` would change on disk.

    Plans every flowgroup (writing nothing), compares the planned files to the
    on-disk ``generated/<ENV>`` tree, and prints one ``~`` / ``+`` / ``-`` line
    per changed path. With ``--exit-code`` a non-empty diff exits ``1``.
    """
    # Group-level ``lhp --no-progress diff`` falls through here: OR the
    # per-command flag with the group value stored in ``ctx.obj`` (main.py).
    no_progress = no_progress or bool(
        (click.get_current_context().obj or {}).get("no_progress")
    )
    logger.debug(f"Diffing planned vs on-disk output for environment '{env}'")
    facade = build_facade(resolve_project_root())
    # One ProgressSink shared by the facade and the renderer. The plan/diff path
    # may not drive the counter; the renderer's total>0 guard then shows just
    # the animated spinner + phase rather than a misleading 0/0 bar.
    progress = ProgressSink()
    events = facade.generation.plan_generation(
        env,
        pipeline_filter=pipeline,
        include_tests=include_tests,
        progress=progress,
    )
    outcome = render(
        events,
        RunHeader("diff", env),
        options=RenderOptions(show_details=show_details),
        no_progress=no_progress,
        progress=progress,
    )
    plan = outcome.response
    assert isinstance(plan, GenerationPlan)
    on_disk = _read_on_disk_tree(plan.output_location)
    changed = diff_presenter.render_diff(
        plan, on_disk, console=_console_module.console, show_details=show_details
    )
    if exit_code and changed:
        raise SystemExit(ExitCode.ERROR)

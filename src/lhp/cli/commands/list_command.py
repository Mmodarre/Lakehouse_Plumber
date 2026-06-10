"""``lhp list`` command group — enumerate presets, templates, and blueprints.

Thin CLI shell (constitution §7 / §9.11): each subcommand resolves the project
root, builds the facade, calls the matching ``inspection.list_*`` read, and hands
the frozen views to ``list_presenter``. No business logic, no domain imports —
only ``lhp.api`` (via ``_app_context``) and the CLI-internal presenter / error
boundary. The listing reads are environment-independent, so there is no ``-e``.
"""

from __future__ import annotations

import logging

import click
from rich_click import RichGroup

from lhp.cli._app_context import build_facade, resolve_project_root
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.presenters import list_presenter

logger = logging.getLogger(__name__)


@click.group(name="list", cls=RichGroup)
def list_group() -> None:
    """List project presets, templates, and blueprints."""


@list_group.command(name="presets")
@cli_error_boundary("list presets")
def presets() -> None:
    """List presets declared under the project's ``presets/`` directory."""
    facade = build_facade(resolve_project_root())
    list_presenter.render_presets(facade.inspection.list_presets())


@list_group.command(name="templates")
@cli_error_boundary("list templates")
def templates() -> None:
    """List templates declared under the project's ``templates/`` directory."""
    facade = build_facade(resolve_project_root())
    list_presenter.render_templates(facade.inspection.list_templates())


@list_group.command(name="blueprints")
@click.option(
    "--instances",
    is_flag=True,
    default=False,
    help="Show each blueprint's instances and the pipelines they produce.",
)
@cli_error_boundary("list blueprints")
def blueprints(instances: bool) -> None:
    """List blueprints; with ``--instances`` show per-instance pipelines."""
    facade = build_facade(resolve_project_root())
    views = facade.inspection.list_blueprints(include_instances=instances)
    list_presenter.render_blueprints(views, instances=instances)

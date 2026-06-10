"""``lhp substitutions`` command — show resolved substitution tokens for an env.

Thin CLI shell (constitution §9.11 / TARGET_ARCHITECTURE §7): parse options,
resolve the project root, call the inspection facade, hand the resulting
:class:`lhp.api.views.SubstitutionView` to the presenter, and let
``cli_error_boundary`` map any ``LHPError`` to an exit code. No business logic
lives here — token resolution happens entirely behind the facade.
"""

from __future__ import annotations

import logging

import click
from rich_click import RichCommand

from .. import console as _console_module
from .._app_context import build_facade, resolve_project_root
from ..error_boundary import cli_error_boundary
from ..presenters import substitutions_presenter

logger = logging.getLogger(__name__)


@click.command(cls=RichCommand, name="substitutions")
@click.option(
    "-e",
    "--env",
    default="dev",
    show_default=True,
    help="Environment whose substitution tokens to resolve.",
)
@cli_error_boundary("substitutions")
def substitutions_command(env: str) -> None:
    """Show the resolved substitution tokens for ENV.

    Lists every ``${token}`` mapping (with nested maps rendered as trees),
    the observed ``${secret:scope/key}`` references, and the default secret
    scope. A missing ``substitutions/<env>.yaml`` is not an error — an empty
    context is reported instead.
    """
    logger.debug(f"Resolving substitutions for environment '{env}'")
    project_root = resolve_project_root()
    facade = build_facade(project_root)
    view = facade.inspection.build_substitution_view(env)
    substitutions_presenter.render(view, console=_console_module.console)

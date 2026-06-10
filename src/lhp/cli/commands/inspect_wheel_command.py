"""``lhp inspect-wheel`` command — list or extract a built wheel's modules.

Thin CLI shell (constitution §9.11 / TARGET_ARCHITECTURE §7): disambiguate the
SELECTOR by shape, resolve the project root, call the wheel facade, hand the
resulting DTO to the presenter, and let ``cli_error_boundary`` map any
``LHPError`` to an exit code. No business logic lives here — wheel location,
validation, and extraction happen entirely behind the facade.

SELECTOR is a wheel path (``*.whl`` or anything containing an OS path
separator) or a pipeline name. A pipeline name REQUIRES ``--env`` so the facade
can locate the single wheel built under ``generated/<env>/_wheels/<pipeline>/``;
``--env`` is ignored in path-mode. File existence / corruption are not checked
here — the facade raises the typed ``LHP-IO-*`` / ``LHP-GEN-*`` error.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

import click
from rich_click import RichCommand

from .. import console as _console_module
from .._app_context import build_facade, resolve_project_root
from ..error_boundary import cli_error_boundary
from ..presenters import wheel_presenter

logger = logging.getLogger(__name__)


def _looks_like_path(selector: str) -> bool:
    """True when SELECTOR is a wheel path rather than a pipeline name.

    Treated as a path if it ends in ``.whl`` or contains an OS path
    separator (``os.sep``, or ``os.altsep`` where the platform defines one).
    """
    separators = [os.sep] + ([os.altsep] if os.altsep else [])
    return selector.endswith(".whl") or any(sep in selector for sep in separators)


@click.command(cls=RichCommand, name="inspect-wheel")
@click.argument("selector")
@click.option(
    "-e",
    "--env",
    default=None,
    help="Environment whose built wheel to inspect (required in pipeline-mode).",
)
@click.option(
    "--extract",
    "extract_dir",
    type=click.Path(file_okay=False, path_type=Path),
    default=None,
    help="Extract the wheel's .py modules into this directory instead of listing.",
)
@cli_error_boundary("inspect-wheel")
def inspect_wheel_command(
    selector: str, env: Optional[str], extract_dir: Optional[Path]
) -> None:
    """Inspect (or extract from) a built pipeline wheel given by SELECTOR.

    SELECTOR is either a wheel path (``*.whl`` / a path with a separator) or a
    pipeline name; a pipeline name requires ``-e/--env``. With ``--extract`` the
    wheel's ``.py`` modules are written to disk; otherwise they are listed.
    """
    logger.debug(f"Inspecting wheel selector '{selector}' (env={env})")
    project_root = resolve_project_root()
    facade = build_facade(project_root)

    if _looks_like_path(selector):
        wheel_path: Optional[Path] = Path(selector)
        pipeline: Optional[str] = None
    else:
        if env is None:
            raise click.UsageError(
                f"'{selector}' is a pipeline name; '-e/--env' is required. "
                "Pass a path ending in '.whl' to inspect a wheel directly."
            )
        wheel_path = None
        pipeline = selector

    if extract_dir is None:
        view = facade.wheel.list_modules(
            wheel_path=wheel_path, pipeline=pipeline, env=env
        )
        wheel_presenter.render_inspection(view, console=_console_module.console)
    else:
        result = facade.wheel.extract_modules(
            extract_dir, wheel_path=wheel_path, pipeline=pipeline, env=env
        )
        wheel_presenter.render_extraction(result, console=_console_module.console)

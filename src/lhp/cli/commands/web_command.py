"""``lhp web`` command — launch the local web IDE backend (uvicorn + FastAPI).

Thin CLI shell (constitution §9.11 / TARGET_ARCHITECTURE §7): resolve the
project root (with ``--allow-empty``, fall back to the current directory so the
in-app init wizard is reachable outside an initialized project), check that the
optional webapp dependencies are installed, mint
the per-session token, export the ``LHP_WEBAPP_*`` configuration into the
environment, preflight the port, and hand off to ``uvicorn.run`` with the
zero-argument factory string ``lhp.webapp.app:create_app``. Launch mechanics
(token minting, port preflight, readiness-poll browser opener) live in
:mod:`lhp.cli.commands._web_launch`.

All configuration flows through environment variables (read by
:func:`lhp.webapp.settings.get_settings`), which is what lets ``--reload`` work:
uvicorn re-imports the factory in a fresh worker process that inherits the same
environment. The factory takes no arguments, so there is deliberately no
``create_app(settings=...)`` channel. This module also never imports
``lhp.webapp`` — the factory string defers that import to uvicorn's process, so
``lhp --help`` and the dependency guard below never pull the FastAPI stack.
"""

from __future__ import annotations

import importlib.util
import logging
import os

import click
from rich_click import RichCommand

from lhp.errors import ErrorFactory, codes

from .._app_context import resolve_project_root
from ..error_boundary import cli_error_boundary
from . import _web_launch

logger = logging.getLogger(__name__)

# Webapp handoff contract. These mirror lhp.webapp.settings (HOST and the
# LHP_WEBAPP_* env var names) and the uvicorn factory target in
# lhp.webapp.app:create_app. They are inlined — not imported from lhp.webapp —
# so this command never pulls the FastAPI/uvicorn stack into the CLI import
# graph; the factory string defers that import to uvicorn's own process.
_WEBAPP_HOST = "127.0.0.1"
_WEBAPP_FACTORY = "lhp.webapp.app:create_app"
_ENV_PROJECT_ROOT = "LHP_WEBAPP_PROJECT_ROOT"
_ENV_PORT = "LHP_WEBAPP_PORT"
_ENV_LOG_LEVEL = "LHP_WEBAPP_LOG_LEVEL"
_ENV_TOKEN = "LHP_WEBAPP_TOKEN"


@click.command(cls=RichCommand, name="web")
@click.option(
    "--port",
    type=int,
    default=8000,
    show_default=True,
    help="Port to bind the local web IDE on (host is pinned to 127.0.0.1).",
)
@click.option(
    "--no-open",
    "no_open",
    is_flag=True,
    default=False,
    help="Do not open a browser window automatically on startup.",
)
@click.option(
    "--reload",
    "reload",
    is_flag=True,
    default=False,
    help="Reload the server on code changes (development convenience).",
)
@click.option(
    "--allow-empty",
    "allow_empty",
    is_flag=True,
    default=False,
    help=(
        "Launch even when the current directory is not an initialized LHP "
        "project (enables the in-app init wizard)."
    ),
)
@cli_error_boundary("web")
def web_command(port: int, no_open: bool, reload: bool, allow_empty: bool) -> None:
    """Launch the Lakehouse Plumber local web IDE for the current project.

    Starts a FastAPI backend (served via uvicorn) bound to ``127.0.0.1`` — the
    IDE is loopback-only and never exposed beyond this machine. Access is
    protected by a per-session token embedded in the URL fragment. Requires the
    optional webapp extra: ``pip install lakehouse-plumber[webapp]``.

    Configuration is passed to the server through ``LHP_WEBAPP_*`` environment
    variables, so ``--reload`` works across uvicorn's worker reloads.

    With ``--allow-empty`` the IDE also starts outside an initialized project
    and serves the in-app init wizard instead of the project view.
    """
    logger.debug(f"Starting web IDE on port {port} (reload={reload})")
    if allow_empty:
        # Permissive launch: the root handed to the server may lack lhp.yaml —
        # its lifespan detects the missing marker and serves the init wizard.
        project_root = _web_launch.resolve_project_root_or_cwd()
    else:
        project_root = resolve_project_root()

    # Dependency guard: the webapp stack is an optional extra. Fail with a
    # friendly, actionable error rather than an ImportError traceback.
    if (
        importlib.util.find_spec("fastapi") is None
        or importlib.util.find_spec("uvicorn") is None
    ):
        raise ErrorFactory.io_error(
            codes.IO_026,
            title="Web IDE dependencies are not installed",
            details=(
                "The 'lhp web' command requires the optional webapp dependencies "
                "(fastapi and uvicorn), which are not installed."
            ),
            suggestions=[
                "Install the webapp extra: pip install lakehouse-plumber[webapp]",
                "Then re-run: lhp web",
            ],
        )

    _web_launch.preflight_port(_WEBAPP_HOST, port)

    # Hand configuration to the uvicorn worker process via the environment;
    # the factory string takes no arguments, so env vars are the only channel
    # (they also survive --reload's worker respawns).
    token = _web_launch.mint_token()
    log_level = "info"
    os.environ[_ENV_PROJECT_ROOT] = str(project_root)
    os.environ[_ENV_PORT] = str(port)
    os.environ[_ENV_LOG_LEVEL] = log_level
    os.environ[_ENV_TOKEN] = token

    base_url = f"http://{_WEBAPP_HOST}:{port}"
    tokened_url = f"{base_url}/#token={token}"
    # Echo the tokened URL so users who suppress the browser can click it.
    click.echo(f"Starting Lakehouse Plumber web IDE at {tokened_url}")

    if not no_open:
        _web_launch.open_browser_when_ready(
            tokened_url, health_url=f"{base_url}/api/health"
        )

    # Imported lazily, after the dependency guard, so the FastAPI/uvicorn stack
    # is never pulled into the help/version paths or when the extra is absent.
    import uvicorn

    # access_log=False: uvicorn's access log would print full request targets,
    # leaking the session token that SSE clients pass as ?token=... (the app's
    # RequestLoggingMiddleware already logs method/path/status WITHOUT the
    # query string, so no logging coverage is lost).
    uvicorn.run(
        _WEBAPP_FACTORY,
        host=_WEBAPP_HOST,
        port=port,
        factory=True,
        reload=reload,
        log_level=log_level,
        access_log=False,
    )

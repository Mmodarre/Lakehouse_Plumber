"""Launch mechanics for the ``lhp web`` command.

Private helper module so :mod:`lhp.cli.commands.web_command` stays a thin Click
shell (constitution §9.11): resolves the permissive project root for
``--allow-empty``, mints the per-session token, preflights the port, and opens
the browser once the server answers its health probe. Deliberately
never imports ``lhp.webapp`` — the uvicorn factory string owns that import in
the server's own process, keeping the FastAPI stack out of the CLI import
graph.
"""

from __future__ import annotations

import errno
import logging
import secrets
import socket
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path

from lhp.errors import ErrorFactory, LHPError, codes

from .._app_context import resolve_project_root

logger = logging.getLogger(__name__)

# Windows sockets surface WSAEADDRINUSE (10048) instead of the POSIX value.
_ADDR_IN_USE_ERRNOS = frozenset(
    {errno.EADDRINUSE, getattr(errno, "WSAEADDRINUSE", errno.EADDRINUSE)}
)

_READINESS_TIMEOUT_S = 15.0
_READINESS_POLL_INTERVAL_S = 0.25
_READINESS_REQUEST_TIMEOUT_S = 1.0


def resolve_project_root_or_cwd() -> Path:
    """Return the LHP project root, or the current directory when none exists.

    Backs ``lhp web --allow-empty``: prefers the normal walk-up discovery so a
    launch from inside a real project (including a subdirectory) still lands on
    the project root, and only falls back to the current working directory when
    no ``lhp.yaml`` exists anywhere up the tree. The server's lifespan detects
    the missing ``lhp.yaml`` itself and serves the in-app init wizard, so this
    helper deliberately does not replicate that detection (§9.11 — the CLI just
    stops blocking).
    """
    try:
        return resolve_project_root()
    except LHPError:
        root = Path.cwd().resolve()
        logger.debug(f"No lhp.yaml found; launching web IDE with empty root {root}")
        return root


def mint_token() -> str:
    """Mint an unguessable per-session token for the web IDE."""
    return secrets.token_urlsafe(32)


def preflight_port(host: str, port: int) -> None:
    """Fail fast if ``port`` is already bound on ``host``.

    Binds and immediately closes a probe socket. ``SO_REUSEADDR`` is
    deliberately left off so a live listener is detected rather than silently
    shared.

    :raises LHPError: ``LHP-IO-027`` when the port is already in use.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind((host, port))
    except OSError as exc:
        if exc.errno in _ADDR_IN_USE_ERRNOS:
            raise ErrorFactory.io_error(
                codes.IO_027,
                title=f"Port {port} is already in use",
                details=(
                    f"Another process is already listening on {host}:{port}, "
                    "so the web IDE server cannot start there."
                ),
                suggestions=[
                    f"Pick another port: lhp web --port {port + 1}",
                    f"Or stop the process currently listening on port {port}",
                ],
            ) from exc
        raise
    finally:
        probe.close()


def open_browser_when_ready(
    url: str,
    health_url: str,
    timeout_s: float = _READINESS_TIMEOUT_S,
    poll_interval_s: float = _READINESS_POLL_INTERVAL_S,
) -> threading.Thread:
    """Open ``url`` in a browser once ``health_url`` answers 200 (or on timeout).

    Replaces a fixed startup delay with a readiness poll: a daemon thread polls
    the (token-exempt) health endpoint until it returns 200 or ``timeout_s``
    elapses, then opens the browser exactly once. Daemon + best-effort: it
    never blocks process exit, and a failed browser open (headless box, no
    browser configured) must not bring the server down. Returns the thread so
    callers/tests can join it.
    """

    def _poll_then_open() -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(
                    health_url, timeout=_READINESS_REQUEST_TIMEOUT_S
                ) as response:
                    if response.status == 200:
                        break
            except (urllib.error.URLError, OSError):
                logger.debug("Web IDE not ready yet; retrying health probe")
            time.sleep(poll_interval_s)
        try:
            webbrowser.open(url)
        except Exception:
            logger.debug("Could not open a web browser automatically", exc_info=True)

    thread = threading.Thread(
        target=_poll_then_open, name="lhp-web-browser-opener", daemon=True
    )
    thread.start()
    return thread

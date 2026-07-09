"""Omnigent daemon lifecycle: base-URL policy, detection ladder, spawn.

The assistant feature depends on a USER-MANAGED local omnigent daemon (an
HTTP server plus a host process that runs agent sessions). This module owns
the three lifecycle concerns the webapp has:

* :func:`resolve_base_url` — the single policy for which URL the webapp
  talks to (``settings.assistant_url`` or the loopback default), with
  loopback enforcement as defense in depth (settings validates the env var
  at load time too).
* :func:`detect` — the three-rung readiness ladder (``omnigent`` binary on
  PATH -> server ``GET /health`` answers -> an ``"online"`` host in
  ``GET /v1/hosts``), reported as a :class:`DaemonStatus`.
* :func:`start_daemon` — spawn ``omnigent server start`` and ``omnigent
  host`` as DETACHED processes. LHP retains no handles and NEVER waits for,
  terminates, or kills them: the daemon is user-owned and deliberately
  outlives the webapp (live agent sessions must not die with a server
  restart). Output is appended to ``<project>/.lhp/logs/omnigent-<name>.log``.

:stability: internal
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional, Union
from urllib.parse import urlsplit

import httpx

from lhp.webapp.services.omnigent_client import OmnigentClient, OmnigentUnavailable
from lhp.webapp.settings import WebappSettings

logger = logging.getLogger(__name__)

#: Omnigent's own default bind address.
DEFAULT_ASSISTANT_URL = "http://127.0.0.1:6767"

_BINARY = "omnigent"

#: The daemon is unauthenticated — loopback-only is a security property.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

#: The two detached daemon processes :func:`start_daemon` spawns, in order.
_DAEMON_COMMANDS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("server", (_BINARY, "server", "start")),
    ("host", (_BINARY, "host")),
)


@dataclass(frozen=True)
class DaemonStatus:
    """Achieved state of :func:`detect`'s readiness ladder.

    ``detail`` names the rung that stopped the ladder (``None`` when every
    rung passed); ``host_id`` is the first ``"online"`` host, set only when
    ``host_online`` is true.
    """

    binary_found: bool
    server_ok: bool
    host_online: bool
    host_id: Optional[str]
    base_url: str
    detail: Optional[str]


def resolve_base_url(settings: WebappSettings) -> str:
    """Resolve the omnigent base URL from settings, enforcing loopback.

    Defense in depth: settings validates ``LHP_WEBAPP_ASSISTANT_URL`` at load
    time too; re-checking here keeps the policy attached to the value's USE,
    so no other construction path can smuggle in a non-loopback daemon URL.

    Raises:
        ValueError: if the URL is not http(s) on 127.0.0.1 / localhost / ::1.
    """
    url = settings.assistant_url or DEFAULT_ASSISTANT_URL
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or parts.hostname not in _LOOPBACK_HOSTS:
        raise ValueError(
            "assistant URL must be http(s) on a loopback host "
            f"(127.0.0.1, localhost or ::1), got {url!r}"
        )
    return url.rstrip("/")


async def detect(
    client_or_getter: Union[OmnigentClient, Callable[[], OmnigentClient]],
    settings: WebappSettings,
) -> DaemonStatus:
    """Probe daemon readiness and report the achieved state.

    ``binary_found`` (``omnigent`` on PATH) is probed INDEPENDENTLY of the
    server rungs: a daemon started from a venv whose bin dir is not on PATH
    is a supported setup, so a missing binary must not hide a healthy
    server. The binary flag only gates "Start it for me" (spawning needs a
    binary). Server rungs stay laddered: ``GET /health`` answers -> some
    host in ``GET /v1/hosts`` has ``status == "online"``. Daemon absence
    never raises — only a mis-configured URL does
    (:func:`resolve_base_url`'s ``ValueError``).

    ``client_or_getter`` is the client itself or a zero-arg callable
    returning it (e.g. ``lambda: get_omnigent_client(app)``).
    """
    base_url = resolve_base_url(settings)
    binary_found = shutil.which(_BINARY) is not None
    binary_detail = None if binary_found else f"'{_BINARY}' binary not found on PATH"
    if isinstance(client_or_getter, OmnigentClient):
        client = client_or_getter
    else:
        client = client_or_getter()
    try:
        await client.health()
    except (OmnigentUnavailable, httpx.HTTPStatusError) as exc:
        logger.debug(f"omnigent health rung failed: {exc}")
        return DaemonStatus(
            binary_found=binary_found,
            server_ok=False,
            host_online=False,
            host_id=None,
            base_url=base_url,
            detail=binary_detail or f"omnigent server not answering at {base_url}",
        )
    try:
        hosts = await client.hosts()
    except (OmnigentUnavailable, httpx.HTTPStatusError) as exc:
        logger.debug(f"omnigent hosts rung failed: {exc}")
        return DaemonStatus(
            binary_found=binary_found,
            server_ok=True,
            host_online=False,
            host_id=None,
            base_url=base_url,
            detail="omnigent host listing failed",
        )
    online = next((h for h in hosts if h.get("status") == "online"), None)
    if online is None:
        return DaemonStatus(
            binary_found=binary_found,
            server_ok=True,
            host_online=False,
            host_id=None,
            base_url=base_url,
            detail="no omnigent host is online",
        )
    return DaemonStatus(
        binary_found=binary_found,
        server_ok=True,
        host_online=True,
        host_id=online.get("host_id"),
        base_url=base_url,
        detail=None,
    )


def start_daemon(project_root: Path) -> None:
    """Spawn ``omnigent server start`` and ``omnigent host``, fully detached.

    Fire-and-forget by contract:

    * ``start_new_session=True`` — each child leads its own session, so it
      never receives this process's signals and survives webapp shutdown.
    * NO ``Popen`` handle is retained and LHP NEVER waits for, terminates, or
      kills these processes. The daemon is user-owned and deliberately
      outlives the IDE (live agent sessions must not die with a page reload
      or server restart); stopping it is the user's call.
    * stdout+stderr are appended to ``.lhp/logs/omnigent-<name>.log`` under
      ``project_root`` (the directory is created if missing). The parent's
      copy of the log handle is closed right after spawn — the child holds
      its own duplicate.

    Raises:
        OSError: if the ``omnigent`` binary cannot be executed (callers gate
            on :func:`detect`'s ``binary_found`` rung first).
    """
    logs_dir = project_root / ".lhp" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    for name, command in _DAEMON_COMMANDS:
        log_path = logs_dir / f"omnigent-{name}.log"
        with log_path.open("ab") as log_file:
            subprocess.Popen(  # detached on purpose; no handle retained
                list(command),
                stdin=subprocess.DEVNULL,
                stdout=log_file,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        logger.info(f"spawned detached '{' '.join(command)}' (log: {log_path})")

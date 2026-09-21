"""Route-family request counting for the web IDE's anonymous telemetry.

:class:`TelemetrySessionMiddleware` is the innermost middleware: guard
rejections (host, origin, token) never reach it, and it observes a request
after the router has matched, so ``request.scope["route"]`` is the
``APIRoute`` that served it. Each counted request is attributed to the tab's
session (the ``X-LHP-Session`` header) by its route FAMILY — a label from the
bounded table below — and never by URL: the concrete ``request.url.path`` is
not read here at all, so a file name, a flowgroup name or a query string can
never reach an event.

Template spelling: the table is keyed by the template as the router declares
it — the router's own sub-prefix included (``/assistant/chat``), the
app-level ``/api`` mount excluded — because FastAPI hands the middleware
either form depending on version: releases that clone included routes bake
the mount prefix into ``APIRoute.path``, releases that keep included routers
nested do not. Stripping a known mount prefix is lossless, so
:func:`route_family` accepts both spellings; adding one is not (the SPA
shell registers root-level ``APIRoute`` objects too).

The middleware also runs the opportunistic idle sweep: at most every
``SWEEP_INTERVAL_SECONDS`` it hands ``WebSessionRegistry.sweep_idle`` to a
worker thread and returns without waiting. Nothing here can raise into a
response.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Optional

from fastapi import FastAPI
from fastapi.routing import APIRoute
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from lhp.webapp.services import telemetry_sessions
from lhp.webapp.services._telemetry_events import session_for
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry
from lhp.webapp.static_app import _API_PREFIX

logger = logging.getLogger(__name__)

#: The only methods counted; ``HEAD``/``OPTIONS`` and the like are skipped.
COUNTED_METHODS: frozenset[str] = frozenset({"GET", "POST", "PUT", "DELETE"})
#: The family of an ``APIRoute`` the table does not know — visible, not lost.
OTHER_FAMILY = "other"
#: Templates never counted: the health poll, the SSE channel, the UI-event
#: sink and the OpenAPI pages (which are plain routes, listed for the record).
SKIPPED_TEMPLATES: frozenset[str] = frozenset(
    {"/health", "/events", "/telemetry/ui", "/docs", "/redoc", "/openapi.json"}
)

#: Explicit rows, ``(METHOD, template) -> family`` (DESIGN §E3).
ROUTE_FAMILIES: dict[tuple[str, str], str] = {
    ("GET", "/project"): "project.read",
    ("GET", "/project/stats"): "project.stats",
    ("POST", "/project/init"): "project.init",
    ("GET", "/pipelines"): "pipelines.read",
    ("GET", "/pipelines/{name}"): "pipelines.read",
    ("GET", "/pipelines/{name}/flowgroups"): "pipelines.read",
    ("GET", "/flowgroups"): "flowgroups.read",
    ("GET", "/flowgroups/{name}"): "flowgroups.read",
    ("GET", "/flowgroups/{name}/related-files"): "flowgroups.read",
    ("GET", "/flowgroups/{name}/resolved"): "flowgroups.read",
    ("GET", "/tables"): "tables.read",
    ("GET", "/lineage"): "lineage.read",
    ("GET", "/presets"): "presets.read",
    ("GET", "/presets/{name}"): "presets.read",
    ("GET", "/templates"): "templates.read",
    ("GET", "/templates/{name}"): "templates.read",
    ("GET", "/blueprints"): "blueprints.read",
    ("GET", "/environments"): "environments.read",
    ("GET", "/environments/{env}/resolved"): "environments.read",
    ("GET", "/operational-metadata"): "metadata.read",
    ("GET", "/dependencies"): "dependencies.read",
    ("GET", "/dependencies/graph/pipeline"): "dependencies.read",
    ("GET", "/dependencies/graph/flowgroup"): "dependencies.read",
    ("GET", "/dependencies/graph/action"): "dependencies.read",
    ("GET", "/dependencies/cross-pipeline"): "dependencies.read",
    ("GET", "/dependencies/execution-order"): "dependencies.read",
    ("GET", "/dependencies/circular"): "dependencies.read",
    ("GET", "/dependencies/external-sources"): "dependencies.read",
    ("GET", "/dependencies/staleness"): "dependencies.read",
    ("POST", "/dependencies/refresh"): "dependencies.refresh",
    ("GET", "/sandbox"): "sandbox.read",
    ("GET", "/schemas/{kind}"): "schemas.read",
    ("GET", "/config-templates/{kind}"): "config_templates.read",
    ("GET", "/files"): "files.tree",
    ("GET", "/files/{path:path}"): "files.read",
    ("PUT", "/files/{path:path}"): "files.write",
    ("DELETE", "/files/{path:path}"): "files.delete",
    ("POST", "/validate/stream"): "runs.validate",
    ("POST", "/generate/stream"): "runs.generate",
    ("GET", "/runs"): "runs.history",
    ("GET", "/runs/{run_id}"): "runs.history",
    ("GET", "/assistant/status"): "assistant.status",
    ("POST", "/assistant/chat"): "assistant.chat",
    ("POST", "/assistant/approval"): "assistant.control",
    ("POST", "/assistant/interrupt"): "assistant.control",
    ("POST", "/assistant/daemon/start"): "assistant.daemon",
    ("GET", "/assistant/config"): "assistant.config_read",
    ("GET", "/assistant/pricing"): "assistant.config_read",
    ("GET", "/assistant/permissions"): "assistant.config_read",
    ("GET", "/assistant/databricks-profiles"): "assistant.config_read",
    ("PUT", "/assistant/config"): "assistant.config_write",
    ("PUT", "/assistant/pricing"): "assistant.config_write",
    ("PUT", "/assistant/permissions"): "assistant.config_write",
    ("POST", "/assistant/skill"): "assistant.skill_install",
    ("GET", "/assistant/session"): "assistant.sessions_read",
    ("GET", "/assistant/sessions"): "assistant.sessions_read",
    ("POST", "/assistant/session/new"): "assistant.sessions_write",
    ("POST", "/assistant/session/archive"): "assistant.sessions_write",
}

#: Ordered ``(METHOD, template prefix, family)`` rules consulted after the
#: explicit rows, so a sub-route added under one of these trees inherits the
#: tree's family instead of surfacing as ``other``.
PREFIX_FAMILIES: tuple[tuple[str, str, str], ...] = (
    ("GET", "/files/", "files.read"),
    ("PUT", "/files/", "files.write"),
    ("DELETE", "/files/", "files.delete"),
    ("GET", "/assistant/session", "assistant.sessions_read"),
    ("POST", "/assistant/session/", "assistant.sessions_write"),
    ("GET", "/assistant/config", "assistant.config_read"),
    ("PUT", "/assistant/config", "assistant.config_write"),
)


def route_family(method: str, template: str) -> Optional[str]:
    """The family ``method`` on ``template`` counts under; ``None`` to skip.

    ``template`` is a route template such as ``/pipelines/{name}``, in either
    spelling the module docstring describes. Skipped templates and methods
    outside :data:`COUNTED_METHODS` give ``None``; a template the table does
    not know gives :data:`OTHER_FAMILY`, so an unclassified route shows up in
    the data instead of vanishing.
    """
    if template.startswith(_API_PREFIX + "/"):
        template = template[len(_API_PREFIX) :]
    if method not in COUNTED_METHODS or template in SKIPPED_TEMPLATES:
        return None
    family = ROUTE_FAMILIES.get((method, template))
    if family is not None:
        return family
    for rule_method, prefix, rule_family in PREFIX_FAMILIES:
        if method == rule_method and template.startswith(prefix):
            return rule_family
    return OTHER_FAMILY


class TelemetrySessionMiddleware(BaseHTTPMiddleware):
    """Count each matched request on its tab's session by route family."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        try:
            return await call_next(request)
        finally:
            # A handler that crashed still served a request the tab made;
            # counting here keeps the tally status-agnostic.
            _observe(request)


def _observe(request: Request) -> None:
    """Count the request and run the sweep when due; never raises."""
    try:
        route = request.scope.get("route")
        if isinstance(route, APIRoute):
            _count(request, route_family(request.method, route.path))
        _maybe_sweep(request.app)
    except Exception:  # telemetry never reaches the response that triggered it
        logger.debug("telemetry: request accounting failed", exc_info=True)


def _count(request: Request, family: Optional[str]) -> None:
    """Attribute one request to its session; a skipped route touches nothing.

    The family is resolved before the session is looked up so a skipped
    route — the health poll above all — never creates or refreshes a record
    and cannot keep an abandoned tab from going idle.
    """
    if family is None:
        return
    attributed = session_for(request)
    if attributed is None:
        return
    registry, sid = attributed
    registry.count_request(sid, family)


def _maybe_sweep(app: FastAPI) -> None:
    """Hand an idle sweep to a worker thread at most every sweep interval.

    Fire-and-forget: the request never waits for the sweep, and the sweep
    logs its own failures, so no exception can surface from the future.
    """
    state = app.state
    if not getattr(state, "telemetry_enabled", False):
        return
    registry: Optional[WebSessionRegistry] = getattr(state, "web_sessions", None)
    if registry is None:
        return
    now = time.monotonic()
    last: Optional[float] = getattr(state, "telemetry_last_sweep", None)
    if last is not None and now - last < telemetry_sessions.SWEEP_INTERVAL_SECONDS:
        return
    state.telemetry_last_sweep = now
    asyncio.get_running_loop().run_in_executor(None, _sweep_quietly, registry)


def _sweep_quietly(registry: WebSessionRegistry) -> None:
    try:
        registry.sweep_idle()
    except Exception:  # runs detached on a worker thread; nothing can catch it later
        logger.debug("telemetry: idle sweep failed", exc_info=True)

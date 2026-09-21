"""UI-surface event sink for the web IDE's anonymous telemetry.

``POST /api/telemetry/ui`` is the one endpoint the SPA's telemetry client
posts to. It accepts a batch of ``{surface, action, via?}`` observations and
turns each recognised one into a ``surface.action[.via]`` counter on the
tab's session; nothing about the batch is stored, echoed or logged.

Three properties are load-bearing:

- It always answers ``204`` with an empty body once the request is
  structurally valid. An unrecognised surface, action or ``via`` drops that
  one event, so a browser bundle newer or older than the server never sees a
  failed post, and a telemetry failure never becomes a user-visible error.
- It is NOT gated on a loaded project: the init wizard runs in
  ``no_project`` state and reports from there. Nothing here reads or writes
  the project tree, so that state stays write-free.
- With telemetry off it returns before touching the session registry, so a
  disabled process never builds session state it would then have to discard.

The route holds no reference to :mod:`lhp.telemetry`; it goes through the
registry on ``app.state`` like every other web hook. Its template is in the
middleware's skipped set, so posting events does not itself count as a
request and cannot keep an abandoned tab from going idle.
"""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Request, Response

from lhp.webapp.schemas.telemetry import (
    UI_ACTIONS,
    UI_SURFACES,
    UI_VIA,
    UiEvent,
    UiEventsRequest,
)
from lhp.webapp.services._telemetry_events import session_for, session_id_from
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry

router = APIRouter(prefix="/telemetry", tags=["telemetry"])


@router.post("/ui", status_code=204, response_class=Response)
async def record_ui_events(body: UiEventsRequest, request: Request) -> Response:
    """Count a batch of UI-surface observations on the posting tab's session.

    Deliberately silent: there is no logger in this module, so no posted
    value can reach a log line even at DEBUG level.
    """
    if getattr(request.app.state, "telemetry_enabled", False):
        _count_batch(request, body)
    return Response(status_code=204)


def _count_batch(request: Request, body: UiEventsRequest) -> None:
    """Count every recognised event; a batch with none touches no session.

    The labels are built before a session is looked up so that a batch of
    purely unrecognised events cannot create a session record.
    """
    labels = [label for event in body.events if (label := _label(event)) is not None]
    if not labels:
        return
    attributed = _attribute(request, body.session_id)
    if attributed is None:
        return
    registry, sid = attributed
    for label in labels:
        registry.count_ui(sid, label)


def _label(event: UiEvent) -> Optional[str]:
    """``surface.action[.via]`` for a recognised event, else ``None``.

    Every component is checked against its closed set, so the label is
    assembled only from values this module declares. ``via`` says how
    something was created, so it qualifies a ``created`` action only.
    """
    if event.surface not in UI_SURFACES or event.action not in UI_ACTIONS:
        return None
    if event.via is not None and event.action != "created":
        return None
    if event.via is None:
        return f"{event.surface}.{event.action}"
    if event.via not in UI_VIA:
        return None
    return f"{event.surface}.{event.action}.{event.via}"


def _attribute(
    request: Request, body_session_id: str
) -> Optional[tuple[WebSessionRegistry, str]]:
    """The registry and session id to count against, or ``None`` for neither.

    The request's own id wins whenever it carries a well-formed one, which
    keeps this route's attribution identical to every other hook's; the
    body's id is consulted only when it does not, so a header the registry
    refuses is never silently re-attributed to a different session.
    """
    if session_id_from(request) is not None:
        return session_for(request)
    registry: Optional[WebSessionRegistry] = getattr(
        request.app.state, "web_sessions", None
    )
    if registry is None or not registry.touch(body_session_id):
        return None
    return registry, body_session_id

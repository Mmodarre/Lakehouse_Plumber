"""Per-session token guard for the LHP web IDE backend.

The ``lhp web`` command mints an unguessable token, exports it via
``LHP_WEBAPP_TOKEN``, and embeds it in the URL fragment it opens
(``/#token=<value>``); the SPA replays it on every API call. A browser page on
a foreign origin cannot read that fragment, so even if a request slips past
the Host/Origin checks it cannot present the token.

Scope: paths under ``/api/`` except ``/api/health`` (kept open so the SPA can
always render guidance and the launch readiness-poll needs no credential).
Non-``/api`` paths — the SPA shell and its assets — are exempt. When settings
carry no token (tests, direct ``create_app`` use) the guard is a no-op.
"""

import logging
import secrets

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

TOKEN_HEADER = "X-LHP-Token"
TOKEN_QUERY_PARAM = "token"

_API_PREFIX = "/api/"
_EXEMPT_PATH = "/api/health"


def _matches(presented: str, expected: str) -> bool:
    """Constant-time token comparison (bytes form tolerates non-ASCII input)."""
    return secrets.compare_digest(presented.encode("utf-8"), expected.encode("utf-8"))


class TokenGuardMiddleware(BaseHTTPMiddleware):
    """Require the session token on ``/api/`` requests (header or query param)."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        settings = getattr(request.app.state, "settings", None)
        expected = getattr(settings, "token", None)
        if expected is None:
            return await call_next(request)

        path = request.url.path
        if not path.startswith(_API_PREFIX) or path == _EXEMPT_PATH:
            return await call_next(request)

        # Header is the SPA channel; the query param exists for consumers that
        # cannot set headers (e.g. EventSource for the SSE stream).
        for presented in (
            request.headers.get(TOKEN_HEADER),
            request.query_params.get(TOKEN_QUERY_PARAM),
        ):
            if presented is not None and _matches(presented, expected):
                return await call_next(request)

        logger.warning(f"Rejected unauthenticated {request.method} {path}")
        return JSONResponse(
            {"detail": "invalid or missing session token"},
            status_code=401,
        )

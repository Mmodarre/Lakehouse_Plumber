"""Cross-origin write-request guard (CSRF defense) for the LHP web IDE backend.

The local IDE is same-origin only, so any state-changing request that declares
a non-loopback ``Origin`` or ``Referer`` must have been forged by another site
running in the user's browser (a DNS-rebinding or classic CSRF page). Such
requests are rejected with ``403``. Requests without either header (curl,
scripts, same-origin fetches from older browsers) are allowed through, as are
all read-only methods.
"""

import logging
from urllib.parse import urlsplit

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)

_WRITE_METHODS = frozenset({"POST", "PUT", "DELETE", "PATCH"})
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def _is_loopback_url(value: str) -> bool:
    """True if ``value`` parses to a URL whose host is a loopback name.

    Anything unparseable (including the literal ``Origin: null`` sent for
    opaque origins) fails closed.
    """
    try:
        host = urlsplit(value).hostname
    except ValueError:
        return False
    return host in _LOOPBACK_HOSTS


class OriginGuardMiddleware(BaseHTTPMiddleware):
    """Reject POST/PUT/DELETE/PATCH requests with a foreign Origin/Referer."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if request.method in _WRITE_METHODS:
            for header in ("origin", "referer"):
                value = request.headers.get(header)
                if value and not _is_loopback_url(value):
                    logger.warning(
                        f"Rejected cross-origin {request.method} "
                        f"{request.url.path} ({header}={value!r})"
                    )
                    return JSONResponse(
                        {"detail": "cross-origin request rejected"},
                        status_code=403,
                    )
        return await call_next(request)

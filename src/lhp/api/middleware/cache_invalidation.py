import logging

from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from lhp.api.dependencies import invalidate_caches

logger = logging.getLogger(__name__)

MUTATING_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

SKIP_INVALIDATION_PREFIXES = {
    "/api/workspace",
    "/api/health",
}


class CacheInvalidationMiddleware(BaseHTTPMiddleware):
    """Automatically invalidate caches after successful write operations."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        response = await call_next(request)

        if (
            request.method in MUTATING_METHODS
            and 200 <= response.status_code < 300
            and not self._should_skip(request.url.path)
        ):
            self._invalidate(request)

        return response

    def _should_skip(self, path: str) -> bool:
        return any(path.startswith(prefix) for prefix in SKIP_INVALIDATION_PREFIXES)

    def _invalidate(self, request: Request) -> None:
        workspace_root = getattr(request.state, "workspace_project_root", None)
        if workspace_root is not None:
            invalidate_caches(workspace_root)
        else:
            settings = request.app.state.settings
            project_root = settings.project_root.resolve()
            invalidate_caches(project_root)
        logger.debug(f"Cache invalidated after {request.method} {request.url.path}")

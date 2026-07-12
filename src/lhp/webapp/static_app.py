"""SPA static-asset serving for the LHP web IDE backend.

Single responsibility: decide whether a built single-page app is present and,
if so, serve its ``index.html``, its ``assets`` bundle, and any top-level
static files (e.g. ``vite.svg``); otherwise serve a plain-text "not built"
page. A ``404`` handler supplies the client-side-routing fallback for browser
navigations while guaranteeing that unmatched ``/api`` paths and non-HTML
clients always receive a JSON ``404`` rather than the HTML shell.

This module owns all static-serving concerns so that
:mod:`lhp.webapp.app` stays limited to the application factory, router
registry, middleware, and exception-handler wiring.
"""

from __future__ import annotations

import importlib.resources
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.responses import FileResponse, Response
from starlette.staticfiles import StaticFiles

logger = logging.getLogger(__name__)

_API_PREFIX = "/api"

_SPA_NOT_BUILT_MESSAGE = (
    "Lakehouse Plumber web IDE\n"
    "=========================\n\n"
    "The single-page app has not been built, so only the JSON API is being\n"
    "served (try /api/health).\n\n"
    "To build the frontend assets, run:\n\n"
    "    scripts/build_webapp.sh\n\n"
    "then restart the server.\n"
)

#: Unhashed entry points (``index.html``, top-level files) must revalidate on
#: every load: without this, browsers heuristically cache ``index.html``, and
#: a newer build deletes the hashed chunks the cached copy references —
#: serving a stale or broken app. ``FileResponse`` sends ``ETag`` /
#: ``Last-Modified``, so revalidation is a cheap ``304``.
_NO_CACHE = "no-cache"


class _ImmutableStaticFiles(StaticFiles):
    """``/assets`` serving with far-future caching.

    Every file under ``assets/`` carries a content hash in its name, so a
    changed file is a NEW URL — cached copies can never go stale.
    """

    def file_response(self, *args: Any, **kwargs: Any) -> Response:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response


def resolve_static_dir() -> Optional[Path]:
    """Return the built SPA directory if ``static/index.html`` exists, else None.

    Tests monkeypatch this function to force either mode; :func:`mount_spa`
    always calls it rather than inlining the lookup.

    Under a zipped install the resolved path is not a real file, so
    ``is_file()`` fails and the app degrades to "not built" (``StaticFiles``
    cannot serve from a zip anyway).
    """
    static_dir = Path(str(importlib.resources.files("lhp.webapp") / "static"))
    if (static_dir / "index.html").is_file():
        return static_dir
    return None


def _add_static_file_route(app: FastAPI, file_path: Path) -> None:
    """Register a ``GET``/``HEAD`` route serving one top-level static file verbatim."""

    async def _serve() -> FileResponse:
        return FileResponse(file_path, headers={"Cache-Control": _NO_CACHE})

    app.add_api_route(
        f"/{file_path.name}",
        _serve,
        methods=["GET", "HEAD"],
        include_in_schema=False,
    )


async def _spa_not_found_handler(request: Request, exc: Exception) -> Response:
    """Serve ``index.html`` for browser navigations; JSON ``404`` otherwise.

    HTML is returned only for a ``GET`` to a non-``/api`` path from a client
    that accepts ``text/html`` when a built SPA is present (``app.state``
    carries the resolved ``index.html``). Every other case — including any
    unmatched ``/api`` path and any route-raised :class:`HTTPException` — gets a
    JSON body, and a route-raised ``detail`` is preserved verbatim.
    """
    spa_index: Optional[Path] = getattr(request.app.state, "spa_index", None)
    if (
        request.method == "GET"
        and not request.url.path.startswith(_API_PREFIX)
        and "text/html" in request.headers.get("accept", "")
        and spa_index is not None
    ):
        return FileResponse(
            spa_index, media_type="text/html", headers={"Cache-Control": _NO_CACHE}
        )

    detail = "Not Found"
    headers: Optional[Mapping[str, str]] = None
    if isinstance(exc, StarletteHTTPException):
        if exc.detail:
            detail = exc.detail
        headers = exc.headers
    return JSONResponse({"detail": detail}, status_code=404, headers=headers)


def mount_spa(app: FastAPI) -> None:
    """Wire SPA serving onto ``app``; call after the API routers are registered.

    Resolves the static directory once and stashes the resolved ``index.html``
    path (or ``None``) on ``app.state.spa_index`` for the ``404`` fallback.
    """
    static_dir = resolve_static_dir()
    app.state.spa_index = (static_dir / "index.html") if static_dir else None

    if static_dir is not None:
        index_html = static_dir / "index.html"
        assets_dir = static_dir / "assets"
        if assets_dir.is_dir():
            app.mount(
                "/assets",
                _ImmutableStaticFiles(directory=str(assets_dir)),
                name="assets",
            )

        async def _serve_index() -> FileResponse:
            return FileResponse(index_html, headers={"Cache-Control": _NO_CACHE})

        app.add_api_route(
            "/", _serve_index, methods=["GET", "HEAD"], include_in_schema=False
        )

        for entry in sorted(static_dir.iterdir()):
            if (
                entry.name.startswith(".")
                or not entry.is_file()
                or entry.name == "index.html"
            ):
                continue
            _add_static_file_route(app, entry)

        logger.info(f"Serving SPA from {static_dir}")
    else:
        logger.warning(
            "SPA static assets not found — serving API only. "
            "Run scripts/build_webapp.sh to build the frontend."
        )

        async def _spa_not_built() -> PlainTextResponse:
            return PlainTextResponse(_SPA_NOT_BUILT_MESSAGE)

        app.add_api_route(
            "/", _spa_not_built, methods=["GET", "HEAD"], include_in_schema=False
        )

    app.add_exception_handler(404, _spa_not_found_handler)

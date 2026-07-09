"""Shared request guards for webapp routers.

One implementation of the ``no_project`` gate (the lifespan only runs DB
migrations for a real project, so store-backed routers must refuse in that
state) — previously copied per router.
"""

from __future__ import annotations

from fastapi import HTTPException, Request


def assert_project_loaded(request: Request, unavailable: str) -> None:
    """409 when the server runs in ``no_project`` state.

    ``unavailable`` names the refused resource for the error message, e.g.
    ``"the assistant is unavailable"`` / ``"run history is unavailable"``.
    """
    if getattr(request.app.state, "project_state", "ok") != "ok":
        raise HTTPException(409, f"No LHP project loaded; {unavailable}")

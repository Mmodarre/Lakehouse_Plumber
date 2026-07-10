"""Packaged config-template router for the LHP web IDE backend.

Serves the environment config templates that ship as package data inside the
``lhp.templates.init`` package (``config/<kind>_env.yaml.tmpl``). The
frontend's "create from template" dialog consumes the response body *directly*
as the initial file content, so the raw template text is returned as
``text/plain`` with no envelope.

``lhp.templates`` is accessed as package DATA via ``importlib.resources`` on
the package-name string — never imported as a module — so this router stays
within the ``lhp.webapp`` boundary contract (it imports only stdlib and
fastapi).
"""

from __future__ import annotations

import re
from importlib.resources import files

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse

router = APIRouter(prefix="/config-templates", tags=["config-templates"])

# A template ``kind`` is a bare lowercase identifier. Validating against this
# pattern BEFORE building any path blocks traversal (``..``, ``/``, ``%2F``,
# etc.) — only ``[a-z_]`` characters can ever reach the filesystem lookup.
_KIND_PATTERN = re.compile(r"^[a-z_]+$")

# The only kinds served — one per packaged ``<kind>_env.yaml.tmpl`` file.
# Allow-listing (rather than probing the package for arbitrary names) keeps
# the endpoint's surface exactly the three config templates the UI offers.
_ALLOWED_KINDS = frozenset({"pipeline_config", "job_config", "monitoring_job_config"})

# Package whose data files (``config/<kind>_env.yaml.tmpl``) are served.
# Passed as a STRING to importlib.resources — NOT imported as a module
# (boundary §5.8).
_TEMPLATES_PACKAGE = "lhp.templates.init"


@router.get("/{kind}", response_class=PlainTextResponse)
def get_config_template(kind: str) -> str:
    """Return the raw packaged ``config/<kind>_env.yaml.tmpl`` template text.

    The body is the template file itself (``text/plain``, no envelope),
    suitable for direct use as initial file content by the frontend.

    Raises:
        HTTPException: 404 if ``kind`` is malformed, not allow-listed, or the
            packaged resource is missing.
    """
    if not _KIND_PATTERN.fullmatch(kind) or kind not in _ALLOWED_KINDS:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown config template kind: {kind!r}",
        )

    resource = files(_TEMPLATES_PACKAGE) / "config" / f"{kind}_env.yaml.tmpl"
    if not resource.is_file():
        raise HTTPException(
            status_code=404,
            detail=f"Unknown config template kind: {kind!r}",
        )

    return resource.read_text(encoding="utf-8")

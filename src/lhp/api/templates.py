"""Public read-only authoring operations for reusable flowgroup templates."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def template_catalog(project_root: Path) -> dict[str, Any]:
    """List path identities, preserving malformed/non-invocable sources for repair.

    :stability: provisional
    """
    from lhp.core.processing.template_authoring import catalog

    return catalog(Path(project_root).resolve())


def template_source(project_root: Path, *, path: str) -> dict[str, Any]:
    """Inspect one source path without relying on a possibly duplicate declared name.

    :stability: provisional
    """
    from lhp.core.processing.template_authoring import project_path, source_entry

    root = Path(project_root).resolve()
    candidate = project_path(root, path, template=True)
    if not candidate.is_file():
        raise FileNotFoundError(f"Template file not found: {path}")
    return {"template": source_entry(root, path)}


def preview_template(project_root: Path, *, request: dict[str, Any]) -> dict[str, Any]:
    """Inspect, expand or resolve a supplied draft; never write or generate files.

    All saved dependencies use request-local services. Rendering runs in a child
    process with a 10-second deadline and a 2 MiB output cap. Project-relative
    identities and indirect source reads remain containment checked.

    :stability: provisional
    """
    from lhp.api._template_preview_worker import bounded_preview
    from lhp.core.processing.template_authoring import MAX_SOURCE_BYTES, project_path

    root = Path(project_root).resolve()
    project_path(root, request["source_path"], template=True)
    if request.get("stage") not in {"inspect", "expanded", "resolved"}:
        raise ValueError("Preview stage must be inspect, expanded or resolved.")
    if not isinstance(request.get("source_yaml"), str):
        raise TypeError("Template source must be a string.")
    encoded = json.dumps(request, allow_nan=False).encode("utf-8")
    if (
        len(request["source_yaml"].encode("utf-8")) > MAX_SOURCE_BYTES
        or len(encoded) > 2 * MAX_SOURCE_BYTES
    ):
        raise ValueError("Preview request exceeds the 1 MiB limit (512 KiB source).")
    return bounded_preview(root, request)

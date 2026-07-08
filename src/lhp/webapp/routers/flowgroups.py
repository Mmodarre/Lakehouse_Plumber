"""Read-only flowgroup endpoints for the LHP local web IDE backend.

Four read operations the local IDE needs:

- ``GET /api/flowgroups`` — list (optionally filtered by ``?pipeline=``).
- ``GET /api/flowgroups/{name}`` — processed detail plus source-file path.
- ``GET /api/flowgroups/{name}/resolved`` — processed flowgroup for an env.
- ``GET /api/flowgroups/{name}/related-files`` — external file references.

There are no create / update / delete endpoints and no auth ``Depends``: this
server is same-origin, single-user, read-only over one local project.

Per the ``webapp-uses-public-api`` import contract, this module imports only
:mod:`lhp.api` / :mod:`lhp.errors` from the ``lhp`` package (plus ``yaml`` for
the raw flowgroup load that feeds the related-files extractor, and the webapp's
own DI / schema / service modules).
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import InspectionFacade
from lhp.api.views import FlowgroupView
from lhp.webapp.dependencies import get_inspection, get_project_root
from lhp.webapp.schemas.flowgroup import (
    FlowgroupDetailResponse,
    FlowgroupListResponse,
    FlowgroupRelatedFilesResponse,
    RelatedFileInfo,
    ResolvedFlowgroupResponse,
)
from lhp.webapp.services.related_files_extractor import extract_related_files

router = APIRouter(prefix="/flowgroups", tags=["flowgroups"])


def _relative_source(file_path: Optional[Path], project_root: Path) -> str:
    """Return ``file_path`` relative to ``project_root``, or ``""`` if unknown.

    ``file_path`` is the absolute source-YAML path carried on a
    :class:`FlowgroupView`; the frontend's files API expects project-relative
    paths. A path outside the project root (or a missing path) yields ``""``.
    """
    if file_path is None:
        return ""
    try:
        return str(file_path.relative_to(project_root))
    except ValueError:
        return ""


def _find_view(inspection: InspectionFacade, name: str) -> FlowgroupView:
    """Locate the named flowgroup's view or raise 404.

    Centralises the unknown-name handling (404 on a name miss) for every endpoint.
    """
    for view in inspection.list_flowgroups():
        if view.name == name:
            return view
    raise HTTPException(status_code=404, detail=f"Flowgroup '{name}' not found")


@router.get("", response_model=FlowgroupListResponse)
def list_flowgroups(
    pipeline: Optional[str] = Query(None, description="Filter by pipeline name"),
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> FlowgroupListResponse:
    """List all flowgroups, optionally filtered by pipeline name."""
    views = inspection.list_flowgroups(pipeline_filter=pipeline)
    pairs = [(v, _relative_source(v.file_path, project_root)) for v in views]
    return FlowgroupListResponse.from_views(pairs)


@router.get(
    "/{name}",
    response_model=FlowgroupDetailResponse,
    response_model_exclude_none=True,
)
def get_flowgroup(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> FlowgroupDetailResponse:
    """Get a flowgroup's processed config plus its source-file path.

    Resolution defaults to the ``dev`` environment; pass ``?env=`` to resolve
    substitutions against another environment (matching ``/resolved``).
    """
    view = _find_view(inspection, name)
    processed = inspection.process_flowgroup(name, env=env)
    return FlowgroupDetailResponse.from_view(
        processed, source_file=_relative_source(view.file_path, project_root)
    )


@router.get(
    "/{name}/resolved",
    response_model=ResolvedFlowgroupResponse,
    response_model_exclude_none=True,
)
def get_resolved_flowgroup(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    inspection: InspectionFacade = Depends(get_inspection),
) -> ResolvedFlowgroupResponse:
    """Get a flowgroup after template expansion, preset merge, and substitution.

    Same processing pipeline as ``lhp show <flowgroup> --env <env>``.
    """
    _find_view(inspection, name)
    processed = inspection.process_flowgroup(name, env=env)
    return ResolvedFlowgroupResponse.from_view(processed, environment=env)


@router.get(
    "/{name}/related-files",
    response_model=FlowgroupRelatedFilesResponse,
)
def get_related_files(
    name: str,
    env: str = Query("dev", description="Environment for substitution resolution"),
    inspection: InspectionFacade = Depends(get_inspection),
    project_root: Path = Depends(get_project_root),
) -> FlowgroupRelatedFilesResponse:
    """Get all files referenced by a flowgroup (SQL, Python, schema, expectations).

    Locates the flowgroup's source YAML, loads it raw, and walks the known
    file-path-bearing fields via the related-files extractor. The ``env``
    query param is accepted for parity with the old endpoint but does not
    change the raw-YAML walk (template-side refs are a documented v1 deferral
    in the extractor).
    """
    view = _find_view(inspection, name)
    source_path = view.file_path
    source_rel = _relative_source(source_path, project_root)

    related = []
    if source_path is not None and source_path.is_file():
        raw = yaml.safe_load(source_path.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            related = extract_related_files(raw, project_root)

    source_info = RelatedFileInfo(
        path=source_rel,
        category="yaml",
        action_name="",
        field="source_file",
        exists=bool(source_path is not None and source_path.exists()),
    )

    return FlowgroupRelatedFilesResponse(
        flowgroup=name,
        source_file=source_info,
        related_files=[RelatedFileInfo.from_related_file(rf) for rf in related],
        environment=env,
    )

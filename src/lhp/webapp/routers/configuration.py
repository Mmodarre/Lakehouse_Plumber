"""Read-only configuration preview; all resolution stays behind the public API."""

from pathlib import Path
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query

from lhp.api import preview_configuration
from lhp.webapp.dependencies import get_project_root
from lhp.webapp.schemas.configuration import ConfigurationPreviewResponse

router = APIRouter(prefix="/configuration", tags=["configuration"])


@router.get("/preview", response_model=ConfigurationPreviewResponse)
def get_configuration_preview(
    path: str = Query(min_length=1),
    kind: Literal["pipeline", "job"] = Query(),
    env: str = Query(min_length=1),
    target: str = Query(default=""),
    project_root: Path = Depends(get_project_root),
) -> ConfigurationPreviewResponse:
    try:
        return ConfigurationPreviewResponse(
            **preview_configuration(
                project_root, path=path, kind=kind, env=env, target=target
            )
        )
    except PermissionError as exc:
        raise HTTPException(status_code=403, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except (ValueError, TypeError, AttributeError) as exc:
        raise HTTPException(
            status_code=422, detail=f"Could not resolve saved configuration: {exc}"
        ) from exc

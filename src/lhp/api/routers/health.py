import platform
import sys
from importlib.metadata import version as pkg_version

from fastapi import APIRouter, Depends

from lhp.api.auth import UserContext, get_current_user
from lhp.api.schemas.health import HealthResponse, UserResponse, VersionResponse

router = APIRouter(tags=["health"])


@router.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """Health check endpoint. No auth required."""
    return HealthResponse(
        version=pkg_version("lakehouse-plumber"),
        python_version=platform.python_version(),
    )


@router.get("/version", response_model=VersionResponse)
async def get_version() -> VersionResponse:
    """Detailed version information including dependency versions."""
    deps = {}
    for pkg in ["fastapi", "uvicorn", "pydantic", "networkx", "pyyaml", "jinja2", "click"]:
        try:
            deps[pkg] = pkg_version(pkg)
        except Exception:
            deps[pkg] = "unknown"

    return VersionResponse(
        lhp_version=pkg_version("lakehouse-plumber"),
        python_version=platform.python_version(),
        dependencies=deps,
    )


@router.get("/me", response_model=UserResponse)
async def get_me(user: UserContext = Depends(get_current_user)) -> UserResponse:
    """Current authenticated user information."""
    return UserResponse(
        email=user.email,
        username=user.username,
        user_id=user.user_id,
    )

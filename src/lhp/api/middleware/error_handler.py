import logging
from typing import Dict, Type

from fastapi import Request
from fastapi.responses import JSONResponse

from lhp.utils.error_formatter import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
)

logger = logging.getLogger(__name__)

# Map LHPError subtypes to HTTP status codes
# Follows the mapping from docs_old/prompt.md
ERROR_STATUS_MAP: Dict[Type[LHPError], int] = {
    LHPConfigError: 422,
    LHPValidationError: 422,
    LHPFileError: 404,
}

# Map ErrorCategory to HTTP status codes (fallback)
CATEGORY_STATUS_MAP: Dict[ErrorCategory, int] = {
    ErrorCategory.VALIDATION: 422,
    ErrorCategory.CONFIG: 422,
    ErrorCategory.IO: 404,
    ErrorCategory.DEPENDENCY: 422,
    ErrorCategory.ACTION: 422,
    ErrorCategory.CLOUDFILES: 422,
    ErrorCategory.GENERAL: 500,
}


def _get_http_status(error: LHPError) -> int:
    """Determine HTTP status code for an LHPError."""
    # Check specific error type first
    for error_type, status in ERROR_STATUS_MAP.items():
        if isinstance(error, error_type):
            return status
    # Fall back to category-based mapping
    return CATEGORY_STATUS_MAP.get(error.category, 500)


async def lhp_error_handler(request: Request, exc: LHPError) -> JSONResponse:
    """Convert LHPError to structured JSON error response."""
    status_code = _get_http_status(exc)

    logger.warning(f"LHP error [{exc.code}]: {exc.title} (HTTP {status_code})")

    return JSONResponse(
        status_code=status_code,
        content={
            "error": {
                "code": exc.code,
                "category": exc.category.value,
                "message": exc.title,
                "details": exc.details,
                "suggestions": exc.suggestions,
                "context": exc.context,
                "http_status": status_code,
            }
        },
    )


async def generic_error_handler(request: Request, exc: Exception) -> JSONResponse:
    """Catch-all for unhandled exceptions.

    In dev_mode, the raw exception message is included in the response.
    In production, only a generic "Internal server error" is returned
    to avoid leaking internal details (file paths, SQL, stack traces).
    """
    logger.exception(f"Unhandled error: {exc}")

    # Gate internal details by dev_mode
    settings = getattr(request.app.state, "settings", None)
    is_dev = getattr(settings, "dev_mode", False) if settings else False

    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "LHP-GEN-000",
                "category": "GENERAL",
                "message": "Internal server error",
                "details": str(exc) if is_dev else "An unexpected error occurred.",
                "suggestions": (
                    ["Check server logs for details"]
                    if not is_dev
                    else []
                ),
                "context": {},
                "http_status": 500,
            }
        },
    )

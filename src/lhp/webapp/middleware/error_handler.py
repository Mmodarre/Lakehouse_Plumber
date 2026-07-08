"""FastAPI exception handlers for the LHP web IDE backend.

Maps domain ``LHPError`` exceptions to structured JSON error responses with
appropriate HTTP status codes, and provides a catch-all handler that always
redacts internal details (they go to the server log instead).
"""

import logging
from typing import Dict, Type

from fastapi import Request
from fastapi.responses import JSONResponse

from lhp.errors import (
    ErrorCategory,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
)

logger = logging.getLogger(__name__)

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

    The response is always redacted to a generic "Internal server error" so no
    internal detail (file paths, SQL, stack traces) can leak to the client; the
    full exception with traceback goes to the server log via
    ``logger.exception``.
    """
    logger.exception("Unhandled error")

    return JSONResponse(
        status_code=500,
        content={
            "error": {
                "code": "LHP-GEN-000",
                "category": "GENERAL",
                "message": "Internal server error",
                "details": "An unexpected error occurred.",
                "suggestions": ["Check server logs for details"],
                "context": {},
                "http_status": 500,
            }
        },
    )

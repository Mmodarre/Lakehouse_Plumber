"""Domain error types and text formatting for LHP.

No Rich/colorama/click dependencies — Rich rendering lives in
``lhp.cli.error_panel.render_error_panel``.
"""

from . import codes
from .categories import ErrorCategory
from .codes import ErrorCode
from .factory import ErrorFactory
from .types import (
    BundleConfigurationError,
    BundleResourceError,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
    MultiDocumentError,
    PythonFunctionConflictError,
    TemplateError,
    YAMLProcessingError,
    lhp_error_from_worker_failure,
)

__all__ = [
    "BundleConfigurationError",
    "BundleResourceError",
    "ErrorCategory",
    "ErrorCode",
    "ErrorFactory",
    "codes",
    "LHPConfigError",
    "LHPError",
    "LHPFileError",
    "LHPValidationError",
    "MultiDocumentError",
    "PythonFunctionConflictError",
    "TemplateError",
    "YAMLProcessingError",
    "lhp_error_from_worker_failure",
]

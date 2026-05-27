"""Domain error types and text formatting for LHP.

No Rich/colorama/click dependencies — Rich rendering lives in
``lhp.cli.error_panel.render_error_panel``.
"""
from .categories import ErrorCategory
from .formatter import ErrorFormatter
from .types import (
    BundleConfigurationError,
    BundleResourceError,
    LHPConfigError,
    LHPError,
    LHPFileError,
    LHPValidationError,
    MultiDocumentError,
    TemplateError,
    YAMLProcessingError,
    lhp_error_from_worker_failure,
)

__all__ = [
    "BundleConfigurationError",
    "BundleResourceError",
    "ErrorCategory",
    "ErrorFormatter",
    "LHPConfigError",
    "LHPError",
    "LHPFileError",
    "LHPValidationError",
    "MultiDocumentError",
    "TemplateError",
    "YAMLProcessingError",
    "lhp_error_from_worker_failure",
]

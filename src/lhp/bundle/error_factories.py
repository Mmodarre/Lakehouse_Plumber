"""Factory functions to convert bundle exceptions to LHPError."""

import logging
from typing import List, Optional

from ..utils.error_formatter import ErrorCategory, LHPError

logger = logging.getLogger(__name__)


def create_bundle_resource_error(
    operation: str,
    details: str,
    file_path: Optional[str] = None,
    suggestions: Optional[List[str]] = None,
    original_error: Optional[Exception] = None,
) -> LHPError:
    """Create an LHPError for bundle resource operation failures."""
    context = {"Operation": operation}
    if file_path:
        context["File"] = file_path
    if original_error:
        context["Original Error"] = type(original_error).__name__

    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="020",
        title=f"Bundle resource error during {operation}",
        details=details,
        suggestions=suggestions
        or [
            "Check your databricks.yml configuration",
            "Verify bundle resource files are valid YAML",
            "Run 'lhp validate' to check configuration",
        ],
        context=context,
    )


def create_yaml_processing_error(
    file_path: str,
    message: str,
    line_number: Optional[int] = None,
    context: Optional[str] = None,
) -> LHPError:
    """Create an LHPError for YAML processing failures in bundle files."""
    error_context = {"File": file_path}
    if line_number:
        error_context["Line"] = str(line_number)

    details = f"Failed to process YAML in '{file_path}': {message}"
    if context:
        details += f"\nContext: {context}"

    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="021",
        title="Bundle YAML processing error",
        details=details,
        suggestions=[
            "Check YAML syntax (indentation, colons, dashes)",
            "Validate the file with a YAML linter",
            "Ensure the file is not corrupted or truncated",
        ],
        context=error_context,
    )


def create_template_error(
    operation: str,
    details: str,
    original_error: Optional[Exception] = None,
) -> LHPError:
    """Create an LHPError for template fetch/processing failures."""
    context = {"Operation": operation}
    if original_error:
        context["Original Error"] = type(original_error).__name__

    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="024",
        title=f"Bundle template error during {operation}",
        details=details,
        suggestions=[
            "Check your internet connection if downloading templates",
            "Verify the template path or URL is correct",
            "Try running the command again",
        ],
        context=context,
    )


def create_bundle_config_error(
    message: str,
    file_path: Optional[str] = None,
    suggestions: Optional[List[str]] = None,
) -> LHPError:
    """Create an LHPError for bundle configuration errors."""
    context = {}
    if file_path:
        context["File"] = file_path

    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="025",
        title="Bundle configuration error",
        details=message,
        suggestions=suggestions
        or [
            "Review your databricks.yml configuration",
            "Check the Databricks Asset Bundles documentation",
            "Run 'lhp validate' to check for configuration issues",
        ],
        context=context,
    )


def convert_bundle_error(
    error: Exception, operation: str = "bundle operation"
) -> LHPError:
    """Convert any bundle exception to an appropriate LHPError.

    This is the main entry point for the CLI error boundary to convert
    BundleResourceError and its subclasses to LHPError.
    """
    from ..bundle.exceptions import (
        BundleConfigurationError,
        BundleResourceError,
        TemplateError,
        YAMLProcessingError,
    )

    error_message = str(error)

    if isinstance(error, YAMLProcessingError):
        return create_yaml_processing_error(
            file_path=getattr(error, "file_path", "unknown"),
            message=error_message,
            line_number=getattr(error, "line_number", None),
            context=getattr(error, "context", None),
        )

    if isinstance(error, BundleConfigurationError):
        return create_bundle_config_error(
            message=error_message,
            suggestions=[
                "Check your databricks.yml structure",
                "Verify all required fields are present",
                "Run 'lhp validate' for detailed diagnostics",
            ],
        )

    if isinstance(error, TemplateError):
        return create_template_error(
            operation=operation,
            details=error_message,
            original_error=getattr(error, "original_error", None),
        )

    if isinstance(error, BundleResourceError):
        return create_bundle_resource_error(
            operation=operation,
            details=error_message,
            original_error=getattr(error, "original_error", None),
        )

    # Fallback for unknown bundle-related errors
    return create_bundle_resource_error(
        operation=operation,
        details=error_message,
        original_error=error,
    )

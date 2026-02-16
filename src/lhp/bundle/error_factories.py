"""Factory functions to convert bundle exceptions to LHPError."""

import logging
from typing import List, Optional

from ..utils.error_formatter import ErrorCategory, LHPError, LHPFileError

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


def create_missing_databricks_file_error(file_path: str) -> LHPFileError:
    """Create an LHPFileError for missing databricks.yml.

    Returns LHPFileError (which inherits from both LHPError and
    FileNotFoundError) so that existing ``except FileNotFoundError``
    handlers continue to catch it.
    """
    return LHPFileError(
        category=ErrorCategory.CONFIG,
        code_number="022",
        title="Missing databricks.yml",
        details=(
            f"Expected Databricks bundle configuration at '{file_path}' "
            f"but the file does not exist."
        ),
        suggestions=[
            "Run 'lhp init <project_name>' to create a new project "
            "with bundle support",
            "Create a databricks.yml file manually in your project root",
            "Use --no-bundle flag to skip bundle operations",
        ],
        context={"Expected Path": file_path},
    )


def create_missing_target_error(
    missing_targets: List[str],
    available_targets: List[str],
    file_path: str,
) -> LHPError:
    """Create an LHPError for missing Databricks targets."""
    missing_list = ", ".join(f"'{t}'" for t in missing_targets)
    available_list = (
        ", ".join(f"'{t}'" for t in available_targets)
        if available_targets
        else "none found"
    )

    return LHPError(
        category=ErrorCategory.CONFIG,
        code_number="023",
        title="Missing Databricks bundle targets",
        details=(
            f"Substitution files exist for targets {missing_list}, "
            f"but these targets are not defined in '{file_path}'.\n"
            f"Available targets: {available_list}"
        ),
        suggestions=[
            "Add the missing targets to your databricks.yml file",
            "Ensure target names match your substitution file names "
            "(e.g., substitutions/dev.yaml -> targets.dev)",
            "Remove unused substitution files if they are not needed",
        ],
        context={
            "Missing Targets": missing_list,
            "Available Targets": available_list,
            "File": file_path,
        },
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
        MissingDatabricksTargetError,
        TemplateError,
        YAMLProcessingError,
    )

    error_message = str(error)

    if isinstance(error, MissingDatabricksTargetError):
        return create_bundle_resource_error(
            operation="target resolution",
            details=error_message,
            suggestions=[
                "Add the missing targets to your databricks.yml",
                "Ensure target names match substitution file names",
                "Use --no-bundle to skip bundle operations",
            ],
            original_error=error,
        )

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

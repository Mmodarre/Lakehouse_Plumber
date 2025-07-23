"""Centralized error handling for LakehousePlumber."""

from __future__ import annotations

import logging
import sys
from typing import Optional, Dict, Any, List
from pathlib import Path
from enum import Enum
import click
import shutil

from .error_formatter import LHPError, ErrorCategory, ErrorFormatter


class ProgressStatus(Enum):
    """Status indicators for progress display."""

    SUCCESS = "✅"
    FAILED = "❌"
    IN_PROGRESS = "🔧"
    SKIPPED = "⏭️"
    WARNING = "⚠️"


class ProgressFormatter:
    """Utility for formatting progress indication."""

    @staticmethod
    def format_pipeline_progress(
        pipeline_name: str, status: ProgressStatus, details: str = None
    ) -> str:
        """Format pipeline progress line."""
        base = f"{status.value} {pipeline_name}"
        if details:
            return f"{base} - {details}"
        return base

    @staticmethod
    def format_flowgroup_progress(
        flowgroup_name: str, status: ProgressStatus, indent: int = 1
    ) -> str:
        """Format flowgroup progress line with tree structure."""
        prefix = "   " * indent + "├─ "
        return f"{prefix}{flowgroup_name} {status.value}"

    @staticmethod
    def format_final_flowgroup(
        flowgroup_name: str, status: ProgressStatus, details: str = None
    ) -> str:
        """Format final flowgroup in tree with └─ connector."""
        prefix = "   └─ "
        base = f"{prefix}{flowgroup_name} {status.value}"
        if details:
            return f"{base} - {details}"
        return base


class ErrorContext:
    """Context information for error handling."""

    def __init__(self):
        self.pipeline: Optional[str] = None
        self.environment: Optional[str] = None
        self.flowgroup: Optional[str] = None
        self.action: Optional[str] = None
        self.file_path: Optional[str] = None
        self.extra: Dict[str, Any] = {}

    def set_pipeline_context(self, pipeline: str, environment: str) -> ErrorContext:
        """Set pipeline and environment context."""
        self.pipeline = pipeline
        self.environment = environment
        return self

    def set_flowgroup_context(self, flowgroup: str) -> ErrorContext:
        """Set flowgroup context."""
        self.flowgroup = flowgroup
        return self

    def set_action_context(self, action: str) -> ErrorContext:
        """Set action context."""
        self.action = action
        return self

    def set_file_context(self, file_path: str) -> ErrorContext:
        """Set file context."""
        self.file_path = file_path
        return self

    def add_extra(self, key: str, value: Any) -> ErrorContext:
        """Add extra context information."""
        self.extra[key] = value
        return self

    def to_dict(self) -> Dict[str, Any]:
        """Convert context to dictionary for error formatting."""
        context = {}
        if self.pipeline:
            context["Pipeline"] = self.pipeline
        if self.environment:
            context["Environment"] = self.environment
        if self.flowgroup:
            context["FlowGroup"] = self.flowgroup
        if self.action:
            context["Action"] = self.action
        if self.file_path:
            context["File"] = self.file_path
        context.update(self.extra)
        return context


class ErrorHandler:
    """Centralized error handling for LakehousePlumber."""

    def __init__(self, verbose: Optional[bool] = None):
        self.logger = logging.getLogger(__name__)
        self.verbose = verbose if verbose is not None else self._detect_verbose_mode()
        self.context = ErrorContext()
        self.progress_formatter = ProgressFormatter()

    def _detect_verbose_mode(self) -> bool:
        """Detect if we're in verbose mode from current logging config."""
        try:
            # Check if INFO level logging is enabled on console handlers
            root_logger = logging.getLogger()
            for handler in root_logger.handlers:
                if (
                    isinstance(handler, logging.StreamHandler)
                    and handler.stream == sys.stderr
                ):
                    return handler.level <= logging.INFO
            return False
        except Exception:
            # Fallback to False if logging not configured
            return False

    def _get_terminal_width(self) -> int:
        """Get terminal width for formatting."""
        try:
            return shutil.get_terminal_size().columns
        except:
            return 80  # Fallback width

    def _format_error_box(
        self, title: str, content: List[str], error_code: str = None
    ) -> str:
        """Format error in a clean box layout."""
        terminal_width = self._get_terminal_width()
        box_width = min(terminal_width - 4, 80)  # Leave some margin

        lines = []

        # Top border
        lines.append(
            "╭─" + " " + title + " " + "─" * (box_width - len(title) - 4) + "╮"
        )

        # Empty line for spacing
        lines.append("│" + " " * (box_width - 2) + "│")

        # Content lines
        for line in content:
            if len(line) <= box_width - 4:
                # Line fits, center it
                padding = (box_width - 4 - len(line)) // 2
                lines.append(
                    "│"
                    + " " * (padding + 1)
                    + line
                    + " " * (box_width - 3 - padding - len(line))
                    + "│"
                )
            else:
                # Line too long, wrap it
                words = line.split()
                current_line = ""
                for word in words:
                    if len(current_line + " " + word) <= box_width - 4:
                        current_line += (" " if current_line else "") + word
                    else:
                        if current_line:
                            lines.append(
                                "│ " + current_line.ljust(box_width - 4) + " │"
                            )
                        current_line = word
                if current_line:
                    lines.append("│ " + current_line.ljust(box_width - 4) + " │")

        # Empty line for spacing
        lines.append("│" + " " * (box_width - 2) + "│")

        # Error code if provided
        if error_code:
            code_line = f"📚 Error Code: {error_code}"
            lines.append("│ " + code_line.ljust(box_width - 4) + " │")

        # Bottom border
        lines.append("╰" + "─" * (box_width - 2) + "╯")

        return "\n".join(lines)

    def _extract_lhp_error_details(self, error_str: str) -> Dict[str, str]:
        """Extract details from LHPError formatted string."""
        details = {}

        # Extract error code
        import re

        code_match = re.search(r"\[LHP-([A-Z]+-[0-9]+)\]", error_str)
        if code_match:
            details["code"] = code_match.group(1)

        # Extract title (after "Error [CODE]: ")
        title_match = re.search(r"Error \[LHP-[A-Z]+-[0-9]+\]: (.+?)\\n", error_str)
        if title_match:
            details["title"] = title_match.group(1)

        # Extract action name from context (try multiple patterns)
        action_match = re.search(r"• Action: (.+?)\\n", error_str)
        if not action_match:
            action_match = re.search(r"Action '(.+?)' has", error_str)
        if action_match:
            details["action"] = action_match.group(1)

        # Extract unknown fields (try multiple patterns)
        unknown_match = re.search(r"• Unknown: \['(.+?)'\]", error_str)
        if not unknown_match:
            unknown_match = re.search(r"Unknown field '(.+?)'", error_str)
        if unknown_match:
            details["unknown_fields"] = unknown_match.group(1)

        # Extract section/component info
        section_match = re.search(r"• Section: (.+?)\\n", error_str)
        if section_match:
            details["section"] = section_match.group(1)

        # Extract component type
        type_match = re.search(r"• Type: (.+?)\\n", error_str)
        if type_match:
            details["component_type"] = type_match.group(1)

        return details

    def set_context(self, context: ErrorContext) -> ErrorHandler:
        """Set error context for subsequent operations."""
        self.context = context
        return self

    def with_pipeline_context(self, pipeline: str, environment: str) -> ErrorHandler:
        """Create handler with pipeline context."""
        new_context = ErrorContext().set_pipeline_context(pipeline, environment)
        return ErrorHandler(self.verbose).set_context(new_context)

    def with_action_context(self, action: str) -> ErrorHandler:
        """Create handler with action context."""
        new_context = ErrorContext()
        new_context.__dict__.update(self.context.__dict__)
        new_context.set_action_context(action)
        return ErrorHandler(self.verbose).set_context(new_context)

    def handle_cli_error(
        self, e: Exception, operation: str, show_logs_hint: bool = True
    ) -> None:
        """Handle CLI command errors with proper logging and output."""

        if isinstance(e, LHPError):
            # LHPError is already user-friendly, just display it
            click.echo(str(e))
            if self.verbose:
                self.logger.exception(f"{operation} failed with LHPError")
            else:
                self.logger.error(f"{operation} failed: {e.title}")
        else:
            # Check if the error message contains a formatted LHPError
            error_message = str(e)
            if "❌ Error [LHP-" in error_message and "======" in error_message:
                # This is a formatted LHPError string, display it cleanly
                self._display_formatted_error(error_message, operation)
                if self.verbose:
                    self.logger.exception(f"{operation} failed with formatted LHPError")
                else:
                    # Don't log the error message since we've already displayed a clean error box
                    self.logger.debug(
                        f"{operation} failed: Configuration validation error"
                    )
            else:
                # Convert other exceptions to user-friendly format
                click.echo(f"❌ {operation} failed: {e}")

                if self.verbose:
                    # Show full stack trace only in verbose mode
                    self.logger.exception(f"{operation} failed - Full error details")
                    click.echo("🔍 Full error details logged")
                else:
                    # Just log the error message
                    self.logger.error(f"{operation} failed: {e}")
                    if show_logs_hint:
                        click.echo(
                            "📝 Use --verbose flag for detailed error information"
                        )

    def _display_formatted_error(self, error_message: str, operation: str) -> None:
        """Display a formatted LHPError in a clean, readable way."""
        details = self._extract_lhp_error_details(error_message)

        # Create clean error display
        if "title" in details:
            title = details["title"]
        else:
            title = "Configuration Error"

        content = []

        # Add main error message
        if "action" in details and "unknown_fields" in details:
            if "section" in details:
                content.append(
                    f"Action '{details['action']}' has unknown field '{details['unknown_fields']}' in {details['section']}"
                )
            else:
                content.append(
                    f"Action '{details['action']}' has unknown field: '{details['unknown_fields']}'"
                )
        elif "title" in details:
            content.append(details["title"])

        content.append("")  # Empty line for spacing

        # Add context if available
        if self.context.pipeline:
            content.append(f"Pipeline: {self.context.pipeline}")
        if self.context.environment:
            content.append(f"Environment: {self.context.environment}")
        if "component_type" in details:
            content.append(f"Component: {details['component_type']}")

        content.append("")  # Empty line for spacing

        # Add helpful suggestions
        content.append("💡 How to fix:")
        if "unknown_fields" in details:
            content.append(
                f"   • Remove the '{details['unknown_fields']}' field from your configuration"
            )
            if "section" in details:
                content.append(
                    f"   • Check valid fields for {details['section']} in documentation"
                )
            else:
                content.append("   • Check documentation for valid field names")
        else:
            content.append("   • Review your configuration syntax")
            content.append("   • Check for typos in field names")

        # Display the formatted error box
        error_code = details.get("code", "")
        formatted_error = self._format_error_box(title, content, error_code)
        click.echo(formatted_error)

    def display_pipeline_progress(
        self, pipeline_name: str, status: ProgressStatus, details: str = None
    ) -> None:
        """Display pipeline progress with consistent formatting."""
        progress_line = self.progress_formatter.format_pipeline_progress(
            pipeline_name, status, details
        )
        click.echo(progress_line)

    def display_flowgroup_progress(
        self, flowgroup_name: str, status: ProgressStatus, is_final: bool = False
    ) -> None:
        """Display flowgroup progress with tree structure."""
        if is_final:
            progress_line = self.progress_formatter.format_final_flowgroup(
                flowgroup_name, status
            )
        else:
            progress_line = self.progress_formatter.format_flowgroup_progress(
                flowgroup_name, status
            )
        click.echo(progress_line)

    def handle_generation_error(self, e: Exception, action_name: str) -> LHPError:
        """Convert generation errors to user-friendly LHPError."""

        if isinstance(e, LHPError):
            return e

        # Convert common exceptions to LHPError
        if isinstance(e, FileNotFoundError):
            return LHPError(
                category=ErrorCategory.IO,
                code_number="003",
                title="File not found during generation",
                details=f"Could not find required file for action '{action_name}': {e}",
                suggestions=[
                    "Check that all referenced files exist",
                    "Verify file paths are correct in your configuration",
                    "Ensure files are accessible from the current working directory",
                ],
                context=self.context.add_extra(
                    "Error Type", "FileNotFoundError"
                ).to_dict(),
            )

        if isinstance(e, ValueError):
            return LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="003",
                title="Invalid configuration for code generation",
                details=f"Configuration error in action '{action_name}': {e}",
                suggestions=[
                    "Check your YAML configuration for syntax errors",
                    "Verify all required fields are present",
                    "Review the action configuration against documentation",
                ],
                context=self.context.add_extra("Error Type", "ValueError").to_dict(),
            )

        # Generic exception handling
        return LHPError(
            category=ErrorCategory.GENERAL,
            code_number="001",
            title="Unexpected error during code generation",
            details=f"An unexpected error occurred while generating code for action '{action_name}': {e}",
            suggestions=[
                "Check the configuration for any unusual values",
                "Try running with --verbose flag for more details",
                "If the error persists, please report this issue",
            ],
            context=self.context.add_extra("Error Type", type(e).__name__).to_dict(),
        )

    def handle_validation_error(self, e: Exception, component: str) -> LHPError:
        """Convert validation errors to user-friendly LHPError."""

        if isinstance(e, LHPError):
            return e

        if isinstance(e, ValueError):
            return LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="004",
                title="Configuration validation failed",
                details=f"Validation error in {component}: {e}",
                suggestions=[
                    "Review the configuration syntax",
                    "Check for missing required fields",
                    "Verify field values are in the correct format",
                ],
                context=self.context.add_extra("Component", component).to_dict(),
            )

        # Generic validation error
        return LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="005",
            title="Unexpected validation error",
            details=f"An unexpected error occurred while validating {component}: {e}",
            suggestions=[
                "Check the configuration for any unusual values",
                "Try running with --verbose flag for more details",
                "If the error persists, please report this issue",
            ],
            context=self.context.add_extra("Component", component).to_dict(),
        )

    def handle_file_error(
        self, e: Exception, file_path: str, operation: str
    ) -> LHPError:
        """Convert file operation errors to user-friendly LHPError."""

        if isinstance(e, LHPError):
            return e

        if isinstance(e, FileNotFoundError):
            return ErrorFormatter.file_not_found(
                file_path=file_path,
                search_locations=[str(Path.cwd()), "relative to YAML file"],
                file_type="file",
            )

        if isinstance(e, PermissionError):
            return LHPError(
                category=ErrorCategory.IO,
                code_number="004",
                title="Permission denied",
                details=f"Permission denied while {operation} file: {file_path}",
                suggestions=[
                    "Check file permissions",
                    "Ensure you have write access to the target directory",
                    "Run with appropriate privileges if needed",
                ],
                context=self.context.add_extra("File Path", file_path).to_dict(),
            )

        # Generic file error
        return LHPError(
            category=ErrorCategory.IO,
            code_number="005",
            title="File operation failed",
            details=f"Error while {operation} file '{file_path}': {e}",
            suggestions=[
                "Check that the file path is correct",
                "Verify file permissions",
                "Ensure the directory exists",
            ],
            context=self.context.add_extra("File Path", file_path).to_dict(),
        )

    def handle_yaml_error(self, e: Exception, file_path: str) -> LHPError:
        """Convert YAML parsing errors to user-friendly LHPError."""

        if isinstance(e, LHPError):
            return e

        import yaml

        if isinstance(e, yaml.YAMLError):
            return LHPError(
                category=ErrorCategory.CONFIG,
                code_number="004",
                title="YAML syntax error",
                details=f"Invalid YAML syntax in file '{file_path}': {e}",
                suggestions=[
                    "Check YAML syntax (indentation, colons, dashes)",
                    "Validate YAML online or with a YAML linter",
                    "Ensure all strings are properly quoted if they contain special characters",
                ],
                example="""Valid YAML syntax:
flowgroup: my_flowgroup
actions:
  - name: my_action
    type: load
    source:
      type: cloudfiles
      path: /path/to/files/*.csv""",
                context=self.context.add_extra("File Path", file_path).to_dict(),
            )

        # Generic YAML error
        return LHPError(
            category=ErrorCategory.CONFIG,
            code_number="005",
            title="YAML processing error",
            details=f"Error processing YAML file '{file_path}': {e}",
            suggestions=[
                "Check the file format and structure",
                "Ensure the file is valid YAML",
                "Try opening the file in a text editor to check for corruption",
            ],
            context=self.context.add_extra("File Path", file_path).to_dict(),
        )

    def log_error(self, e: Exception, context: str) -> None:
        """Log errors respecting verbose mode."""
        if self.verbose:
            self.logger.exception(f"{context} - Full error details")
        else:
            self.logger.error(f"{context}: {e}")

    def create_dependency_error(self, cycle_components: List[str]) -> LHPError:
        """Create a dependency cycle error."""
        return ErrorFormatter.dependency_cycle(cycle_components)

    def create_config_conflict_error(
        self,
        action_name: str,
        field_pairs: List[tuple],
        preset_name: Optional[str] = None,
    ) -> LHPError:
        """Create a configuration conflict error."""
        return ErrorFormatter.configuration_conflict(
            action_name, field_pairs, preset_name
        )

    def create_unknown_type_error(
        self,
        value_type: str,
        provided_value: str,
        valid_values: List[str],
        example_usage: str,
    ) -> LHPError:
        """Create an unknown type error with suggestions."""
        return ErrorFormatter.unknown_type_with_suggestion(
            value_type, provided_value, valid_values, example_usage
        )


# Global error handler instance for convenience
_global_error_handler = ErrorHandler()


def get_error_handler(verbose: Optional[bool] = None) -> ErrorHandler:
    """Get a global error handler instance."""
    if verbose is not None:
        return ErrorHandler(verbose)
    return _global_error_handler


def handle_cli_error(e: Exception, operation: str, verbose: bool = False) -> None:
    """Convenience function for CLI error handling."""
    handler = ErrorHandler(verbose)
    handler.handle_cli_error(e, operation)

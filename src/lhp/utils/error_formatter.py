"""Error formatter for user-friendly error messages."""

from __future__ import annotations

import textwrap
from difflib import get_close_matches
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

if TYPE_CHECKING:
    from rich.panel import Panel


class ErrorCategory(Enum):
    """Error categories with prefixes."""

    CLOUDFILES = "CF"  # CloudFiles specific errors
    VALIDATION = "VAL"  # Validation errors
    IO = "IO"  # File/IO errors
    CONFIG = "CFG"  # Configuration errors
    DEPENDENCY = "DEP"  # Dependency errors
    ACTION = "ACT"  # Action type errors
    GENERAL = "GEN"  # General errors


class LHPError(Exception):
    """User-friendly error with formatting support.

    Base class for all LHP-specific exceptions. Subclasses use dual
    inheritance (e.g. ``LHPValidationError(LHPError, ValueError)``)
    so that existing ``except ValueError`` handlers still catch them.
    """

    def __init__(
        self,
        category: ErrorCategory,
        code_number: str,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[Dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ):
        self.category = category
        self.code_number = code_number
        self.code = f"LHP-{category.value}-{code_number}"
        self.title = title
        self.details = details
        self.suggestions = suggestions or []
        self.example = example
        self.context = context or {}
        self.doc_link = (
            doc_link
            or "https://lakehouse-plumber.readthedocs.io/en/latest/errors_reference.html"
        )

        # Format the complete error message
        super().__init__(self._format_message())

    def _category_label(self) -> str:
        """Return a human-readable label for the error category."""
        return {
            ErrorCategory.VALIDATION: "Validation Error",
            ErrorCategory.CONFIG: "Configuration Error",
            ErrorCategory.IO: "I/O Error",
            ErrorCategory.DEPENDENCY: "Dependency Error",
            ErrorCategory.ACTION: "Action Error",
            ErrorCategory.CLOUDFILES: "CloudFiles Error",
            ErrorCategory.GENERAL: "Error",
        }.get(self.category, "Error")

    def _border_style(self) -> str:
        """Return the Rich border style for this error's category."""
        return {
            ErrorCategory.VALIDATION: "yellow",
            ErrorCategory.CONFIG: "red",
            ErrorCategory.IO: "red",
            ErrorCategory.DEPENDENCY: "red",
            ErrorCategory.ACTION: "red",
            ErrorCategory.CLOUDFILES: "red",
            ErrorCategory.GENERAL: "red",
        }.get(self.category, "red")

    def _template_data(self) -> Dict[str, Any]:
        """Return structured data for rendering this error.

        Single source of truth consumed by both ``_format_message`` (plain
        text used by ``__str__`` and the file logger) and ``__rich__`` (Rich
        Panel rendering on stderr). Keeps the two presentations from drifting.
        """
        return {
            "code": self.code,
            "category_label": self._category_label(),
            "title": self.title,
            "details": self.details,
            "context": dict(self.context) if self.context else {},
            "suggestions": list(self.suggestions or []),
            "example": self.example or None,
            "doc_link": self.doc_link or None,
        }

    def _format_message(self) -> str:
        """Format the error as a plain-text ASCII message.

        Used by ``__str__`` and the file logger. No emoji, no ANSI escapes —
        safe for log files, piped output, and non-TTY contexts.
        """
        d = self._template_data()
        lines = [f"Error [{d['code']}]: {d['title']}"]
        lines.append("=" * 70)
        if d["details"]:
            lines.append("")
            lines.append(textwrap.fill(d["details"], width=70))
        if d["context"]:
            lines.append("")
            lines.append("Context")
            for key, value in d["context"].items():
                lines.append(f"  {key}: {value}")
        if d["suggestions"]:
            lines.append("")
            lines.append("Suggestions")
            for suggestion in d["suggestions"]:
                wrapped = textwrap.fill(suggestion, width=66, subsequent_indent="     ")
                lines.append(f"  -> {wrapped}")
        if d["example"]:
            lines.append("")
            lines.append("Example")
            for line in d["example"].strip().split("\n"):
                lines.append(f"  {line}")
        if d["doc_link"]:
            lines.append("")
            lines.append(f"More info: {d['doc_link']}")
        lines.append("=" * 70)
        return "\n".join(lines)

    def __rich__(self) -> Panel:
        """Render the error as a Rich Panel for stderr console output.

        Rich detects ``__rich__`` automatically when ``Console.print`` is
        called with an LHPError instance.
        """
        # Local imports to keep ``rich`` an optional render-time dependency
        # for callers that only need ``__str__``.
        from rich.panel import Panel
        from rich.text import Text

        d = self._template_data()
        body = Text()
        body.append(d["title"] + "\n\n", style="bold")
        if d["details"]:
            body.append(d["details"] + "\n")
        if d["context"]:
            body.append("\n")
            body.append("Context\n", style="bold dim")
            for key, value in d["context"].items():
                body.append(f"  {key}: {value}\n", style="dim")
        if d["suggestions"]:
            body.append("\n")
            body.append("Suggestions\n", style="bold dim")
            for suggestion in d["suggestions"]:
                body.append("  -> ")
                body.append(f"{suggestion}\n")
        if d["example"]:
            body.append("\n")
            body.append("Example\n", style="bold dim")
            for line in d["example"].strip().split("\n"):
                body.append(f"  {line}\n")
        if d["doc_link"]:
            body.append("\n")
            body.append(f"More info: {d['doc_link']}", style="dim underline")
        return Panel(
            body,
            title=f"{d['code']}   {d['category_label']}",
            border_style=self._border_style(),
            title_align="left",
            padding=(1, 2),
        )

    def __reduce__(self):
        return (
            self.__class__,
            (
                self.category,
                self.code_number,
                self.title,
                self.details,
                self.suggestions,
                self.example,
                self.context,
                self.doc_link,
            ),
        )

    @classmethod
    def from_unexpected_exception(
        cls,
        exc: BaseException,
        operation: str,
    ) -> "LHPError":
        """Wrap a non-LHP, non-Bundle exception as an LHPError for CLI rendering.

        Used by :func:`cli_error_boundary` to surface generic fallback failures
        through the project's unified Rich panel UX rather than as plain stderr
        lines. The full Python traceback is intentionally NOT carried on the
        returned LHPError; callers are expected to ``logger.exception(...)`` so
        the traceback lands in the log file only.
        """
        return cls(
            category=ErrorCategory.GENERAL,
            code_number="902",
            title=f"{operation} failed: unexpected error",
            details=str(exc) or repr(exc),
            context={"exception_type": type(exc).__name__},
            suggestions=[
                "Re-run with --verbose to keep the traceback in your terminal",
                "Check the log file for the full Python traceback",
                "If this is reproducible, please file an issue with the log excerpt",
            ],
        )


class LHPValidationError(LHPError, ValueError):
    """LHPError subclass that is also a ValueError.

    Use when replacing a bare ValueError so that existing
    ``except ValueError`` handlers continue to catch it.
    """

    pass


class LHPConfigError(LHPError, ValueError):
    """LHPError subclass for configuration errors.

    Also a ValueError for backward compatibility with existing
    ``except ValueError`` handlers.
    """

    pass


class LHPFileError(LHPError, FileNotFoundError):
    """LHPError subclass that is also a FileNotFoundError.

    Use when replacing a bare FileNotFoundError so that existing
    ``except FileNotFoundError`` handlers continue to catch it.
    """

    pass


_WORKER_ERROR_TYPE_TO_LHP_CLASS: Dict[str, type] = {
    # Common stdlib exception names map to the LHP subclass that carries
    # the matching dual inheritance, so legacy ``except ValueError`` /
    # ``except FileNotFoundError`` handlers still catch worker failures
    # surfaced across the spawn boundary. LHPError instances raised in
    # workers do NOT round-trip through this mapping: they travel back
    # via ``PipelineDelta.lhp_error`` and are re-raised unchanged.
    "ValueError": LHPValidationError,
    "FileNotFoundError": LHPFileError,
    "OSError": LHPFileError,
    "IOError": LHPFileError,
    "PermissionError": LHPFileError,
    "IsADirectoryError": LHPFileError,
    "NotADirectoryError": LHPFileError,
}


def lhp_error_from_worker_failure(
    *,
    pipeline_name: str,
    error_type: str,
    error_message: str,
    error_traceback: str,
) -> LHPError:
    """Reconstruct a worker-side exception with type fidelity preserved.

    Dispatches to the right :class:`LHPError` subclass based on
    ``error_type`` so that callers catching ``ValueError`` /
    ``FileNotFoundError`` / etc. continue to catch worker failures the
    same way they caught them when generation ran in the main thread.
    Unknown types fall back to plain :class:`LHPError`.

    LHPError instances raised in workers do NOT go through this function:
    they travel back via :attr:`PipelineDelta.lhp_error` and are re-raised
    unchanged by the orchestrator / layers consumer.
    """
    target_cls = _WORKER_ERROR_TYPE_TO_LHP_CLASS.get(error_type, LHPError)
    first_trace_line = (
        error_traceback.strip().splitlines()[-1] if error_traceback else ""
    )
    details = error_message or "(no message)"
    if first_trace_line and first_trace_line not in details:
        details = f"{details}\n[{first_trace_line}]"
    return target_cls(
        category=ErrorCategory.GENERAL,
        code_number="901",
        title=f"Pipeline '{pipeline_name}' failed in worker ({error_type})",
        details=details,
        context={"pipeline": pipeline_name, "worker_exception": error_type},
    )


class MultiDocumentError(LHPError):
    """Error raised when a single-document loader encounters wrong number of documents."""

    def __init__(
        self,
        file_path: Union[Path, str],
        num_documents: int,
        error_context: Optional[str] = None,
    ):
        """
        Initialize MultiDocumentError.

        Args:
            file_path: Path to the YAML file
            num_documents: Number of documents found (0 for empty, 2+ for multi-document)
            error_context: Optional context for error message
        """
        # Normalize to Path for consistent handling
        file_path = Path(file_path)
        self.file_path = file_path
        self.num_documents = num_documents
        self.error_context = error_context
        context_str = error_context or f"YAML file {file_path}"

        if num_documents == 0:
            details = (
                f"The file '{file_path}' is empty or contains no valid YAML documents."
            )
            suggestions = [
                "Ensure the file contains valid YAML content",
                "Check that the file is not empty",
                "Verify the file encoding is UTF-8",
            ]
        else:
            details = f"The {context_str} contains {num_documents} documents (separated by '---'), but expected exactly 1."
            suggestions = [
                "Use load_yaml_documents_all() for multi-document YAML files",
                "Remove extra '---' separators if you intended a single document",
                "Split the file into separate files, one per document",
            ]

        super().__init__(
            category=ErrorCategory.IO,
            code_number="003",
            title=f"Invalid Document Count: Expected 1, Found {num_documents}",
            details=details,
            suggestions=suggestions,
            context={"file_path": str(file_path), "num_documents": num_documents},
        )

    def __reduce__(self):
        return (
            self.__class__,
            (self.file_path, self.num_documents, self.error_context),
        )


class ErrorFormatter:
    """Utility class for formatting common errors."""

    @staticmethod
    def configuration_conflict(
        action_name: str, field_pairs: List[tuple], preset_name: Optional[str] = None
    ) -> LHPConfigError:
        """Format configuration conflict errors."""

        conflicts = []
        examples = []

        for old_field, new_field in field_pairs:
            conflicts.append(f"• '{old_field}' (legacy) vs '{new_field}' (new format)")

            # Generate example for this conflict
            if "cloudFiles." in new_field:
                examples.append(f"""Option 1 (Recommended - New format):
  options:
    {new_field}: "value"
    
Option 2 (Legacy - will be deprecated):
  {old_field}: "value" """)

        details = (
            "You have specified the same configuration in multiple ways:\n"
            + "\n".join(conflicts)
        )

        suggestions = [
            "Use only ONE approach for each configuration option",
            "Prefer the new format (options.cloudFiles.*) for future compatibility",
        ]

        if preset_name:
            suggestions.append(
                f"Check if this option is already defined in preset '{preset_name}'"
            )

        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title=f"Configuration conflict in action '{action_name}'",
            details=details,
            suggestions=suggestions,
            example="\n\n".join(examples),
            context=(
                {"Action": action_name, "Preset": preset_name}
                if preset_name
                else {"Action": action_name}
            ),
        )

    @staticmethod
    def incompatible_options(
        action_name: str,
        option_a: str,
        option_b: str,
        reason: str,
        suggestion: str,
        example: Optional[str] = None,
    ) -> LHPValidationError:
        """Format incompatible Delta options errors."""
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="013",
            title=f"Incompatible options in action '{action_name}'",
            details=(
                f"Options '{option_a}' and '{option_b}' cannot be used together. "
                f"{reason}"
            ),
            suggestions=[
                suggestion,
                "See Delta Lake documentation for valid option combinations",
            ],
            example=example,
            context={
                "Action": action_name,
                "Option A": option_a,
                "Option B": option_b,
            },
        )

    @staticmethod
    def missing_required_field(
        field_name: str,
        component_type: str,
        component_name: str,
        field_description: str,
        example_config: str,
    ) -> LHPValidationError:
        """Format missing required field errors."""

        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="001",
            title=f"Missing required field '{field_name}'",
            details=f"The {component_type} '{component_name}' requires a '{field_name}' field. {field_description}",
            suggestions=[
                f"Add the '{field_name}' field to your configuration",
                "Check the example below for the correct format",
            ],
            example=example_config,
            context={
                "Component Type": component_type,
                "Component Name": component_name,
                "Missing Field": field_name,
            },
        )

    @staticmethod
    def file_not_found(
        file_path: str, search_locations: List[str], file_type: str = "file"
    ) -> LHPFileError:
        """Format file not found errors."""

        locations_text = "\n".join([f"  • {loc}" for loc in search_locations])

        return LHPFileError(
            category=ErrorCategory.IO,
            code_number="001",
            title=f"{file_type.capitalize()} not found",
            details=f"Could not find {file_type}: '{file_path}'",
            suggestions=[
                f"Ensure the {file_type} exists in one of these locations:\n{locations_text}",
                "Use relative paths from your YAML file location",
                "Check for typos in the file path",
            ],
            example="""Valid path examples:
  Relative: ../sql/my_query.sql
  Absolute: /absolute/path/to/query.sql
  From YAML: ./expectations/quality_checks.json""",
            context={"File Path": file_path, "File Type": file_type},
        )

    @staticmethod
    def unknown_type_with_suggestion(
        value_type: str,
        provided_value: str,
        valid_values: List[str],
        example_usage: str,
    ) -> LHPConfigError:
        """Format unknown type errors with suggestions."""

        # Find close matches
        suggestions = get_close_matches(provided_value, valid_values, n=3, cutoff=0.6)

        did_you_mean = ""
        if suggestions:
            suggestion_list = [f"'{s}'" for s in suggestions]
            did_you_mean = f"\n\nDid you mean: {', '.join(suggestion_list)}?"

        valid_list = "\n".join([f"  • {v}" for v in sorted(valid_values)])

        return LHPConfigError(
            category=ErrorCategory.ACTION,
            code_number="001",
            title=f"Unknown {value_type}: '{provided_value}'",
            details=f"'{provided_value}' is not a valid {value_type}.{did_you_mean}",
            suggestions=[
                f"Use one of these valid {value_type}s:\n{valid_list}",
                "Check spelling and case sensitivity",
            ],
            example=example_usage,
            context={"Provided": provided_value, "Value Type": value_type},
        )

    @staticmethod
    def validation_errors(
        component_name: str, component_type: str, errors: List[str]
    ) -> LHPValidationError:
        """Format validation errors with clear explanations."""

        error_details = []
        suggestions = []

        for error in errors:
            # Parse common validation errors and provide specific help
            if "Missing source" in error:
                error_details.append("✗ Missing source view or configuration")
                suggestions.append(
                    "Add a 'source' field pointing to a view or configuration"
                )
            elif "Invalid target" in error:
                error_details.append("✗ Invalid target reference")
                suggestions.append(
                    "Ensure 'target' references a defined view or valid table"
                )
            elif "circular dependency" in error.lower():
                error_details.append("✗ Circular dependency detected")
                suggestions.append("Review view dependencies to break the cycle")
            else:
                error_details.append(f"✗ {error}")

        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="002",
            title=f"Validation failed for {component_type} '{component_name}'",
            details="\n".join(error_details),
            suggestions=suggestions,
            example="""Example valid configuration:
actions:
  - name: process_data
    type: transform
    sub_type: sql
    source: v_raw_data      # ← Required: source view
    target: v_processed     # ← Required: target view
    sql: |
      SELECT * FROM $source""",
            context={
                "Component": component_name,
                "Type": component_type,
                "Error Count": len(errors),
            },
        )

    @staticmethod
    def yaml_parse_error(
        file_path: str,
        error_message: str,
        context: Optional[str] = None,
    ) -> "LHPConfigError":
        """Format YAML parsing errors."""
        details = f"Failed to parse YAML file '{file_path}': {error_message}"
        if context:
            details += f"\nContext: {context}"

        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="009",
            title="YAML parsing error",
            details=details,
            suggestions=[
                "Check YAML syntax (indentation, colons, dashes)",
                "Validate the file with a YAML linter",
                "Ensure all strings with special characters are quoted",
            ],
            context={"File": file_path},
        )

    @staticmethod
    def deprecated_field(
        action_name: str,
        field_name: str,
        replacement: str,
        example: Optional[str] = None,
    ) -> LHPConfigError:
        """Format deprecated field warnings as errors."""
        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="010",
            title=f"Deprecated field '{field_name}' in action '{action_name}'",
            details=(
                f"The field '{field_name}' has been removed. "
                f"Use '{replacement}' instead."
            ),
            suggestions=[
                f"Replace '{field_name}' with '{replacement}' in your configuration",
                "Check the migration guide for details on the new format",
            ],
            example=example,
            context={"Action": action_name, "Deprecated Field": field_name},
        )

    @staticmethod
    def invalid_field_value(
        action_name: str,
        field_name: str,
        value: Any,
        valid_values: List[str],
        example: Optional[str] = None,
    ) -> LHPValidationError:
        """Format invalid field value errors."""
        valid_list = ", ".join(f"'{v}'" for v in valid_values)
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="006",
            title=f"Invalid value for '{field_name}'",
            details=(
                f"Action '{action_name}' has invalid value '{value}' "
                f"for field '{field_name}'. Valid values: {valid_list}"
            ),
            suggestions=[
                f"Use one of: {valid_list}",
                "Check spelling and case sensitivity",
            ],
            example=example,
            context={
                "Action": action_name,
                "Field": field_name,
                "Provided": str(value),
            },
        )

    @staticmethod
    def dependency_cycle(cycle_components: List[str]) -> LHPError:
        """Format circular dependency errors."""

        # Create visual representation of the cycle
        cycle_visual = " → ".join(cycle_components + [cycle_components[0]])

        return LHPError(
            category=ErrorCategory.DEPENDENCY,
            code_number="001",
            title="Circular dependency detected",
            details=f"The following components form a dependency cycle:\n\n{cycle_visual}",
            suggestions=[
                "Review the dependency chain and remove one of the dependencies",
                "Consider splitting complex transformations into separate stages",
                "Use materialized views to break dependency cycles",
            ],
            example="""To break the cycle, you could:
1. Remove direct dependency:
   # Instead of: A → B → C → A
   # Create:     A → B → C
   #             D → A (separate flow)

2. Use intermediate materialization:
   # Create a materialized view at one point in the chain""",
            context={"Cycle": cycle_visual, "Components": ", ".join(cycle_components)},
        )

    @staticmethod
    def invalid_read_mode(
        action_name: str,
        action_type: str,
        provided: str,
        valid_modes: List[str],
    ) -> "LHPValidationError":
        """Format invalid readMode errors."""
        valid_list = ", ".join(f"'{m}'" for m in valid_modes)
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="007",
            title=f"Invalid readMode '{provided}' in action '{action_name}'",
            details=(
                f"Action '{action_name}' (type: {action_type}) has readMode "
                f"'{provided}' which is not valid. Valid modes: {valid_list}"
            ),
            suggestions=[
                f"Use one of: {valid_list}",
                "Use 'stream' for streaming ingestion (spark.readStream)",
                "Use 'batch' for batch reads (spark.read)",
            ],
            context={
                "Action": action_name,
                "Action Type": action_type,
                "Provided": provided,
            },
        )

    @staticmethod
    def invalid_field_type(
        action_name: str,
        field_name: str,
        expected_type: str,
        actual_type: str,
        example: Optional[str] = None,
    ) -> "LHPValidationError":
        """Format invalid field type errors."""
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="008",
            title=f"Invalid type for field '{field_name}' in action '{action_name}'",
            details=(
                f"Expected '{field_name}' to be {expected_type}, but got {actual_type}."
            ),
            suggestions=[
                f"Change '{field_name}' to a {expected_type} value",
                "Check the documentation for the correct format",
            ],
            example=example,
            context={
                "Action": action_name,
                "Field": field_name,
                "Expected Type": expected_type,
                "Actual Type": actual_type,
            },
        )

    @staticmethod
    def invalid_source_format(
        action_name: str,
        action_type: str,
        expected_formats: List[str],
    ) -> "LHPValidationError":
        """Format invalid source format errors."""
        formats = "\n".join(f"  - {fmt}" for fmt in expected_formats)
        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="012",
            title=f"Invalid source format in action '{action_name}'",
            details=(
                f"The source configuration for {action_type} action "
                f"'{action_name}' is not in a valid format."
            ),
            suggestions=[
                f"Use one of these formats:\n{formats}",
                "Check the documentation for source configuration examples",
            ],
            context={
                "Action": action_name,
                "Action Type": action_type,
            },
        )

    @staticmethod
    def template_not_found(
        template_name: str,
        available_templates: List[str],
        templates_dir: Optional[str] = None,
    ) -> "LHPConfigError":
        """Format template not found errors."""
        matches = get_close_matches(template_name, available_templates, n=3, cutoff=0.6)
        suggestions = []
        if matches:
            suggestions.append(f"Did you mean: {', '.join(repr(m) for m in matches)}?")
        if available_templates:
            suggestions.append(
                f"Available templates: {', '.join(sorted(available_templates))}"
            )
        suggestions.append("Check for typos in the template name")
        if templates_dir:
            suggestions.append(f"Ensure the template file exists in {templates_dir}/")

        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="027",
            title=f"Template '{template_name}' not found",
            details=f"No template named '{template_name}' was found.",
            suggestions=suggestions,
            context={"Template": template_name},
        )

    @staticmethod
    def missing_template_parameters(
        template_name: str,
        missing_params: List[str],
        available_params: Optional[List[str]] = None,
    ) -> "LHPConfigError":
        """Format missing template parameters errors."""
        missing_list = ", ".join(f"'{p}'" for p in missing_params)
        suggestions = [
            f"Add the missing parameters to template_parameters: {missing_list}",
        ]
        if available_params:
            suggestions.append(
                f"Available parameters: {', '.join(sorted(available_params))}"
            )
        return LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="012",
            title=f"Missing required template parameters for '{template_name}'",
            details=(
                f"Template '{template_name}' requires the following parameters "
                f"that were not provided: {missing_list}"
            ),
            suggestions=suggestions,
            example=f"""template_parameters:
  {missing_params[0] if missing_params else "param"}: value""",
            context={
                "Template": template_name,
                "Missing": missing_list,
            },
        )

    @staticmethod
    def schema_syntax_error(
        file_path: str,
        line_content: Optional[str],
        expected_format: str,
        example: Optional[str] = None,
    ) -> "LHPValidationError":
        """Format schema syntax errors."""
        details = f"Invalid syntax in schema file '{file_path}'."
        if line_content:
            details += f"\nProblematic content: {line_content}"
        details += f"\nExpected format: {expected_format}"

        return LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="011",
            title="Schema syntax error",
            details=details,
            suggestions=[
                "Check the schema syntax against the expected format",
                "Ensure column names and types are separated correctly",
                "Review the documentation for schema format examples",
            ],
            example=example,
            context={"File": file_path},
        )

    @staticmethod
    def preset_not_found(
        preset_name: str,
        available_presets: List[str],
    ) -> "LHPConfigError":
        """Format preset not found errors."""
        return ErrorFormatter.unknown_type_with_suggestion(
            value_type="preset",
            provided_value=preset_name,
            valid_values=available_presets,
            example_usage=f"""presets:
  - {available_presets[0] if available_presets else "my_preset"}""",
        )

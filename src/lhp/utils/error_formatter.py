"""Error formatter for user-friendly error messages."""

from typing import List, Optional, Dict, Any
from enum import Enum
import textwrap
from difflib import get_close_matches


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
    """User-friendly error with formatting support."""

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
        self.code = f"LHP-{category.value}-{code_number}"
        self.title = title
        self.details = details
        self.suggestions = suggestions or []
        self.example = example
        self.context = context or {}
        self.doc_link = (
            doc_link or f"https://docs.lakehouseplumber.com/errors/{self.code.lower()}"
        )

        # Format the complete error message
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with all components."""
        lines = []

        # Header with error code
        lines.append(f"\n❌ Error [{self.code}]: {self.title}")
        lines.append("=" * 70)

        # Details
        if self.details:
            lines.append("")
            lines.append(textwrap.fill(self.details, width=70))

        # Context information
        if self.context:
            lines.append("\n📍 Context:")
            for key, value in self.context.items():
                lines.append(f"   • {key}: {value}")

        # Suggestions
        if self.suggestions:
            lines.append("\n💡 How to fix:")
            for i, suggestion in enumerate(self.suggestions, 1):
                wrapped = textwrap.fill(
                    suggestion, width=66, subsequent_indent="      "
                )
                lines.append(f"   {i}. {wrapped}")

        # Example
        if self.example:
            lines.append("\n📝 Example:")
            example_lines = self.example.strip().split("\n")
            for line in example_lines:
                lines.append(f"   {line}")

        # Documentation link
        lines.append(f"\n📚 More info: {self.doc_link}")
        lines.append("=" * 70)

        return "\n".join(lines)


class ErrorFormatter:
    """Utility class for formatting common errors."""

    @staticmethod
    def configuration_conflict(
        action_name: str, field_pairs: List[tuple], preset_name: Optional[str] = None
    ) -> LHPError:
        """Format configuration conflict errors."""

        conflicts = []
        examples = []

        for old_field, new_field in field_pairs:
            conflicts.append(f"• '{old_field}' (legacy) vs '{new_field}' (new format)")

            # Generate example for this conflict
            if "cloudFiles." in new_field:
                examples.append(
                    f"""Option 1 (Recommended - New format):
  options:
    {new_field}: "value"
    
Option 2 (Legacy - will be deprecated):
  {old_field}: "value" """
                )

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

        return LHPError(
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
    def missing_required_field(
        field_name: str,
        component_type: str,
        component_name: str,
        field_description: str,
        example_config: str,
    ) -> LHPError:
        """Format missing required field errors."""

        return LHPError(
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
    ) -> LHPError:
        """Format file not found errors."""

        locations_text = "\n".join([f"  • {loc}" for loc in search_locations])

        return LHPError(
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
    ) -> LHPError:
        """Format unknown type errors with suggestions."""

        # Find close matches
        suggestions = get_close_matches(provided_value, valid_values, n=3, cutoff=0.6)

        did_you_mean = ""
        if suggestions:
            did_you_mean = (
                f"\n\nDid you mean: {', '.join([f"'{s}'" for s in suggestions])}?"
            )

        valid_list = "\n".join([f"  • {v}" for v in sorted(valid_values)])

        return LHPError(
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
    ) -> LHPError:
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

        return LHPError(
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

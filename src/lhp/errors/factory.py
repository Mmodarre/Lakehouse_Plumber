"""Single error-construction factory for LHP (TARGET §6).

Each method sources its ``(category, number)`` pair from
:mod:`lhp.errors.codes` (never re-typed as a literal) and returns — does
not raise — the constructed error.

Internal module per constitution §1.7: no ``:stability:`` annotations and
not re-exported from ``lhp/api/``.
"""

# JUSTIFIED: ErrorFactory is the single cohesive error-construction
# taxonomy mandated by TARGET §6 — ~24 public methods, over the §3.2
# ≤15 cap, and >500 lines (§3.3). TARGET §6 explicitly sanctions a
# "~25 factory-method" class here: each method maps one domain
# situation to one LHPError subclass, sourcing its (category, number)
# from errors/codes.py. The 17 intent constructors carry verbatim
# multi-line example blocks that drive most of the line count and
# CANNOT be compressed without breaking byte-identity, and the 7
# generic per-category constructors add the overloaded-code coverage.
# They share no state and
# co-locating them keeps the error-code taxonomy auditable in one
# place; splitting to module functions or per-category files would
# scatter the taxonomy and is NOT what TARGET §6 wants (single factory
# class) — see LOCAL spec §2.3 / OD-4 (RESOLVED: single class).

from __future__ import annotations

from difflib import get_close_matches
from typing import Any, List, Optional, Tuple

from . import codes
from .codes import ErrorCode
from .types import LHPConfigError, LHPError, LHPFileError, LHPValidationError


class ErrorFactory:
    """Build well-formed ``LHPError`` subclasses from the code registry."""

    @staticmethod
    def configuration_conflict(
        action_name: str,
        field_pairs: List[Tuple[str, str]],
        preset_name: Optional[str] = None,
    ) -> LHPConfigError:
        """Build a configuration-conflict error. Returns code LHP-CFG-001."""
        code = codes.CFG_001
        conflicts: List[str] = []
        examples: List[str] = []

        for old_field, new_field in field_pairs:
            conflicts.append(f"• '{old_field}' (legacy) vs '{new_field}' (new format)")

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
            category=code.category,
            code_number=code.number,
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
        """Build an incompatible-options error. Returns code LHP-VAL-013."""
        code = codes.VAL_013
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
        """Build a missing-required-field error. Returns code LHP-VAL-001."""
        code = codes.VAL_001
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
        """Build a file-not-found error. Returns code LHP-IO-001."""
        code = codes.IO_001
        locations_text = "\n".join([f"  • {loc}" for loc in search_locations])

        return LHPFileError(
            category=code.category,
            code_number=code.number,
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
        """Build an unknown-type error with suggestions. Returns code LHP-ACT-001."""
        code = codes.ACT_001
        suggestions = get_close_matches(provided_value, valid_values, n=3, cutoff=0.6)

        did_you_mean = ""
        if suggestions:
            suggestion_list = [f"'{s}'" for s in suggestions]
            did_you_mean = f"\n\nDid you mean: {', '.join(suggestion_list)}?"

        valid_list = "\n".join([f"  • {v}" for v in sorted(valid_values)])

        return LHPConfigError(
            category=code.category,
            code_number=code.number,
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
        """Build an aggregate validation error. Returns code LHP-VAL-002."""
        code = codes.VAL_002
        error_details: List[str] = []
        suggestions: List[str] = []

        for error in errors:
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
            category=code.category,
            code_number=code.number,
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
    ) -> LHPConfigError:
        """Build a YAML-parse error. Returns code LHP-CFG-009."""
        code = codes.CFG_009
        details = f"Failed to parse YAML file '{file_path}': {error_message}"
        if context:
            details += f"\nContext: {context}"

        return LHPConfigError(
            category=code.category,
            code_number=code.number,
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
        """Build a deprecated-field error. Returns code LHP-CFG-010."""
        code = codes.CFG_010
        return LHPConfigError(
            category=code.category,
            code_number=code.number,
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
        """Build an invalid-field-value error. Returns code LHP-VAL-006."""
        code = codes.VAL_006
        valid_list = ", ".join(f"'{v}'" for v in valid_values)
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
    def invalid_read_mode(
        action_name: str,
        action_type: str,
        provided: str,
        valid_modes: List[str],
    ) -> LHPValidationError:
        """Build an invalid-readMode error. Returns code LHP-VAL-007."""
        code = codes.VAL_007
        valid_list = ", ".join(f"'{m}'" for m in valid_modes)
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
    ) -> LHPValidationError:
        """Build an invalid-field-type error. Returns code LHP-VAL-008."""
        code = codes.VAL_008
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
    ) -> LHPValidationError:
        """Build an invalid-source-format error. Returns code LHP-VAL-012."""
        code = codes.VAL_012
        formats = "\n".join(f"  - {fmt}" for fmt in expected_formats)
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
    ) -> LHPConfigError:
        """Build a template-not-found error. Returns code LHP-CFG-027."""
        code = codes.CFG_027
        matches = get_close_matches(template_name, available_templates, n=3, cutoff=0.6)
        suggestions: List[str] = []
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
            category=code.category,
            code_number=code.number,
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
    ) -> LHPConfigError:
        """Build a missing-template-parameters error. Returns code LHP-CFG-012."""
        code = codes.CFG_012
        missing_list = ", ".join(f"'{p}'" for p in missing_params)
        suggestions = [
            f"Add the missing parameters to template_parameters: {missing_list}",
        ]
        if available_params:
            suggestions.append(
                f"Available parameters: {', '.join(sorted(available_params))}"
            )
        return LHPConfigError(
            category=code.category,
            code_number=code.number,
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
    ) -> LHPValidationError:
        """Build a schema-syntax error. Returns code LHP-VAL-011."""
        code = codes.VAL_011
        details = f"Invalid syntax in schema file '{file_path}'."
        if line_content:
            details += f"\nProblematic content: {line_content}"
        details += f"\nExpected format: {expected_format}"

        return LHPValidationError(
            category=code.category,
            code_number=code.number,
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
    ) -> LHPConfigError:
        """Build a preset-not-found error. Returns code LHP-ACT-001."""
        return ErrorFactory.unknown_type_with_suggestion(
            value_type="preset",
            provided_value=preset_name,
            valid_values=available_presets,
            example_usage=f"""presets:
  - {available_presets[0] if available_presets else "my_preset"}""",
        )

    @staticmethod
    def validation_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPValidationError:
        return LHPValidationError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def config_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPConfigError:
        return LHPConfigError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def io_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPFileError:
        return LHPFileError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def action_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPConfigError:
        return LHPConfigError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def dependency_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPError:
        return LHPError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def general_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPError:
        return LHPError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )

    @staticmethod
    def deprecation_error(
        code: ErrorCode,
        title: str,
        details: str,
        suggestions: Optional[List[str]] = None,
        example: Optional[str] = None,
        context: Optional[dict[str, Any]] = None,
        doc_link: Optional[str] = None,
    ) -> LHPError:
        return LHPError(
            category=code.category,
            code_number=code.number,
            title=title,
            details=details,
            suggestions=suggestions,
            example=example,
            context=context,
            doc_link=doc_link,
        )


__all__ = ["ErrorFactory"]

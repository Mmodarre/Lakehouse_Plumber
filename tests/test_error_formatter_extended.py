"""Extended tests for error_formatter module covering uncovered logic paths."""

import pytest

from lhp.utils.error_formatter import (
    LHPError,
    LHPValidationError,
    LHPConfigError,
    LHPFileError,
    MultiDocumentError,
    ErrorFormatter,
    ErrorCategory,
)


class TestLHPErrorFormatMessage:
    """Tests for LHPError._format_message output formatting."""

    def test_empty_details(self):
        """Empty details string produces no details section in output."""
        error = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="999",
            title="Test error",
            details="",
        )
        msg = str(error)
        assert "LHP-GEN-999" in msg
        assert "Test error" in msg

    def test_full_message_with_all_fields(self):
        """Error with all optional fields includes suggestions, example, context, and doc link."""
        error = LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="100",
            title="Full error",
            details="Something went wrong",
            suggestions=["Fix it", "Try again"],
            example="example: value",
            context={"Action": "my_action"},
            doc_link="https://example.com/docs",
        )
        msg = str(error)
        assert "LHP-VAL-100" in msg
        assert "Full error" in msg
        assert "Something went wrong" in msg
        assert "Fix it" in msg
        assert "Try again" in msg
        assert "example: value" in msg
        assert "my_action" in msg
        assert "https://example.com/docs" in msg

    def test_code_format(self):
        """Error code follows LHP-<category>-<number> format."""
        error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="042",
            title="Config error",
            details="Bad config",
        )
        assert error.code == "LHP-CFG-042"

    def test_default_doc_link(self):
        """Default documentation link is included when no doc_link provided."""
        error = LHPError(
            category=ErrorCategory.GENERAL,
            code_number="001",
            title="Test",
            details="Details",
        )
        assert "readthedocs" in str(error)


class TestLHPErrorSubclasses:
    """Tests for LHPError subclass hierarchy."""

    def test_lhp_validation_error_is_value_error(self):
        """LHPValidationError is catchable as ValueError."""
        error = LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="001",
            title="Validation",
            details="Bad value",
        )
        assert isinstance(error, ValueError)
        assert isinstance(error, LHPError)

    def test_lhp_config_error_is_value_error(self):
        """LHPConfigError is catchable as ValueError."""
        error = LHPConfigError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config",
            details="Bad config",
        )
        assert isinstance(error, ValueError)
        assert isinstance(error, LHPError)

    def test_lhp_file_error_is_file_not_found_error(self):
        """LHPFileError is catchable as FileNotFoundError."""
        error = LHPFileError(
            category=ErrorCategory.IO,
            code_number="001",
            title="File missing",
            details="Not found",
        )
        assert isinstance(error, FileNotFoundError)
        assert isinstance(error, LHPError)


class TestMultiDocumentError:
    """Tests for MultiDocumentError."""

    def test_multi_document(self):
        """num_documents > 0 produces a multi-document message."""
        error = MultiDocumentError("/tmp/test.yaml", num_documents=3)
        msg = str(error)
        assert "3 documents" in msg
        assert "LHP-IO-003" in error.code

    def test_empty_file(self):
        """num_documents == 0 produces an empty file message."""
        error = MultiDocumentError("/tmp/test.yaml", num_documents=0)
        msg = str(error)
        assert "empty" in msg.lower()
        assert "LHP-IO-003" in error.code

    def test_with_error_context(self):
        """Custom error_context is included in the message."""
        error = MultiDocumentError(
            "/tmp/test.yaml", num_documents=2, error_context="substitution file"
        )
        msg = str(error)
        assert "substitution file" in msg

    def test_context_dict_contains_file_path(self):
        """Context dict includes the file_path and num_documents."""
        error = MultiDocumentError("/tmp/test.yaml", num_documents=5)
        assert error.context["file_path"] == "/tmp/test.yaml"
        assert error.context["num_documents"] == 5


class TestErrorFormatterValidationErrors:
    """Tests for ErrorFormatter.validation_errors."""

    def test_missing_source_pattern(self):
        """'Missing source' error pattern is detected and formatted."""
        error = ErrorFormatter.validation_errors(
            "test_action", "action", ["Missing source view"]
        )
        msg = str(error)
        assert "source" in msg.lower()
        assert "LHP-VAL-002" in error.code

    def test_invalid_target_pattern(self):
        """'Invalid target' error pattern is detected and formatted."""
        error = ErrorFormatter.validation_errors(
            "test_action", "action", ["Invalid target reference"]
        )
        msg = str(error)
        assert "target" in msg.lower()

    def test_circular_dependency_pattern(self):
        """'circular dependency' error pattern is detected and formatted."""
        error = ErrorFormatter.validation_errors(
            "test_action", "action", ["Found circular dependency"]
        )
        msg = str(error)
        assert "circular" in msg.lower()

    def test_generic_error_pattern(self):
        """Unrecognized error string is passed through directly."""
        error = ErrorFormatter.validation_errors(
            "test_action", "action", ["Some other error"]
        )
        msg = str(error)
        assert "Some other error" in msg

    def test_multiple_errors(self):
        """Multiple validation errors are all included in output."""
        error = ErrorFormatter.validation_errors(
            "test_action",
            "action",
            ["Missing source view", "Invalid target reference", "Unknown issue"],
        )
        msg = str(error)
        assert "source" in msg.lower()
        assert "target" in msg.lower()
        assert "Unknown issue" in msg
        assert error.context["Error Count"] == 3


class TestErrorFormatterDependencyCycle:
    """Tests for ErrorFormatter.dependency_cycle."""

    def test_dependency_cycle(self):
        """Cycle visual shows A -> B -> C -> A format."""
        error = ErrorFormatter.dependency_cycle(["A", "B", "C"])
        msg = str(error)
        assert "A" in msg and "B" in msg and "C" in msg
        assert "LHP-DEP-001" in error.code

    def test_dependency_cycle_two_components(self):
        """Two-component cycle is formatted correctly."""
        error = ErrorFormatter.dependency_cycle(["X", "Y"])
        msg = str(error)
        assert "X" in msg and "Y" in msg
        assert "LHP-DEP-001" in error.code


class TestErrorFormatterTemplateNotFound:
    """Tests for ErrorFormatter.template_not_found."""

    def test_with_templates_dir(self):
        """templates_dir path is included in suggestions."""
        error = ErrorFormatter.template_not_found(
            "my_template", ["other_template"], templates_dir="/templates"
        )
        msg = str(error)
        assert "/templates/" in msg
        assert "LHP-CFG-027" in error.code

    def test_with_close_match(self):
        """Close match to template name is suggested."""
        error = ErrorFormatter.template_not_found(
            "my_templat", ["my_template", "other"]
        )
        msg = str(error)
        assert "my_template" in msg

    def test_no_close_match(self):
        """When no close match exists, available templates are still listed."""
        error = ErrorFormatter.template_not_found(
            "zzz_nonexistent", ["alpha", "beta", "gamma"]
        )
        msg = str(error)
        assert "alpha" in msg

    def test_empty_available_templates(self):
        """Empty available_templates list does not crash."""
        error = ErrorFormatter.template_not_found("my_template", [])
        msg = str(error)
        assert "my_template" in msg


class TestErrorFormatterMissingTemplateParameters:
    """Tests for ErrorFormatter.missing_template_parameters."""

    def test_with_available_params(self):
        """Available parameters are listed in the output."""
        error = ErrorFormatter.missing_template_parameters(
            "my_template", ["param1"], available_params=["param1", "param2"]
        )
        msg = str(error)
        assert "param1" in msg
        assert "param2" in msg
        assert "LHP-CFG-012" in error.code

    def test_without_available_params(self):
        """Missing params are listed even without available_params."""
        error = ErrorFormatter.missing_template_parameters(
            "my_template", ["missing_param"]
        )
        msg = str(error)
        assert "missing_param" in msg

    def test_multiple_missing_params(self):
        """Multiple missing parameters are all listed."""
        error = ErrorFormatter.missing_template_parameters(
            "tpl", ["p1", "p2", "p3"]
        )
        msg = str(error)
        assert "p1" in msg
        assert "p2" in msg
        assert "p3" in msg


class TestErrorFormatterInvalidSourceFormat:
    """Tests for ErrorFormatter.invalid_source_format."""

    def test_invalid_source_format(self):
        """Error code is LHP-VAL-012 and expected formats are listed."""
        error = ErrorFormatter.invalid_source_format(
            "test_action", "load", ["string", "dict"]
        )
        assert "LHP-VAL-012" in error.code
        msg = str(error)
        assert "string" in msg
        assert "dict" in msg
        assert "test_action" in msg


class TestErrorFormatterConfigurationConflict:
    """Tests for ErrorFormatter.configuration_conflict."""

    def test_cloudfiles_conflict(self):
        """cloudFiles conflict generates example with cloudFiles format."""
        error = ErrorFormatter.configuration_conflict(
            "test_action",
            [("format", "cloudFiles.format")],
        )
        msg = str(error)
        assert "cloudFiles" in msg
        assert "test_action" in msg
        assert isinstance(error, LHPConfigError)

    def test_multiple_conflicts(self):
        """Multiple field pair conflicts are all listed."""
        error = ErrorFormatter.configuration_conflict(
            "my_action",
            [
                ("format", "cloudFiles.format"),
                ("schema_hints", "cloudFiles.schemaHints"),
            ],
        )
        msg = str(error)
        assert "format" in msg
        assert "schema_hints" in msg

    def test_with_preset_name(self):
        """Preset name is included in suggestions when provided."""
        error = ErrorFormatter.configuration_conflict(
            "test_action",
            [("format", "cloudFiles.format")],
            preset_name="my_preset",
        )
        msg = str(error)
        assert "my_preset" in msg


class TestErrorFormatterMiscMethods:
    """Tests for other ErrorFormatter static methods."""

    def test_file_not_found(self):
        """file_not_found includes search locations and error code."""
        error = ErrorFormatter.file_not_found(
            "missing.sql", ["/path/a", "/path/b"], file_type="SQL file"
        )
        msg = str(error)
        assert "LHP-IO-001" in error.code
        assert "missing.sql" in msg
        assert "/path/a" in msg

    def test_missing_required_field(self):
        """missing_required_field includes field name and component context."""
        error = ErrorFormatter.missing_required_field(
            "source", "action", "load_data", "The source field is required.", "source: my_view"
        )
        msg = str(error)
        assert "LHP-VAL-001" in error.code
        assert "source" in msg

    def test_unknown_type_with_close_match(self):
        """Close match suggestion is included for typos."""
        error = ErrorFormatter.unknown_type_with_suggestion(
            "action type",
            "laod",
            ["load", "transform", "write"],
            "type: load",
        )
        msg = str(error)
        assert "load" in msg
        assert "LHP-ACT-001" in error.code

    def test_invalid_read_mode(self):
        """invalid_read_mode lists valid modes."""
        error = ErrorFormatter.invalid_read_mode(
            "my_action", "load", "invalid", ["stream", "batch"]
        )
        msg = str(error)
        assert "LHP-VAL-007" in error.code
        assert "stream" in msg
        assert "batch" in msg

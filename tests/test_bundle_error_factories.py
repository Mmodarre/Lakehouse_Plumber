"""Tests for bundle error factory functions."""

import pytest

from lhp.bundle.error_factories import (
    convert_bundle_error,
    create_bundle_config_error,
    create_bundle_resource_error,
    create_missing_target_error,
    create_template_error,
    create_yaml_processing_error,
)
from lhp.bundle.exceptions import (
    BundleConfigurationError,
    BundleResourceError,
    MissingDatabricksTargetError,
    TemplateError,
    YAMLProcessingError,
)
from lhp.utils.error_formatter import ErrorCategory, LHPError


class TestCreateBundleResourceError:
    """Tests for create_bundle_resource_error factory."""

    def test_basic_creation(self):
        """Should create LHPError with CONFIG category and code 020."""
        result = create_bundle_resource_error(
            operation="sync resources",
            details="Failed to sync bundle resources",
        )

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-020"
        assert "sync resources" in result.title
        assert "Failed to sync bundle resources" in result.details

    def test_with_file_path(self):
        """Should include file path in context when provided."""
        result = create_bundle_resource_error(
            operation="read file",
            details="Cannot read file",
            file_path="/path/to/databricks.yml",
        )

        assert result.context["File"] == "/path/to/databricks.yml"
        assert result.context["Operation"] == "read file"

    def test_with_original_error(self):
        """Should include original error type name in context."""
        original = ValueError("bad value")
        result = create_bundle_resource_error(
            operation="write file",
            details="Write failed",
            original_error=original,
        )

        assert result.context["Original Error"] == "ValueError"

    def test_without_optional_params(self):
        """Should not include File or Original Error keys when not provided."""
        result = create_bundle_resource_error(
            operation="validate",
            details="Validation failed",
        )

        assert "File" not in result.context
        assert "Original Error" not in result.context
        assert result.context["Operation"] == "validate"

    def test_custom_suggestions(self):
        """Should use custom suggestions when provided."""
        custom_suggestions = ["Try restarting", "Check logs"]
        result = create_bundle_resource_error(
            operation="deploy",
            details="Deploy failed",
            suggestions=custom_suggestions,
        )

        assert result.suggestions == custom_suggestions

    def test_default_suggestions(self):
        """Should use default suggestions when none provided."""
        result = create_bundle_resource_error(
            operation="deploy",
            details="Deploy failed",
        )

        assert len(result.suggestions) == 3
        assert any("databricks.yml" in s for s in result.suggestions)


class TestCreateYamlProcessingError:
    """Tests for create_yaml_processing_error factory."""

    def test_basic_creation(self):
        """Should create LHPError with CONFIG category and code 021."""
        result = create_yaml_processing_error(
            file_path="resources/pipelines.yml",
            message="Invalid indentation",
        )

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-021"
        assert "Bundle YAML processing error" in result.title
        assert "resources/pipelines.yml" in result.details
        assert "Invalid indentation" in result.details

    def test_with_line_number(self):
        """Should include line number in context when provided."""
        result = create_yaml_processing_error(
            file_path="databricks.yml",
            message="Unexpected token",
            line_number=42,
        )

        assert result.context["Line"] == "42"
        assert result.context["File"] == "databricks.yml"

    def test_with_context_string(self):
        """Should append context to details when provided."""
        result = create_yaml_processing_error(
            file_path="databricks.yml",
            message="Missing key",
            context="While parsing targets section",
        )

        assert "Context: While parsing targets section" in result.details

    def test_without_optional_params(self):
        """Should not include Line key when line_number not provided."""
        result = create_yaml_processing_error(
            file_path="databricks.yml",
            message="Parse error",
        )

        assert "Line" not in result.context
        assert result.context["File"] == "databricks.yml"

    def test_suggestions_include_yaml_hints(self):
        """Should include YAML-specific suggestions."""
        result = create_yaml_processing_error(
            file_path="test.yml",
            message="Bad syntax",
        )

        assert any("YAML syntax" in s for s in result.suggestions)
        assert any("YAML linter" in s for s in result.suggestions)


class TestCreateMissingTargetError:
    """Tests for create_missing_target_error factory."""

    def test_basic_creation(self):
        """Should create LHPError with CONFIG category and code 023."""
        result = create_missing_target_error(
            missing_targets=["staging"],
            available_targets=["dev", "prod"],
            file_path="databricks.yml",
        )

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-023"
        assert "Missing Databricks bundle targets" in result.title

    def test_formats_missing_targets(self):
        """Should format missing targets as quoted comma-separated list."""
        result = create_missing_target_error(
            missing_targets=["staging", "qa"],
            available_targets=["dev", "prod"],
            file_path="databricks.yml",
        )

        assert "'staging'" in result.details
        assert "'qa'" in result.details
        assert result.context["Missing Targets"] == "'staging', 'qa'"

    def test_formats_available_targets(self):
        """Should format available targets as quoted list in context."""
        result = create_missing_target_error(
            missing_targets=["staging"],
            available_targets=["dev", "prod"],
            file_path="databricks.yml",
        )

        assert result.context["Available Targets"] == "'dev', 'prod'"

    def test_empty_available_targets(self):
        """Should show 'none found' when no available targets."""
        result = create_missing_target_error(
            missing_targets=["staging"],
            available_targets=[],
            file_path="databricks.yml",
        )

        assert "none found" in result.details
        assert result.context["Available Targets"] == "none found"

    def test_file_path_in_context(self):
        """Should include file path in context."""
        result = create_missing_target_error(
            missing_targets=["staging"],
            available_targets=["dev"],
            file_path="/project/databricks.yml",
        )

        assert result.context["File"] == "/project/databricks.yml"


class TestCreateTemplateError:
    """Tests for create_template_error factory."""

    def test_basic_creation(self):
        """Should create LHPError with CONFIG category and code 024."""
        result = create_template_error(
            operation="fetch template",
            details="Connection timeout while downloading template",
        )

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-024"
        assert "fetch template" in result.title
        assert "Connection timeout" in result.details

    def test_with_original_error(self):
        """Should include original error type in context."""
        original = ConnectionError("DNS resolution failed")
        result = create_template_error(
            operation="download",
            details="Download failed",
            original_error=original,
        )

        assert result.context["Original Error"] == "ConnectionError"
        assert result.context["Operation"] == "download"

    def test_without_original_error(self):
        """Should not include Original Error key when not provided."""
        result = create_template_error(
            operation="process",
            details="Template rendering failed",
        )

        assert "Original Error" not in result.context
        assert result.context["Operation"] == "process"

    def test_suggestions_include_network_hints(self):
        """Should include network-related suggestions."""
        result = create_template_error(
            operation="fetch",
            details="Fetch failed",
        )

        assert any("internet connection" in s for s in result.suggestions)


class TestCreateBundleConfigError:
    """Tests for create_bundle_config_error factory."""

    def test_basic_creation(self):
        """Should create LHPError with CONFIG category and code 025."""
        result = create_bundle_config_error(
            message="Missing required 'bundle.name' field",
        )

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-025"
        assert "Bundle configuration error" in result.title
        assert "Missing required" in result.details

    def test_with_file_path(self):
        """Should include file path in context when provided."""
        result = create_bundle_config_error(
            message="Invalid config",
            file_path="/project/databricks.yml",
        )

        assert result.context["File"] == "/project/databricks.yml"

    def test_without_file_path(self):
        """Should have empty context when file_path not provided."""
        result = create_bundle_config_error(
            message="Invalid config",
        )

        assert "File" not in result.context

    def test_custom_suggestions(self):
        """Should use custom suggestions when provided."""
        custom = ["Check the docs", "Run validation"]
        result = create_bundle_config_error(
            message="Bad config",
            suggestions=custom,
        )

        assert result.suggestions == custom

    def test_default_suggestions(self):
        """Should use default suggestions when none provided."""
        result = create_bundle_config_error(
            message="Bad config",
        )

        assert len(result.suggestions) == 3
        assert any("databricks.yml" in s for s in result.suggestions)


class TestConvertBundleError:
    """Tests for convert_bundle_error dispatch function."""

    def test_missing_databricks_target_error(self):
        """Should dispatch MissingDatabricksTargetError to target resolution error."""
        error = MissingDatabricksTargetError("Target 'staging' not found")

        result = convert_bundle_error(error, "generate")

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-020"
        assert result.context["Operation"] == "target resolution"
        assert "staging" in result.details
        assert any("databricks.yml" in s for s in result.suggestions)

    def test_yaml_processing_error_with_attrs(self):
        """Should dispatch YAMLProcessingError using its file_path, line_number, and context."""
        error = YAMLProcessingError(
            message="Bad indentation",
            file_path="resources/pipeline.yml",
            line_number=15,
            context="In targets section",
        )

        result = convert_bundle_error(error, "parse bundle")

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-021"
        assert result.context["File"] == "resources/pipeline.yml"
        assert result.context["Line"] == "15"
        assert "Context: In targets section" in result.details

    def test_yaml_processing_error_without_optional_attrs(self):
        """Should handle YAMLProcessingError with minimal attrs.

        When file_path is None, getattr returns None (attr exists but is None),
        so the factory receives None rather than the 'unknown' default.
        """
        error = YAMLProcessingError(message="Parse failed")

        result = convert_bundle_error(error)

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-021"
        # file_path attr exists but is None, so getattr returns None
        assert result.context["File"] is None

    def test_bundle_configuration_error(self):
        """Should dispatch BundleConfigurationError to config error."""
        error = BundleConfigurationError("Invalid bundle structure")

        result = convert_bundle_error(error, "validate")

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-025"
        assert "Invalid bundle structure" in result.details
        assert any("databricks.yml" in s for s in result.suggestions)

    def test_template_error_with_original(self):
        """Should dispatch TemplateError and include original_error context."""
        original = TimeoutError("Connection timed out")
        error = TemplateError("Template fetch failed", original_error=original)

        result = convert_bundle_error(error, "init project")

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-024"
        assert result.context["Operation"] == "init project"
        assert result.context["Original Error"] == "TimeoutError"

    def test_template_error_without_original(self):
        """Should dispatch TemplateError without original_error."""
        error = TemplateError("Bad template syntax")

        result = convert_bundle_error(error, "render")

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-024"
        assert "Original Error" not in result.context

    def test_bundle_resource_error_base_class(self):
        """Should dispatch BundleResourceError (base class) as fallback bundle error.

        The factory extracts original_error from the BundleResourceError via getattr,
        which is the OSError that was passed in, so Original Error shows OSError.
        """
        original = OSError("Permission denied")
        error = BundleResourceError("Resource access failed", original_error=original)

        result = convert_bundle_error(error, "sync")

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-020"
        assert result.context["Operation"] == "sync"
        # The factory passes error.original_error (the OSError) to the factory
        assert result.context["Original Error"] == "OSError"

    def test_bundle_resource_error_without_original(self):
        """Should handle BundleResourceError without original_error."""
        error = BundleResourceError("Resource not found")

        result = convert_bundle_error(error)

        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-020"
        assert "Original Error" not in result.context

    def test_unknown_exception_fallback(self):
        """Should fall back to bundle resource error for unknown exception types."""
        error = RuntimeError("Something unexpected happened")

        result = convert_bundle_error(error, "unknown op")

        assert isinstance(result, LHPError)
        assert result.category == ErrorCategory.CONFIG
        assert result.code == "LHP-CFG-020"
        assert result.context["Operation"] == "unknown op"
        assert result.context["Original Error"] == "RuntimeError"
        assert "Something unexpected happened" in result.details

    def test_default_operation_name(self):
        """Should use 'bundle operation' as default operation name."""
        error = RuntimeError("fail")

        result = convert_bundle_error(error)

        assert result.context["Operation"] == "bundle operation"

    def test_dispatch_priority_missing_target_over_base(self):
        """MissingDatabricksTargetError should match before BundleResourceError."""
        error = MissingDatabricksTargetError("Target missing")

        result = convert_bundle_error(error)

        # Should match MissingDatabricksTargetError, not BundleResourceError
        assert result.context["Operation"] == "target resolution"

    def test_dispatch_priority_yaml_over_base(self):
        """YAMLProcessingError should match before BundleResourceError."""
        error = YAMLProcessingError(
            message="Parse error", file_path="test.yml"
        )

        result = convert_bundle_error(error)

        # Should match YAMLProcessingError (code 021), not BundleResourceError (020)
        assert result.code == "LHP-CFG-021"

"""Tests for centralized error handling."""

import logging
import sys
from io import StringIO
from unittest.mock import patch, MagicMock

import pytest

from lhp.utils.error_handler import ErrorHandler, ErrorContext, ProgressFormatter, ProgressStatus, handle_cli_error
from pathlib import Path
from lhp.utils.error_formatter import LHPError, ErrorCategory


class TestErrorContext:
    """Test the ErrorContext class."""
    
    def test_empty_context(self):
        """Test empty context returns empty dict."""
        context = ErrorContext()
        assert context.to_dict() == {}
    
    def test_pipeline_context(self):
        """Test pipeline context setting."""
        context = ErrorContext().set_pipeline_context("test_pipeline", "dev")
        result = context.to_dict()
        assert result["Pipeline"] == "test_pipeline"
        assert result["Environment"] == "dev"
    
    def test_chained_context(self):
        """Test chaining context methods."""
        context = (ErrorContext()
                  .set_pipeline_context("test_pipeline", "dev")
                  .set_flowgroup_context("test_flowgroup")
                  .set_action_context("test_action")
                  .add_extra("custom", "value"))
        
        result = context.to_dict()
        assert result["Pipeline"] == "test_pipeline"
        assert result["Environment"] == "dev"
        assert result["FlowGroup"] == "test_flowgroup"
        assert result["Action"] == "test_action"
        assert result["custom"] == "value"


class TestErrorHandler:
    """Test the ErrorHandler class."""
    
    def test_detect_verbose_mode_default(self):
        """Test verbose mode detection defaults to False."""
        # Set up logging environment that should detect as non-verbose
        import logging
        # Clear any existing handlers
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Set up logging with WARNING level (non-verbose)
        logging.basicConfig(level=logging.WARNING, force=True)
        
        # Explicitly set handler levels to WARNING to ensure non-verbose detection
        for handler in root_logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.WARNING)
        
        handler = ErrorHandler()
        assert handler.verbose is False
    
    def test_explicit_verbose_mode(self):
        """Test explicit verbose mode setting."""
        handler = ErrorHandler(verbose=True)
        assert handler.verbose is True
    
    def test_context_chaining(self):
        """Test context chaining methods."""
        handler = ErrorHandler()
        new_handler = handler.with_pipeline_context("test_pipeline", "dev")
        
        assert new_handler.context.pipeline == "test_pipeline"
        assert new_handler.context.environment == "dev"
        assert handler.context.pipeline is None  # Original unchanged
    
    def test_handle_lhp_error(self):
        """Test handling of LHPError (already formatted)."""
        handler = ErrorHandler(verbose=False)
        
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Test error",
            details="Test details"
        )
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, 'logger') as mock_logger:
            
            handler.handle_cli_error(lhp_error, "Test operation")
            
            # Should echo the LHPError message
            mock_echo.assert_called_once_with(str(lhp_error))
            # Should log error message (not exception) in non-verbose mode
            mock_logger.error.assert_called_once()
            mock_logger.exception.assert_not_called()
    
    def test_handle_generic_error_non_verbose(self):
        """Test handling of generic exception in non-verbose mode."""
        handler = ErrorHandler(verbose=False)
        
        generic_error = ValueError("Test error message")
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, 'logger') as mock_logger:
            
            handler.handle_cli_error(generic_error, "Test operation")
            
            # Should echo user-friendly message
            mock_echo.assert_any_call("❌ Test operation failed: Test error message")
            mock_echo.assert_any_call("📝 Use --verbose flag for detailed error information")
            # Should log error message (not exception) in non-verbose mode
            mock_logger.error.assert_called_once()
            mock_logger.exception.assert_not_called()
    
    def test_handle_generic_error_verbose(self):
        """Test handling of generic exception in verbose mode."""
        handler = ErrorHandler(verbose=True)
        
        generic_error = ValueError("Test error message")
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, 'logger') as mock_logger:
            
            handler.handle_cli_error(generic_error, "Test operation")
            
            # Should echo user-friendly message and verbose hint
            mock_echo.assert_any_call("❌ Test operation failed: Test error message")
            mock_echo.assert_any_call("🔍 Full error details logged")
            # Should log with exception details in verbose mode
            mock_logger.exception.assert_called_once()
            mock_logger.error.assert_not_called()
    
    def test_handle_generation_error_file_not_found(self):
        """Test conversion of FileNotFoundError to LHPError."""
        handler = ErrorHandler().with_action_context("test_action")
        
        file_error = FileNotFoundError("test.sql")
        result = handler.handle_generation_error(file_error, "test_action")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-IO-003"
        assert "test_action" in result.details
        assert "test.sql" in result.details
    
    def test_handle_generation_error_value_error(self):
        """Test conversion of ValueError to LHPError."""
        handler = ErrorHandler().with_action_context("test_action")
        
        value_error = ValueError("Invalid configuration")
        result = handler.handle_generation_error(value_error, "test_action")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-VAL-003"
        assert "test_action" in result.details
        assert "Invalid configuration" in result.details
    
    def test_handle_validation_error_conversion(self):
        """Test conversion of validation errors to LHPError."""
        handler = ErrorHandler()
        
        validation_error = ValueError("Missing required field")
        result = handler.handle_validation_error(validation_error, "test_component")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-VAL-004"
        assert "test_component" in result.context["Component"]
    
    def test_handle_yaml_error_conversion(self):
        """Test conversion of YAML errors to LHPError."""
        handler = ErrorHandler()
        
        # Create a mock YAML error
        class MockYAMLError(Exception):
            pass
        
        yaml_error = MockYAMLError("YAML syntax error")
        
        with patch('yaml.YAMLError', MockYAMLError):
            result = handler.handle_yaml_error(yaml_error, "test.yaml")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-004"  # YAML syntax error
        assert "test.yaml" in result.details


class TestConvenienceFunctions:
    """Test convenience functions."""
    
    def test_handle_cli_error_function(self):
        """Test the convenience handle_cli_error function."""
        test_error = ValueError("Test error")
        
        with patch('click.echo') as mock_echo, \
             patch('logging.getLogger') as mock_get_logger:
            
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            
            handle_cli_error(test_error, "Test operation", verbose=False)
            
            # Should echo user-friendly message
            mock_echo.assert_any_call("❌ Test operation failed: Test error")
            mock_echo.assert_any_call("📝 Use --verbose flag for detailed error information")
            # Should log error (not exception) in non-verbose mode
            mock_logger.error.assert_called_once()
            mock_logger.exception.assert_not_called()


class TestErrorHandlerIntegration:
    """Integration tests for error handler."""
    
    def test_logging_integration(self):
        """Test that error handler integrates with logging configuration."""
        # Setup logging to simulate CLI configuration
        logger = logging.getLogger()
        handler = logging.StreamHandler(StringIO())
        handler.setLevel(logging.INFO)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
        try:
            # Error handler should detect verbose mode from logging config
            error_handler = ErrorHandler()
            # This might be True or False depending on logging setup
            assert isinstance(error_handler.verbose, bool)
            
        finally:
            logger.removeHandler(handler)
    
    def test_context_preservation(self):
        """Test that context is preserved across operations."""
        handler = ErrorHandler()
        
        # Set context
        handler.context.set_pipeline_context("test_pipeline", "dev")
        handler.context.set_action_context("test_action")
        
        # Convert an error - context should be included
        error = ValueError("Test error")
        result = handler.handle_generation_error(error, "test_action")
        
        assert isinstance(result, LHPError)
        assert result.context["Pipeline"] == "test_pipeline"
        assert result.context["Environment"] == "dev"
        assert result.context["Action"] == "test_action"
        assert result.context["Error Type"] == "ValueError"


class TestProgressFormatter:
    """Test ProgressFormatter static methods."""

    def test_format_pipeline_progress_basic(self):
        """Test basic pipeline progress formatting."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_pipeline_progress("test_pipeline", ProgressStatus.SUCCESS)
        assert result == "✅ test_pipeline"

    def test_format_pipeline_progress_with_details(self):
        """Test pipeline progress formatting with details."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_pipeline_progress("test_pipeline", ProgressStatus.FAILED, "Error occurred")
        assert result == "❌ test_pipeline - Error occurred"

    def test_format_flowgroup_progress_default_indent(self):
        """Test flowgroup progress formatting with default indent."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_flowgroup_progress("test_flowgroup", ProgressStatus.IN_PROGRESS)
        assert result == "   ├─ test_flowgroup 🔧"

    def test_format_flowgroup_progress_custom_indent(self):
        """Test flowgroup progress formatting with custom indent."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_flowgroup_progress("test_flowgroup", ProgressStatus.SUCCESS, indent=2)
        assert result == "      ├─ test_flowgroup ✅"

    def test_format_final_flowgroup_basic(self):
        """Test final flowgroup formatting without details."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_final_flowgroup("test_flowgroup", ProgressStatus.SKIPPED)
        assert result == "   └─ test_flowgroup ⏭️"

    def test_format_final_flowgroup_with_details(self):
        """Test final flowgroup formatting with details."""
        from lhp.utils.error_handler import ProgressFormatter, ProgressStatus
        
        result = ProgressFormatter.format_final_flowgroup("test_flowgroup", ProgressStatus.WARNING, "Minor issues")
        assert result == "   └─ test_flowgroup ⚠️ - Minor issues"


class TestErrorContextExtended:
    """Extended tests for ErrorContext."""

    def test_set_file_context(self):
        """Test setting file context."""
        context = ErrorContext().set_file_context("/path/to/file.py")
        result = context.to_dict()
        assert result["File"] == "/path/to/file.py"

    def test_to_dict_with_all_contexts(self):
        """Test to_dict with all possible contexts."""
        context = (ErrorContext()
                  .set_pipeline_context("pipeline", "env")
                  .set_flowgroup_context("flowgroup")
                  .set_action_context("action")
                  .set_file_context("file.py")
                  .add_extra("custom", "value"))
        
        result = context.to_dict()
        expected = {
            "Pipeline": "pipeline",
            "Environment": "env", 
            "FlowGroup": "flowgroup",
            "Action": "action",
            "File": "file.py",
            "custom": "value"
        }
        assert result == expected


class TestErrorHandlerFormatting:
    """Test ErrorHandler formatting and display methods."""

    def test_get_terminal_width_success(self):
        """Test successful terminal width detection."""
        handler = ErrorHandler()
        with patch('shutil.get_terminal_size') as mock_get_size:
            mock_get_size.return_value.columns = 120
            width = handler._get_terminal_width()
            assert width == 120

    def test_get_terminal_width_exception_fallback(self):
        """Test terminal width fallback when exception occurs."""
        handler = ErrorHandler()
        with patch('shutil.get_terminal_size') as mock_get_size:
            mock_get_size.side_effect = OSError("Terminal not available")
            width = handler._get_terminal_width()
            assert width == 80  # Fallback width

    def test_detect_verbose_mode_exception_fallback(self):
        """Test verbose mode detection fallback when exception occurs."""
        handler = ErrorHandler()
        
        # Test the actual _detect_verbose_mode method with exception
        with patch.object(handler, 'logger') as mock_logger, \
             patch('logging.getLogger') as mock_get_logger:
            mock_get_logger.side_effect = Exception("Logging error") 
            
            # Call the method directly to test exception handling
            result = handler._detect_verbose_mode()
            assert result is False  # Should fallback to False

    def test_format_error_box_basic(self):
        """Test basic error box formatting."""
        handler = ErrorHandler()
        with patch.object(handler, '_get_terminal_width', return_value=80):
            result = handler._format_error_box("Test Error", ["Error message"])
            
            assert "╭─" in result
            assert "Test Error" in result
            assert "Error message" in result
            assert "╰" in result

    def test_format_error_box_with_error_code(self):
        """Test error box formatting with error code."""
        handler = ErrorHandler()
        with patch.object(handler, '_get_terminal_width', return_value=80):
            result = handler._format_error_box("Test Error", ["Error message"], "TEST-001")
            
            assert "📚 Error Code: TEST-001" in result

    def test_format_error_box_long_lines_wrapping(self):
        """Test error box formatting with long lines that need wrapping."""
        handler = ErrorHandler()
        with patch.object(handler, '_get_terminal_width', return_value=40):
            long_message = "This is a very long error message that should be wrapped across multiple lines"
            result = handler._format_error_box("Test", [long_message])
            
            # Should contain wrapped content
            lines = result.split('\n')
            content_lines = [line for line in lines if 'This is' in line or 'very long' in line or 'multiple lines' in line]
            assert len(content_lines) > 1  # Should be wrapped into multiple lines

    def test_display_pipeline_progress(self):
        """Test pipeline progress display."""
        from lhp.utils.error_handler import ProgressStatus
        handler = ErrorHandler()
        
        with patch('click.echo') as mock_echo:
            handler.display_pipeline_progress("test_pipeline", ProgressStatus.SUCCESS, "Completed")
            mock_echo.assert_called_once_with("✅ test_pipeline - Completed")

    def test_display_flowgroup_progress_normal(self):
        """Test normal flowgroup progress display."""
        from lhp.utils.error_handler import ProgressStatus
        handler = ErrorHandler()
        
        with patch('click.echo') as mock_echo:
            handler.display_flowgroup_progress("test_flowgroup", ProgressStatus.IN_PROGRESS)
            mock_echo.assert_called_once_with("   ├─ test_flowgroup 🔧")

    def test_display_flowgroup_progress_final(self):
        """Test final flowgroup progress display."""
        from lhp.utils.error_handler import ProgressStatus
        handler = ErrorHandler()
        
        with patch('click.echo') as mock_echo:
            handler.display_flowgroup_progress("test_flowgroup", ProgressStatus.SUCCESS, is_final=True)
            mock_echo.assert_called_once_with("   └─ test_flowgroup ✅")


class TestErrorHandlerParsing:
    """Test ErrorHandler error parsing and extraction methods."""

    def test_extract_lhp_error_details_full_match(self):
        """Test extracting all details from a formatted LHPError."""
        handler = ErrorHandler()
        error_message = """❌ Error [LHP-VAL-001]: Configuration validation failed\\n
        • Action: test_action\\n
        • Unknown: ['invalid_field']\\n
        • Section: load\\n
        • Type: load_action\\n"""
        
        details = handler._extract_lhp_error_details(error_message)
        
        assert details["code"] == "VAL-001"
        assert details["title"] == "Configuration validation failed"
        assert details["action"] == "test_action"
        assert details["unknown_fields"] == "invalid_field"
        assert details["section"] == "load"
        assert details["component_type"] == "load_action"

    def test_extract_lhp_error_details_partial_match(self):
        """Test extracting partial details from error message."""
        handler = ErrorHandler()
        error_message = "Error [LHP-CFG-002]: YAML syntax error\\nAction 'my_action' has issues"
        
        details = handler._extract_lhp_error_details(error_message)
        
        assert details["code"] == "CFG-002"
        assert details.get("action") == "my_action"  # From alternate pattern

    def test_extract_lhp_error_details_no_matches(self):
        """Test extraction with no regex matches."""
        handler = ErrorHandler()
        error_message = "Generic error message with no LHP format"
        
        details = handler._extract_lhp_error_details(error_message)
        
        assert details == {}  # Empty dict when no matches


class TestErrorHandlerAdvanced:
    """Test advanced ErrorHandler functionality."""

    def test_with_action_context_copies_existing_context(self):
        """Test with_action_context preserves existing context."""
        handler = ErrorHandler()
        handler.context.set_pipeline_context("original_pipeline", "dev")
        handler.context.add_extra("custom", "value")
        
        new_handler = handler.with_action_context("new_action")
        
        # New handler should have copied context plus new action
        assert new_handler.context.pipeline == "original_pipeline"
        assert new_handler.context.environment == "dev"
        assert new_handler.context.action == "new_action"
        assert new_handler.context.extra["custom"] == "value"
        
        # Original handler should be unchanged
        assert handler.context.action is None

    def test_handle_cli_error_formatted_error_path(self):
        """Test handle_cli_error with formatted LHPError string."""
        handler = ErrorHandler(verbose=False)
        formatted_error_message = """❌ Error [LHP-VAL-001]: Configuration Error
        ================================================
        Action 'test_action' has unknown field 'invalid_field' in load section"""
        
        error = Exception(formatted_error_message)
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, '_display_formatted_error') as mock_display:
            
            handler.handle_cli_error(error, "Test operation")
            
            # Should call _display_formatted_error instead of generic error handling
            mock_display.assert_called_once_with(formatted_error_message, "Test operation")

    def test_handle_cli_error_lhp_error_verbose_logging(self):
        """Test handle_cli_error with LHPError in verbose mode."""
        handler = ErrorHandler(verbose=True)
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001", 
            title="Test error",
            details="Test details"
        )
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, 'logger') as mock_logger:
            
            handler.handle_cli_error(lhp_error, "Test operation")
            
            mock_echo.assert_called_once_with(str(lhp_error))
            mock_logger.exception.assert_called_once_with("Test operation failed with LHPError")
            mock_logger.error.assert_not_called()

    def test_display_formatted_error_with_context(self):
        """Test _display_formatted_error with error context."""
        handler = ErrorHandler()
        handler.context.set_pipeline_context("test_pipeline", "dev")
        
        error_message = """Error [LHP-VAL-001]: Configuration validation failed\\n
        • Action: test_action\\n
        • Unknown: ['invalid_field']\\n"""
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, '_format_error_box') as mock_format:
            mock_format.return_value = "FORMATTED_ERROR_BOX"
            
            handler._display_formatted_error(error_message, "Test operation")
            
            mock_echo.assert_called_once_with("FORMATTED_ERROR_BOX")
            # Should have called _format_error_box with content including context
            call_args = mock_format.call_args[0]  # Get positional arguments
            content = call_args[1]  # content is second argument
            assert any("Pipeline: test_pipeline" in line for line in content)
            assert any("Environment: dev" in line for line in content)


class TestErrorHandlerFileHandling:
    """Test ErrorHandler file error handling methods."""

    def test_handle_file_error_file_not_found(self):
        """Test file error handling for FileNotFoundError."""
        handler = ErrorHandler()
        file_error = FileNotFoundError("test.yaml not found")
        
        with patch('lhp.utils.error_formatter.ErrorFormatter.file_not_found') as mock_formatter:
            mock_formatter.return_value = LHPError(
                category=ErrorCategory.IO,
                code_number="001",
                title="File not found",
                details="File not found"
            )
            
            result = handler.handle_file_error(file_error, "test.yaml", "reading")
            
            assert isinstance(result, LHPError)
            mock_formatter.assert_called_once_with(
                file_path="test.yaml",
                search_locations=[str(Path.cwd()), "relative to YAML file"],
                file_type="file"
            )

    def test_handle_file_error_permission_error(self):
        """Test file error handling for PermissionError."""
        handler = ErrorHandler()
        perm_error = PermissionError("Permission denied")
        
        result = handler.handle_file_error(perm_error, "/path/to/file.yaml", "writing")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-IO-004"
        assert "Permission denied" in result.details
        assert "/path/to/file.yaml" in result.details
        assert "writing" in result.details

    def test_handle_file_error_generic_error(self):
        """Test file error handling for generic exceptions."""
        handler = ErrorHandler()
        generic_error = IOError("Disk full")
        
        result = handler.handle_file_error(generic_error, "test.yaml", "writing")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-IO-005"
        assert "Disk full" in result.details
        assert "writing" in result.details

    def test_handle_file_error_already_lhp_error(self):
        """Test file error handling when error is already LHPError."""
        handler = ErrorHandler()
        lhp_error = LHPError(
            category=ErrorCategory.IO,
            code_number="999",
            title="Custom error",
            details="Custom details"
        )
        
        result = handler.handle_file_error(lhp_error, "test.yaml", "reading")
        
        # Should return the same LHPError
        assert result is lhp_error

    def test_handle_yaml_error_yaml_error_type(self):
        """Test YAML error handling for actual YAMLError."""
        handler = ErrorHandler()
        
        # Create a mock YAML error
        class MockYAMLError(Exception):
            pass
        
        yaml_error = MockYAMLError("mapping values are not allowed here")
        
        with patch('yaml.YAMLError', MockYAMLError):
            result = handler.handle_yaml_error(yaml_error, "invalid.yaml")
            
            assert isinstance(result, LHPError)
            assert result.code == "LHP-CFG-004"
            assert "YAML syntax error" in result.title
            assert "invalid.yaml" in result.details
            assert "mapping values are not allowed here" in result.details

    def test_handle_yaml_error_generic_error(self):
        """Test YAML error handling for non-YAML errors."""
        handler = ErrorHandler()
        generic_error = ValueError("Not a YAML error")
        
        result = handler.handle_yaml_error(generic_error, "test.yaml")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-CFG-005"
        assert "YAML processing error" in result.title
        assert "test.yaml" in result.details

    def test_handle_yaml_error_already_lhp_error(self):
        """Test YAML error handling when error is already LHPError.""" 
        handler = ErrorHandler()
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="999",
            title="Custom YAML error",
            details="Custom details"
        )
        
        result = handler.handle_yaml_error(lhp_error, "test.yaml")
        
        # Should return the same LHPError
        assert result is lhp_error


class TestErrorHandlerUtilities:
    """Test ErrorHandler utility methods."""

    def test_log_error_verbose_mode(self):
        """Test error logging in verbose mode."""
        handler = ErrorHandler(verbose=True)
        test_error = ValueError("Test error")
        
        with patch.object(handler, 'logger') as mock_logger:
            handler.log_error(test_error, "Test context")
            
            mock_logger.exception.assert_called_once_with("Test context - Full error details")
            mock_logger.error.assert_not_called()

    def test_log_error_non_verbose_mode(self):
        """Test error logging in non-verbose mode."""
        handler = ErrorHandler(verbose=False)
        test_error = ValueError("Test error")
        
        with patch.object(handler, 'logger') as mock_logger:
            handler.log_error(test_error, "Test context")
            
            mock_logger.error.assert_called_once_with("Test context: Test error")
            mock_logger.exception.assert_not_called()

    def test_create_dependency_error(self):
        """Test creating dependency cycle error."""
        handler = ErrorHandler()
        cycle_components = ["A", "B", "C"]
        
        with patch('lhp.utils.error_formatter.ErrorFormatter.dependency_cycle') as mock_formatter:
            mock_formatter.return_value = LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="010",
                title="Dependency cycle",
                details="Cycle detected"
            )
            
            result = handler.create_dependency_error(cycle_components)
            
            assert isinstance(result, LHPError)
            mock_formatter.assert_called_once_with(cycle_components)

    def test_create_config_conflict_error(self):
        """Test creating configuration conflict error."""
        handler = ErrorHandler()
        field_pairs = [("field1", "value1"), ("field2", "value2")]
        
        with patch('lhp.utils.error_formatter.ErrorFormatter.configuration_conflict') as mock_formatter:
            mock_formatter.return_value = LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="011",
                title="Config conflict",
                details="Conflicting fields"
            )
            
            result = handler.create_config_conflict_error("test_action", field_pairs, "test_preset")
            
            assert isinstance(result, LHPError)
            mock_formatter.assert_called_once_with("test_action", field_pairs, "test_preset")

    def test_create_unknown_type_error(self):
        """Test creating unknown type error."""
        handler = ErrorHandler()
        
        with patch('lhp.utils.error_formatter.ErrorFormatter.unknown_type_with_suggestion') as mock_formatter:
            mock_formatter.return_value = LHPError(
                category=ErrorCategory.VALIDATION,
                code_number="012",
                title="Unknown type",
                details="Type not recognized"
            )
            
            result = handler.create_unknown_type_error(
                "load_type", "invalid", ["cloudfiles", "delta"], "type: cloudfiles"
            )
            
            assert isinstance(result, LHPError)
            mock_formatter.assert_called_once_with(
                "load_type", "invalid", ["cloudfiles", "delta"], "type: cloudfiles"
            )


class TestConvenienceFunctionsExtended:
    """Test module-level convenience functions."""

    def test_get_error_handler_with_verbose(self):
        """Test get_error_handler with explicit verbose setting."""
        from lhp.utils.error_handler import get_error_handler
        
        handler = get_error_handler(verbose=True)
        assert handler.verbose is True
        assert isinstance(handler, ErrorHandler)

    def test_get_error_handler_without_verbose(self):
        """Test get_error_handler using global instance."""
        from lhp.utils.error_handler import get_error_handler
        
        handler1 = get_error_handler()
        handler2 = get_error_handler()
        # Should return the same global instance
        assert handler1 is handler2

    def test_module_handle_cli_error_function(self):
        """Test module-level handle_cli_error convenience function."""
        from lhp.utils.error_handler import handle_cli_error
        test_error = ValueError("Module test error")
        
        with patch('click.echo') as mock_echo, \
             patch('lhp.utils.error_handler.ErrorHandler') as mock_handler_class:
            
            mock_handler_instance = MagicMock()
            mock_handler_class.return_value = mock_handler_instance
            
            handle_cli_error(test_error, "Module test operation", verbose=True)
            
            # Should create ErrorHandler with verbose=True
            mock_handler_class.assert_called_once_with(True)
            # Should call handle_cli_error on the instance
            mock_handler_instance.handle_cli_error.assert_called_once_with(test_error, "Module test operation")


class TestErrorHandlerEdgeCases:
    """Test edge cases and additional error handling paths."""

    def test_handle_generation_error_already_lhp_error(self):
        """Test handle_generation_error when error is already LHPError."""
        handler = ErrorHandler()
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="999",
            title="Already formatted",
            details="Already an LHPError"
        )
        
        result = handler.handle_generation_error(lhp_error, "test_action")
        
        # Should return the same LHPError without modification
        assert result is lhp_error

    def test_handle_generation_error_generic_exception(self):
        """Test handle_generation_error with generic exception."""
        handler = ErrorHandler()
        generic_error = RuntimeError("Something went wrong")
        
        result = handler.handle_generation_error(generic_error, "test_action")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-GEN-001"
        assert "Unexpected error during code generation" in result.title
        assert "test_action" in result.details
        assert "RuntimeError" in result.context["Error Type"]

    def test_handle_validation_error_already_lhp_error(self):
        """Test handle_validation_error when error is already LHPError."""
        handler = ErrorHandler()
        lhp_error = LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="999", 
            title="Already formatted validation error",
            details="Already an LHPError"
        )
        
        result = handler.handle_validation_error(lhp_error, "test_component")
        
        # Should return the same LHPError without modification
        assert result is lhp_error

    def test_handle_validation_error_generic_exception(self):
        """Test handle_validation_error with generic exception."""
        handler = ErrorHandler()
        generic_error = RuntimeError("Validation failed unexpectedly")
        
        result = handler.handle_validation_error(generic_error, "test_component")
        
        assert isinstance(result, LHPError)
        assert result.code == "LHP-VAL-005"
        assert "Unexpected validation error" in result.title
        assert "test_component" in result.details
        assert result.context["Component"] == "test_component"

    def test_handle_cli_error_no_show_logs_hint(self):
        """Test handle_cli_error with show_logs_hint=False."""
        handler = ErrorHandler(verbose=False)
        generic_error = ValueError("Test error")
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, 'logger') as mock_logger:
            
            handler.handle_cli_error(generic_error, "Test operation", show_logs_hint=False)
            
            # Should not show the verbose flag hint
            echo_calls = [call[0][0] for call in mock_echo.call_args_list]
            verbose_hint_shown = any("--verbose flag" in call for call in echo_calls)
            assert not verbose_hint_shown

    def test_display_formatted_error_no_details_extraction(self):
        """Test _display_formatted_error when no details can be extracted."""
        handler = ErrorHandler()
        error_message = "Plain error message with no special formatting"
        
        with patch('click.echo') as mock_echo, \
             patch.object(handler, '_format_error_box') as mock_format:
            mock_format.return_value = "PLAIN_ERROR_BOX"
            
            handler._display_formatted_error(error_message, "Test operation")
            
            mock_echo.assert_called_once_with("PLAIN_ERROR_BOX")
            # Should have called _format_error_box with generic title
            call_args = mock_format.call_args[0]
            title = call_args[0]  # title is first argument
            assert title == "Configuration Error"  # Default title

    def test_set_context_method(self):
        """Test explicit set_context method."""
        handler = ErrorHandler()
        new_context = ErrorContext().set_pipeline_context("new_pipeline", "test")
        
        result = handler.set_context(new_context)
        
        # Should return the same handler instance for chaining
        assert result is handler
        assert handler.context is new_context 
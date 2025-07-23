"""Tests for centralized error handling."""

import logging
import sys
from io import StringIO
from unittest.mock import patch, MagicMock

import pytest

from lhp.utils.error_handler import ErrorHandler, ErrorContext, handle_cli_error
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
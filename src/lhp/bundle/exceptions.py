"""
Custom exceptions for LHP Databricks Asset Bundle operations.

This module defines bundle-specific exceptions for better error handling
and user feedback during bundle operations.
"""

from typing import Optional, Any


class BundleResourceError(Exception):
    """
    Exception raised when bundle resource operations fail.
    
    This is the base exception for all bundle resource-related errors,
    including YAML parsing, resource file generation, and sync operations.
    """
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """
        Initialize bundle resource error.
        
        Args:
            message: Human-readable error message
            original_error: Original exception that caused this error
        """
        self.original_error = original_error
        
        if original_error:
            super().__init__(f"{message}\nOriginal error: {original_error}")
        else:
            super().__init__(message)


class TemplateError(Exception):
    """
    Exception raised when template fetching or processing fails.
    
    This exception is used for errors related to downloading, processing,
    or applying Databricks bundle templates from GitHub.
    """
    
    def __init__(self, message: str, original_error: Optional[Exception] = None):
        """
        Initialize template error.
        
        Args:
            message: Human-readable error message
            original_error: Original exception that caused this error
        """
        self.original_error = original_error
        
        if original_error:
            super().__init__(f"{message}\nOriginal error: {original_error}")
        else:
            super().__init__(message)


class YAMLProcessingError(BundleResourceError):
    """
    Exception raised when YAML processing fails.
    
    This exception is used for errors in parsing, validating, or updating
    bundle resource YAML files.
    """
    
    def __init__(self, message: str, file_path: Optional[str] = None, 
                 line_number: Optional[int] = None, context: Optional[str] = None,
                 original_error: Optional[Exception] = None):
        """
        Initialize YAML processing error.
        
        Args:
            message: Human-readable error message
            file_path: Path to the YAML file that caused the error
            line_number: Line number where the error occurred
            context: Additional context about the error
            original_error: Original exception that caused this error
        """
        self.file_path = file_path
        self.line_number = line_number
        self.context = context
        
        # Build comprehensive error message
        full_message = f"YAML processing failed: {message}"
        if file_path:
            full_message = f"YAML processing error in {file_path}: {message}"
        if line_number:
            full_message += f" (line {line_number})"
        if context:
            full_message += f"\nContext: {context}"
            
        super().__init__(full_message, original_error)


class YAMLParsingError(YAMLProcessingError):
    """
    Alias for YAMLProcessingError for backward compatibility and clearer semantics.
    
    This exception is specifically for YAML parsing errors.
    """
    pass


class BundleConfigurationError(BundleResourceError):
    """
    Exception raised when bundle configuration is invalid.
    
    This exception is used for errors in bundle structure, missing
    configuration files, or invalid bundle settings.
    """
    pass


 
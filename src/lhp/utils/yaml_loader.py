"""Shared YAML loading utilities for LakehousePlumber.

This module provides consistent YAML loading with PyYAML, consolidating
11+ duplicated patterns across the codebase while maintaining performance.

Design: Uses PyYAML only (fast) - does NOT interfere with ruamel.yaml usage 
for databricks.yml structure preservation where needed.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, Optional, Union


def load_yaml_file(file_path: Union[Path, str], 
                  allow_empty: bool = True, 
                  error_context: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Standard PyYAML loading with consistent error handling.
    
    Consolidates the repeated pattern:
    ```python
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
        return content or {}
    except yaml.YAMLError as e:
        raise ValueError(f"Invalid YAML in {file_path}: {e}")
    ```
    
    Args:
        file_path: Path to YAML file to load
        allow_empty: If True, return {} for empty/None content; if False, return None
        error_context: Custom context string for error messages (e.g., "substitution file")
        
    Returns:
        Parsed YAML content as dict, {} if empty and allow_empty=True, or None if empty and allow_empty=False
        
    Raises:
        ValueError: If YAML is malformed or file cannot be read
        
    Examples:
        >>> config = load_yaml_file("config.yaml")
        >>> substitutions = load_yaml_file(sub_file, error_context="substitution file") 
        >>> data = load_yaml_file("data.yaml", allow_empty=False)  # Returns None if empty
    """
    file_path = Path(file_path)
    
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = yaml.safe_load(f)
        
        # Handle empty/None content based on allow_empty flag
        if content is None:
            return {} if allow_empty else None
        
        return content
        
    except yaml.YAMLError as e:
        context = error_context or f"YAML file {file_path}"
        raise ValueError(f"Invalid YAML in {context}: {e}")
    except FileNotFoundError as e:
        context = error_context or f"file {file_path}"
        raise ValueError(f"File not found: {context}")
    except Exception as e:
        # Check if it's an LHPError that should be re-raised
        # Import here to avoid circular imports
        try:
            from ..utils.error_formatter import LHPError
            if isinstance(e, LHPError):
                raise  # Re-raise LHPError as-is
        except ImportError:
            pass  # LHPError not available, continue with ValueError
        
        context = error_context or f"file {file_path}"
        raise ValueError(f"Error reading {context}: {e}")


def load_yaml_if_exists(file_path: Union[Path, str], 
                       default_value: Optional[Dict[str, Any]] = None,
                       **kwargs) -> Optional[Dict[str, Any]]:
    """
    Load YAML file if it exists, return default if missing.
    
    Consolidates the common pattern for optional configuration files:
    ```python
    config = {}
    if config_file.exists():
        try:
            with open(config_file, "r") as f:
                config = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning(f"Could not load config: {e}")
    ```
    
    Args:
        file_path: Path to YAML file to load
        default_value: Value to return if file doesn't exist (default: None)
        **kwargs: Additional arguments passed to load_yaml_file()
        
    Returns:
        Parsed YAML content if file exists, default_value if missing
        
    Examples:
        >>> config = load_yaml_if_exists("config.yaml", default_value={})
        >>> substitutions = load_yaml_if_exists(sub_file, error_context="substitution")
    """
    file_path = Path(file_path)
    
    if file_path.exists():
        return load_yaml_file(file_path, **kwargs)
    else:
        return default_value


def safe_load_yaml_with_fallback(file_path: Union[Path, str], 
                                fallback_value: Dict[str, Any] = None,
                                error_context: Optional[str] = None,
                                log_errors: bool = True) -> Dict[str, Any]:
    """
    Load YAML with fallback value if any error occurs.
    
    Consolidates the CLI pattern that uses warning logging:
    ```python
    try:
        with open(config_file, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Could not load config: {e}")
        return {}
    ```
    
    Args:
        file_path: Path to YAML file to load
        fallback_value: Value to return on any error (default: {})
        error_context: Custom context string for error messages
        log_errors: Whether to log errors (for CLI usage)
        
    Returns:
        Parsed YAML content or fallback_value on any error
        
    Examples:
        >>> config = safe_load_yaml_with_fallback("config.yaml")  # Returns {} on error
        >>> data = safe_load_yaml_with_fallback("data.yaml", fallback_value={"default": "value"})
    """
    if fallback_value is None:
        fallback_value = {}
    
    try:
        return load_yaml_file(file_path, error_context=error_context)
    except Exception as e:
        if log_errors:
            import logging
            logger = logging.getLogger(__name__)
            context = error_context or f"YAML file {file_path}"
            logger.warning(f"Could not load {context}: {e}")
        return fallback_value

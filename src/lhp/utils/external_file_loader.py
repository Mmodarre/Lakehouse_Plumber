"""Common utility for loading external files with consistent path resolution.

This module provides a centralized approach to loading external files (SQL, DDL, 
YAML, etc.) used across different generators. It eliminates code duplication and 
ensures consistent error handling and path resolution behavior.

Key features:
- Universal path resolution from project root
- Rich error messages with search locations
- Support for relative and absolute paths
- Subdirectory support
- File path detection heuristic
"""

from pathlib import Path
from typing import Union, List, Optional
from .error_formatter import ErrorFormatter


def is_file_path(value: str) -> bool:
    """Detect if a string is a file path vs inline content.
    
    This heuristic is used primarily by cloudFiles.schemaHints which accepts
    both inline DDL and file paths in a single parameter (matching Databricks API).
    
    Detection criteria:
    - Has file extension: .yaml, .yml, .json, .ddl, .sql
    - Has path separator: / or \\
    
    Args:
        value: String to check
        
    Returns:
        True if value appears to be a file path, False for inline content
        
    Examples:
        >>> is_file_path("schemas/customer.yaml")
        True
        >>> is_file_path("customer_id BIGINT, name STRING")
        False
        >>> is_file_path("sql/query.sql")
        True
    """
    if not value:
        return False
    
    value_lower = value.lower()
    
    # Check for file extensions
    file_extensions = ['.yaml', '.yml', '.json', '.ddl', '.sql']
    if any(ext in value_lower for ext in file_extensions):
        return True
    
    # Check for path separators
    if '/' in value or '\\' in value:
        return True
    
    return False


def resolve_external_file_path(
    file_path: Union[str, Path],
    base_dir: Path,
    file_type: str = "file",
    search_additional: Optional[List[Path]] = None
) -> Path:
    """Resolve external file path with rich error handling.
    
    Provides universal path resolution for all external files. Handles both
    relative and absolute paths, and provides detailed error messages with
    search locations when files are not found.
    
    Args:
        file_path: Path to file (relative or absolute)
        base_dir: Base directory for relative paths (typically project_root)
        file_type: Type of file for error messages (e.g., "SQL file", "schema file")
        search_additional: Additional directories to search (optional)
        
    Returns:
        Resolved Path object that exists
        
    Raises:
        LHPError: If file is not found, with detailed search locations
        
    Examples:
        >>> resolve_external_file_path(
        ...     "schemas/customer.yaml",
        ...     Path("/project"),
        ...     "schema file"
        ... )
        Path('/project/schemas/customer.yaml')
    """
    file_path = Path(file_path)
    
    # Handle absolute paths
    if file_path.is_absolute():
        if file_path.exists():
            return file_path
        else:
            search_locations = [f"Absolute path: {file_path}"]
            raise ErrorFormatter.file_not_found(
                str(file_path),
                search_locations,
                file_type
            )
    
    # Handle relative paths - resolve from base_dir
    resolved_path = base_dir / file_path
    
    if resolved_path.exists():
        return resolved_path
    
    # File not found - build search locations for error message
    search_locations = [
        f"Relative to project root: {resolved_path}"
    ]
    
    # Add additional search locations if provided
    if search_additional:
        for additional_dir in search_additional:
            additional_path = additional_dir / file_path
            search_locations.append(f"Additional location: {additional_path}")
    
    raise ErrorFormatter.file_not_found(
        str(file_path),
        search_locations,
        file_type
    )


def load_external_file_text(
    file_path: Union[str, Path],
    base_dir: Path,
    file_type: str = "file",
    encoding: str = "utf-8"
) -> str:
    """Load external file as text with path resolution.
    
    Convenience function that combines path resolution and text loading.
    Used for SQL, DDL, Python, and other text-based files.
    
    Args:
        file_path: Path to file (relative or absolute)
        base_dir: Base directory for relative paths (typically project_root)
        file_type: Type of file for error messages (e.g., "SQL file")
        encoding: Text encoding (default: utf-8)
        
    Returns:
        File contents as string
        
    Raises:
        LHPError: If file is not found
        
    Examples:
        >>> content = load_external_file_text(
        ...     "sql/query.sql",
        ...     Path("/project"),
        ...     "SQL file"
        ... )
    """
    resolved_path = resolve_external_file_path(
        file_path,
        base_dir,
        file_type
    )
    
    return resolved_path.read_text(encoding=encoding)


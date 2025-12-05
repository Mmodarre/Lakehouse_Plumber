"""Path normalization utilities for cross-platform compatibility."""

from pathlib import Path


def normalize_path(path_str: str) -> str:
    """Normalize path to use forward slashes for cross-platform consistency.
    
    Uses pathlib.Path.as_posix() to convert any path separator format to 
    forward slashes, ensuring consistent behavior across Windows, Mac, and Linux.
    
    Args:
        path_str: Path string with any separator format
        
    Returns:
        Path string with forward slashes only (POSIX-style)
        
    Examples:
        >>> normalize_path("src\\py_functions\\file.py")
        'src/py_functions/file.py'
        >>> normalize_path("src/py_functions/file.py")
        'src/py_functions/file.py'
    """
    if not path_str:
        return path_str
    return Path(path_str).as_posix()


def normalize_path_simple(path_str: str) -> str:
    """Normalize path using simple string replacement.
    
    Faster alternative for simple cases where Path object creation is not needed.
    Simply replaces all backslashes with forward slashes.
    
    Args:
        path_str: Path string with any separator format
        
    Returns:
        Path string with forward slashes only
        
    Examples:
        >>> normalize_path_simple("src\\py_functions\\file.py")
        'src/py_functions/file.py'
        >>> normalize_path_simple("src/py_functions/file.py")
        'src/py_functions/file.py'
    """
    if not path_str:
        return path_str
    return path_str.replace('\\', '/')


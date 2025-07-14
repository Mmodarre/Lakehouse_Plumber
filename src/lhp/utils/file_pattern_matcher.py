"""File pattern matching utility for include functionality."""

import fnmatch
import logging
from pathlib import Path
from typing import List, Set, Union


class FilePatternMatcher:
    """Utility for matching file paths against glob patterns."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def match_patterns(self, patterns: List[str], files: List[Path]) -> List[Path]:
        """Match file paths against multiple glob patterns.
        
        Args:
            patterns: List of glob patterns to match against
            files: List of file paths to filter
            
        Returns:
            List of file paths that match at least one pattern
        """
        if not patterns:
            # Empty patterns list means no filtering (backwards compatibility)
            return files
        
        # Validate patterns first
        for pattern in patterns:
            if not self.validate_pattern(pattern):
                raise ValueError(f"Invalid pattern: {pattern}")
        
        matched_files = []
        
        for file_path in files:
            # Convert to string for pattern matching
            file_str = str(file_path)
            
            # Check if file matches any pattern
            if self._matches_any_pattern(file_str, patterns):
                matched_files.append(file_path)
        
        return matched_files
    
    def validate_pattern(self, pattern: str) -> bool:
        """Validate a glob pattern.
        
        Args:
            pattern: The glob pattern to validate
            
        Returns:
            True if pattern is valid, False otherwise
        """
        if not pattern or not isinstance(pattern, str):
            return False
        
        # Check for invalid characters or patterns
        invalid_patterns = [
            "***/",  # Invalid triple asterisk
            "[unclosed",  # Unclosed bracket
        ]
        
        for invalid in invalid_patterns:
            if invalid in pattern:
                return False
        
        # Try to compile the pattern to check for regex errors
        try:
            fnmatch.translate(pattern)
            return True
        except Exception:
            return False
    
    def _matches_any_pattern(self, file_path: str, patterns: List[str]) -> bool:
        """Check if a file path matches any of the given patterns.
        
        Args:
            file_path: File path to check
            patterns: List of glob patterns
            
        Returns:
            True if file matches any pattern, False otherwise
        """
        for pattern in patterns:
            if self._matches_pattern(file_path, pattern):
                return True
        return False
    
    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        """Check if a file path matches a specific pattern.
        
        Args:
            file_path: File path to check
            pattern: Glob pattern to match against
            
        Returns:
            True if file matches pattern, False otherwise
        """
        # Handle different types of patterns
        if "**" in pattern:
            # Recursive pattern matching
            return self._matches_recursive_pattern(file_path, pattern)
        elif "*" in pattern or "?" in pattern:
            # Wildcard pattern matching
            return self._matches_wildcard_pattern(file_path, pattern)
        else:
            # Exact pattern matching
            return self._matches_exact_pattern(file_path, pattern)
    
    def _matches_recursive_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against recursive pattern (containing **)."""
        # Use pathlib for recursive matching
        path = Path(file_path)
        
        # Create a Path object from the pattern for glob matching
        # We need to match against the parent directory structure
        
        # For patterns like "bronze/**/*.yaml", we need to check if the file
        # is in the bronze directory tree and ends with .yaml
        
        # Split pattern into parts
        parts = pattern.split("**")
        if len(parts) != 2:
            # Invalid recursive pattern
            return False
        
        prefix = parts[0].rstrip("/")
        suffix = parts[1].lstrip("/")
        
        # Convert file path to Path object
        file_path_obj = Path(file_path)
        
        # Check if the file path starts with the prefix pattern
        if prefix:
            # Check if any parent directory matches the prefix
            prefix_matched = False
            for parent in [file_path_obj] + list(file_path_obj.parents):
                parent_str = str(parent)
                if fnmatch.fnmatch(parent_str, prefix + "*"):
                    # Check if the remaining path matches the suffix
                    try:
                        # Get the relative path from this parent
                        relative_path = file_path_obj.relative_to(parent)
                        if fnmatch.fnmatch(str(relative_path), suffix):
                            prefix_matched = True
                            break
                    except ValueError:
                        # relative_to failed, continue
                        continue
            
            if not prefix_matched:
                # Try direct pattern matching as fallback
                return fnmatch.fnmatch(file_path, pattern)
            
            return prefix_matched
        else:
            # Pattern starts with **, match against suffix
            return fnmatch.fnmatch(str(file_path_obj.name), suffix)
    
    def _matches_wildcard_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against wildcard pattern (containing * or ?)."""
        # Use fnmatch for wildcard matching
        return fnmatch.fnmatch(file_path, pattern)
    
    def _matches_exact_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against exact pattern."""
        # For exact patterns, check if the file name matches
        file_name = Path(file_path).name
        return file_name == pattern or fnmatch.fnmatch(file_path, pattern)


def match_patterns(patterns: List[str], files: List[Path]) -> List[Path]:
    """Convenience function to match patterns against files.
    
    Args:
        patterns: List of glob patterns
        files: List of file paths
        
    Returns:
        List of matching file paths
    """
    matcher = FilePatternMatcher()
    return matcher.match_patterns(patterns, files)


def validate_pattern(pattern: str) -> bool:
    """Convenience function to validate a single pattern.
    
    Args:
        pattern: Glob pattern to validate
        
    Returns:
        True if pattern is valid, False otherwise
    """
    matcher = FilePatternMatcher()
    return matcher.validate_pattern(pattern)


def discover_files_with_patterns(base_dir: Path, patterns: List[str]) -> List[Path]:
    """Discover files in a directory that match include patterns.
    
    Args:
        base_dir: Base directory to search in
        patterns: List of glob patterns to match against
        
    Returns:
        List of matching file paths
    """
    if not base_dir.exists():
        return []
    
    # Get all YAML files recursively
    all_files = []
    for ext in ["*.yaml", "*.yml"]:
        all_files.extend(base_dir.rglob(ext))
    
    # If no patterns, return all files (backwards compatibility)
    if not patterns:
        return all_files
    
    # Match patterns against relative paths from base_dir
    relative_files = []
    for file_path in all_files:
        try:
            relative_path = file_path.relative_to(base_dir)
            relative_files.append(relative_path)
        except ValueError:
            # File is not relative to base_dir, skip
            continue
    
    # Match patterns
    matcher = FilePatternMatcher()
    matched_relative = matcher.match_patterns(patterns, relative_files)
    
    # Convert back to absolute paths
    matched_absolute = [base_dir / rel_path for rel_path in matched_relative]
    
    return matched_absolute 
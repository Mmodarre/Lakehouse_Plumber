"""File pattern matching utility."""

import fnmatch
import logging
from pathlib import Path
from typing import List


class FilePatternMatcher:
    """Utility for matching file paths against glob patterns."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def match_patterns(self, patterns: List[str], files: List[Path]) -> List[Path]:
        if not patterns:
            # No patterns: return all files (backwards compatibility)
            self.logger.debug(
                f"No patterns specified, returning all {len(files)} file(s)"
            )
            return files

        for pattern in patterns:
            if not self.validate_pattern(pattern):
                raise ValueError(f"Invalid pattern: {pattern}")

        matched_files = []

        for file_path in files:
            file_str = str(file_path)
            if self._matches_any_pattern(file_str, patterns):
                matched_files.append(file_path)

        self.logger.debug(
            f"Pattern matching: {len(matched_files)} of {len(files)} file(s) matched "
            f"{len(patterns)} pattern(s)"
        )
        return matched_files

    def validate_pattern(self, pattern: str) -> bool:
        if not pattern or not isinstance(pattern, str):
            return False

        invalid_patterns = [
            "***/",  # Invalid triple asterisk
            "[unclosed",  # Unclosed bracket
        ]

        for invalid in invalid_patterns:
            if invalid in pattern:
                return False

        try:
            fnmatch.translate(pattern)
            return True
        except Exception as e:
            self.logger.debug(f"Pattern validation failed for '{pattern}': {e}")
            return False

    def _matches_any_pattern(self, file_path: str, patterns: List[str]) -> bool:
        for pattern in patterns:
            if self._matches_pattern(file_path, pattern):
                return True
        return False

    def _matches_pattern(self, file_path: str, pattern: str) -> bool:
        if "**" in pattern:
            return self._matches_recursive_pattern(file_path, pattern)
        if "*" in pattern or "?" in pattern:
            return self._matches_wildcard_pattern(file_path, pattern)
        return self._matches_exact_pattern(file_path, pattern)

    def _matches_recursive_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against recursive pattern (containing **)."""
        parts = pattern.split("**")
        if len(parts) != 2:
            return False

        prefix = parts[0].rstrip("/")
        suffix = parts[1].lstrip("/")

        file_path_obj = Path(file_path)

        if prefix:
            prefix_matched = False
            for parent in [file_path_obj, *list(file_path_obj.parents)]:
                parent_str = str(parent)
                if fnmatch.fnmatch(parent_str, prefix + "*"):
                    try:
                        relative_path = file_path_obj.relative_to(parent)
                        if fnmatch.fnmatch(str(relative_path), suffix):
                            prefix_matched = True
                            break
                    except ValueError:
                        continue

            if not prefix_matched:
                return fnmatch.fnmatch(file_path, pattern)

            return prefix_matched
        # Pattern starts with **, match against suffix
        return fnmatch.fnmatch(str(file_path_obj.name), suffix)

    def _matches_wildcard_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against wildcard pattern (containing * or ?)."""
        return fnmatch.fnmatch(file_path, pattern)

    def _matches_exact_pattern(self, file_path: str, pattern: str) -> bool:
        """Match file path against exact pattern."""
        file_name = Path(file_path).name
        return file_name == pattern or fnmatch.fnmatch(file_path, pattern)


def match_patterns(patterns: List[str], files: List[Path]) -> List[Path]:
    matcher = FilePatternMatcher()
    return matcher.match_patterns(patterns, files)


def validate_pattern(pattern: str) -> bool:
    matcher = FilePatternMatcher()
    return matcher.validate_pattern(pattern)


def discover_files_with_patterns(base_dir: Path, patterns: List[str]) -> List[Path]:
    if not base_dir.exists():
        return []

    _logger = logging.getLogger(__name__)
    all_files = []
    for ext in ["*.yaml", "*.yml"]:
        all_files.extend(base_dir.rglob(ext))

    # No patterns: return all files (backwards compatibility)
    if not patterns:
        return sorted(all_files)

    relative_files = []
    for file_path in all_files:
        try:
            relative_path = file_path.relative_to(base_dir)
            relative_files.append(relative_path)
        except ValueError:
            continue

    matcher = FilePatternMatcher()
    matched_relative = matcher.match_patterns(patterns, relative_files)
    matched_absolute = [base_dir / rel_path for rel_path in matched_relative]

    _logger.debug(
        f"Discovered {len(matched_absolute)} file(s) in {base_dir} matching {len(patterns)} pattern(s)"
    )

    return sorted(matched_absolute)

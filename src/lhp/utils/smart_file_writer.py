"""Smart file writer that only writes files when content changes."""

import logging
from pathlib import Path
from typing import Tuple


class SmartFileWriter:
    """File writer that only writes files when content actually changes."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.files_written = 0
        self.files_skipped = 0

    def write_if_changed(self, file_path: Path, new_content: str) -> bool:
        """Write file only if content differs from existing file.

        Args:
            file_path: Path to the file to write
            new_content: New content to write

        Returns:
            True if file was written, False if skipped (no change)
        """
        # Normalize content (remove trailing whitespace, ensure single newline at end)
        normalized_content = self._normalize_content(new_content)

        # Check if file exists and content is identical
        if file_path.exists():
            try:
                existing_content = file_path.read_text(encoding="utf-8")
                existing_normalized = self._normalize_content(existing_content)

                if existing_normalized == normalized_content:
                    self.logger.debug(f"Skipping {file_path} (no changes)")
                    self.files_skipped += 1
                    return False

            except Exception as e:
                self.logger.warning(f"Could not read existing file {file_path}: {e}")
                # Continue with writing

        # Content is different or file doesn't exist - write it
        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(normalized_content, encoding="utf-8")
            self.logger.info(f"Generated: {file_path}")
            self.files_written += 1
            return True

        except Exception as e:
            self.logger.error(f"Failed to write {file_path}: {e}")
            raise

    def _normalize_content(self, content: str) -> str:
        """Normalize file content for comparison.

        Args:
            content: Raw content string

        Returns:
            Normalized content string
        """
        # Remove trailing whitespace from each line
        lines = content.splitlines()
        normalized_lines = [line.rstrip() for line in lines]

        # Join with newlines and ensure single newline at end
        normalized = "\n".join(normalized_lines)
        if normalized and not normalized.endswith("\n"):
            normalized += "\n"

        return normalized

    def get_stats(self) -> Tuple[int, int]:
        """Get file writing statistics.

        Returns:
            Tuple of (files_written, files_skipped)
        """
        return (self.files_written, self.files_skipped)

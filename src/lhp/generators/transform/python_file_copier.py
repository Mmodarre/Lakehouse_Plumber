"""Thread-safe Python file copier for parallel flowgroup processing."""

import logging
import threading
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class PythonFunctionConflictError(ValueError):
    """Raised when different Python source files would create same destination file."""

    def __init__(self, destination: str, existing_source: str, new_source: str):
        self.destination = destination
        self.existing_source = existing_source
        self.new_source = new_source

        message = (
            f"Python function naming conflict detected:\n"
            f"  Existing: {existing_source} → {destination}\n"
            f"  New:      {new_source} → {destination}\n\n"
            f"Resolution required:\n"
            f"  1. Rename one of the Python functions\n"
            f"  2. Move functions to different directories\n"
            f"  3. Update YAML module_path to use different name"
        )
        super().__init__(message)


class PythonFileCopier:
    """
    Thread-safe coordinator for Python file copying during parallel generation.

    Ensures that when multiple flowgroups reference the same Python file,
    only one thread performs the copy while others wait and reuse the result.
    """

    def __init__(self):
        """Initialize the copier with empty registry and lock."""
        self._copied_files: Dict[str, str] = {}  # dest_path -> source_path
        self._lock = threading.Lock()
        self._logger = logging.getLogger(__name__)

    def copy_python_file(
        self,
        source_path: str,
        dest_path: Path,
        content: str
    ) -> bool:
        """
        Copy Python file in a thread-safe manner.

        Args:
            source_path: Original source path (e.g., "py_functions/timestamp_converter.py")
            dest_path: Destination path for the copied file
            content: Full content to write (including header)

        Returns:
            True if file was copied, False if already copied by another thread

        Raises:
            PythonFunctionConflictError: If different source tries to write same destination
        """
        dest_key = str(dest_path)

        with self._lock:
            if dest_key in self._copied_files:
                existing_source = self._copied_files[dest_key]
                # Normalize paths for comparison: replace backslashes first, then use as_posix()
                # This handles both Windows native paths and string literals with backslashes
                normalized_existing = existing_source.replace('\\', '/')
                normalized_new = source_path.replace('\\', '/')
                if normalized_existing != normalized_new:
                    # Real conflict - different sources targeting same destination
                    raise PythonFunctionConflictError(
                        destination=dest_key,
                        existing_source=existing_source,
                        new_source=source_path
                    )
                # Same source - already copied, skip
                self._logger.debug(f"Skipping Python file copy (already copied): {source_path} → {dest_path.name}")
                return False

            # Register this file as being copied (normalize to forward slashes for consistency)
            self._copied_files[dest_key] = source_path.replace('\\', '/')
            self._logger.debug(f"Copying Python file: {source_path} → {dest_path.name}")

        # Write file outside the lock (safe - we own this destination now)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        dest_path.write_text(content)
        return True

    def ensure_init_file(self, custom_functions_dir: Path) -> None:
        """
        Ensure __init__.py exists in custom_python_functions directory.
        
        Thread-safe - only creates once even if called from multiple threads.
        
        Args:
            custom_functions_dir: Directory where custom Python functions are stored
        """
        init_key = str(custom_functions_dir / "__init__.py")

        with self._lock:
            if init_key in self._copied_files:
                return  # Already created
            self._copied_files[init_key] = "__init__"

        custom_functions_dir.mkdir(parents=True, exist_ok=True)
        init_file = custom_functions_dir / "__init__.py"
        init_file.write_text("# Generated package for custom Python functions\n")
        self._logger.debug(f"Created __init__.py in {custom_functions_dir}")

    def get_copied_files(self) -> Dict[str, str]:
        """
        Get mapping of all copied files (for debugging/logging).
        
        Returns:
            Dictionary mapping destination paths to source paths
        """
        with self._lock:
            return dict(self._copied_files)


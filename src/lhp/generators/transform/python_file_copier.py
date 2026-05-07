"""Thread-safe Python file copier for parallel flowgroup processing."""

import logging
import threading
from pathlib import Path
from typing import Dict, Optional

from ...utils.error_formatter import ErrorCategory, LHPValidationError

logger = logging.getLogger(__name__)


class PythonFunctionConflictError(LHPValidationError):
    """Raised when different Python source files would create same destination file."""

    def __init__(self, destination: str, existing_source: str, new_source: str):
        self.destination = destination
        self.existing_source = existing_source
        self.new_source = new_source

        super().__init__(
            category=ErrorCategory.VALIDATION,
            code_number="019",
            title="Python function naming conflict",
            details=(
                f"Two different Python source files would create the same destination file.\n"
                f"  Existing: {existing_source} -> {destination}\n"
                f"  New:      {new_source} -> {destination}"
            ),
            suggestions=[
                "Rename one of the Python functions",
                "Move functions to different directories",
                "Update YAML module_path to use a different name",
            ],
            context={
                "Destination": destination,
                "Existing source": existing_source,
                "New source": new_source,
            },
        )


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

    def copy_python_file(self, source_path: str, dest_path: Path, content: str) -> bool:
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
                normalized_existing = existing_source.replace("\\", "/")
                normalized_new = source_path.replace("\\", "/")
                if normalized_existing != normalized_new:
                    # Real conflict - different sources targeting same destination
                    raise PythonFunctionConflictError(
                        destination=dest_key,
                        existing_source=existing_source,
                        new_source=source_path,
                    )
                # Same source - already copied, skip
                self._logger.debug(
                    f"Skipping Python file copy (already copied): {source_path} → {dest_path.name}"
                )
                return False

            # Register this file as being copied (normalize to forward slashes for consistency)
            self._copied_files[dest_key] = source_path.replace("\\", "/")
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

    def copy_user_module(
        self,
        source_file: Path,
        module_path: str,
        custom_functions_dir: Path,
        context: dict,
    ) -> str:
        """Copy a user Python module into the custom_functions directory.

        Reads the source file, applies YAML substitutions and propagates secret
        references, prepends an LHP-SOURCE provenance header, ensures the
        package's ``__init__.py`` exists, copies the module thread-safely, and
        registers both files with the state manager (when present).

        Caller is responsible for resolving ``source_file`` (which must exist)
        and ``custom_functions_dir`` (typically ``output_dir / "custom_python_functions"``).
        Dry-run handling (skipping when ``output_dir is None``) belongs to the
        caller.

        Args:
            source_file: Resolved path to the user's source ``.py`` file.
            module_path: Original user-facing relative path; used in the
                LHP-SOURCE header so generated headers point back to the
                original location.
            custom_functions_dir: Destination directory for the copied module.
            context: Generation context. Reads ``substitution_manager``,
                ``secret_references``, ``state_manager``, ``source_yaml``,
                ``environment``, ``flowgroup``.

        Returns:
            The module stem (e.g. ``"api_source"`` for ``"data/api_source.py"``).
        """
        from ...utils.smart_file_writer import build_lhp_source_header

        module_name = Path(module_path).stem
        dest_file = custom_functions_dir / f"{module_name}.py"

        original_content = source_file.read_text()

        substitution_mgr = context.get("substitution_manager")
        if substitution_mgr is not None:
            original_content = substitution_mgr._process_string(original_content)

            secret_refs = substitution_mgr.get_secret_references()
            secret_target = context.get("secret_references")
            if secret_target is not None:
                secret_target.update(secret_refs)

        full_content = build_lhp_source_header(module_path) + original_content

        self.ensure_init_file(custom_functions_dir)
        file_copied = self.copy_python_file(module_path, dest_file, full_content)

        if file_copied:
            state_manager = context.get("state_manager")
            source_yaml = context.get("source_yaml")
            flowgroup = context.get("flowgroup")
            if state_manager and source_yaml and flowgroup:
                env = context.get("environment", "unknown")
                init_file = custom_functions_dir / "__init__.py"
                state_manager.track_generated_file(
                    generated_path=init_file,
                    source_yaml=source_yaml,
                    environment=env,
                    pipeline=flowgroup.pipeline,
                    flowgroup=flowgroup.flowgroup,
                )
                state_manager.track_generated_file(
                    generated_path=dest_file,
                    source_yaml=source_yaml,
                    environment=env,
                    pipeline=flowgroup.pipeline,
                    flowgroup=flowgroup.flowgroup,
                )
                self._logger.debug(
                    f"Tracked custom module files for module_path={module_path}"
                )

        return module_name

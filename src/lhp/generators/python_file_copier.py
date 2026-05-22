"""Thread-safe Python file copier for parallel flowgroup processing."""

import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional

from ..utils.error_formatter import ErrorCategory, LHPValidationError
from ..utils.external_file_loader import resolve_external_file_path

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CopiedModuleRecord:
    """Pure-compute description of a user Python module copy.

    Produced in Phase A by :func:`compute_copy_record` with no side
    effects — no filesystem writes, no state mutation. Replayed in
    Phase B by :meth:`PythonFileCopier.apply_copy_record`.
    """

    source_path: str
    dest_path: Path
    content: str
    module_path: str
    custom_functions_dir: Path


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

    def __reduce__(self):
        # Override the parent ``LHPError.__reduce__`` (which reconstructs
        # with the 8-arg base ``__init__`` signature) so this subclass's
        # custom 3-arg ``__init__(destination, existing_source,
        # new_source)`` survives the spawn-pool pickle round-trip.
        # Without this override, ``PipelineDelta.lhp_error`` cannot ship
        # this subclass across the worker→main boundary and the original
        # LHP-VAL-019 error is silently replaced by an unpickling
        # ``TypeError`` on the parent side.
        return (
            self.__class__,
            (self.destination, self.existing_source, self.new_source),
        )


class PythonFileCopier:
    """Per-worker file-copy coordinator with intra-pipeline dedup.

    Lives inside a worker process. Each worker constructs its own
    copier; the ``threading.Lock`` serialises copies within one
    worker process (e.g. when multiple flowgroups in the same
    pipeline reference the same module). The lock is NEVER pickled
    across the spawn boundary.

    When multiple flowgroups reference the same Python file, only one
    call performs the copy; subsequent calls are no-ops so callers do
    not double-track state.
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

    def apply_copy_record(
        self,
        record: "CopiedModuleRecord",
    ) -> None:
        """Replay a Phase-A-captured copy: ensure init, copy module.

        Calls :meth:`ensure_init_file`, then :meth:`copy_python_file` (both
        lock-protected for intra-pipeline dedup within this worker process).
        When dedup suppresses the write (same record re-applied), this is a
        no-op. Observability of what was written is exposed via
        :meth:`get_copied_files`.

        Args:
            record: Result of :func:`compute_copy_record`.
        """
        self.ensure_init_file(record.custom_functions_dir)
        self.copy_python_file(record.source_path, record.dest_path, record.content)


def compute_copy_record(
    source_file: Optional[Path],
    module_path: str,
    custom_functions_dir: Path,
    context: dict,
    *,
    inline_source: Optional[str] = None,
) -> CopiedModuleRecord:
    """Phase A pure compute: read source, substitute, build header.

    Returns a :class:`CopiedModuleRecord` describing the file that would be
    written, without touching the filesystem (writes) or the state manager.
    Safe to call from worker threads — the only filesystem touch is reading
    the source file, which is idempotent.

    The caller-supplied ``context`` may carry a ``substitution_manager`` and
    a ``secret_references`` collection. When present, the substitution
    manager processes the file contents and any discovered secret references
    are merged into ``secret_references`` for downstream rendering. Both the
    substitution manager and the secret_references dict are expected to be
    per-flowgroup (per-worker) so this mutation is thread-safe.

    Args:
        source_file: Resolved path to the user's source ``.py`` file. May
            be ``None`` when ``inline_source`` is supplied.
        module_path: User-facing relative path; used in the LHP-SOURCE
            header so generated headers point back to the original location.
        custom_functions_dir: Destination directory for the copied module.
        context: Generation context. Reads ``substitution_manager`` and
            ``secret_references`` only.
        inline_source: Optional pre-loaded source content. When provided,
            the disk read is skipped — used for synthetic flowgroups
            (e.g. monitoring) whose source is generated in memory.

    Returns:
        A :class:`CopiedModuleRecord` describing the planned copy. The
        record's ``dest_path`` is ``custom_functions_dir / f"{stem}.py"``.
    """
    from ..utils.file_header import build_lhp_source_header

    module_name = Path(module_path).stem
    dest_file = custom_functions_dir / f"{module_name}.py"

    if inline_source is not None:
        original_content = inline_source
    else:
        assert source_file is not None, (
            "compute_copy_record requires either source_file or inline_source"
        )
        original_content = source_file.read_text()

    substitution_mgr = context.get("substitution_manager")
    if substitution_mgr is not None:
        original_content = substitution_mgr._process_string(original_content)

        # Track secret references discovered during substitution. The
        # substitution manager is the canonical source; the context-level
        # accumulator mirrors them for downstream consumers (e.g. tests).
        secret_refs = substitution_mgr.secret_references
        secret_target = context.get("secret_references")
        if secret_target is not None:
            secret_target.update(secret_refs)

    full_content = build_lhp_source_header(module_path) + original_content

    return CopiedModuleRecord(
        source_path=module_path,
        dest_path=dest_file,
        content=full_content,
        module_path=module_path,
        custom_functions_dir=custom_functions_dir,
    )


def copy_user_module_for_pipeline(
    module_path: str,
    context: dict,
    *,
    component_label: str,
) -> str:
    """Resolve, validate, and copy a user Python module into the pipeline output.

    Shared by python LOAD/TRANSFORM and the custom_datasource/custom_sink
    generators so that all four emit identical error messages and follow the
    same flat ``custom_python_functions/<leaf>.py`` layout.

    Resolves ``module_path`` relative to ``context["spec_dir"]`` (or CWD) via
    :func:`resolve_external_file_path`, validates that flowgroup context is
    present, then delegates to :meth:`PythonFileCopier.copy_user_module`. In
    dry-run (``context["output_dir"] is None``) the copy is skipped and the
    leaf module name is returned anyway so import lines can still be rendered.

    Synthetic flowgroups (e.g. the monitoring pipeline) may pre-populate the
    ``context["auxiliary_files"]`` mapping (sourced from
    :class:`FlowGroupContext`): ``{module_path: source_str}``. When
    ``module_path`` is found there, the on-disk lookup is skipped and the
    source content is copied directly into ``custom_python_functions/<leaf>.py``.

    Args:
        module_path: User-facing path to the module file (e.g.
            ``"loaders/my_loader.py"``). Must end in ``.py`` — caller is
            responsible for that check; here we treat it as an opaque path
            and use ``Path(module_path).stem`` as the import-time module name.
        context: Generation context. Reads ``spec_dir``, ``flowgroup``,
            ``output_dir``, ``python_file_copier``, ``auxiliary_files`` plus
            the keys :meth:`PythonFileCopier.copy_user_module` consumes.
        component_label: Human-readable label inserted into error messages,
            e.g. ``"Python load action"``, ``"Custom data source"``.

    Returns:
        The leaf module name (e.g. ``"my_loader"`` for
        ``"loaders/my_loader.py"``), suitable for use in
        ``from custom_python_functions.<name> import ...``.

    Raises:
        LHPError: when the resolved source file does not exist (and no inline
            source is registered on the context).
        LHPValidationError: code ``015`` when flowgroup context is missing.
    """
    flowgroup = context.get("flowgroup")
    inline_sources = context.get("auxiliary_files") or {}
    inline_source = inline_sources.get(module_path)

    if inline_source is None:
        project_root = context.get("spec_dir") or Path.cwd()
        source_file: Optional[Path] = resolve_external_file_path(
            module_path,
            base_dir=project_root,
            file_type=f"{component_label} module file",
        )
    else:
        source_file = None

    if not flowgroup:
        raise LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number="015",
            title=f"Missing flowgroup context for {component_label} file copying",
            details=(
                "Flowgroup context is required for Python file copying but "
                "was not provided."
            ),
            suggestions=[
                "Ensure the action is executed within a flowgroup context",
                "Check that the flowgroup configuration is valid",
            ],
            context={"Module Path": module_path, "Component": component_label},
        )

    output_dir = context.get("output_dir")
    if output_dir is None:
        return Path(module_path).stem

    custom_functions_dir = output_dir / "custom_python_functions"

    # Phase A collect mode: when the caller supplies a phase_a_records
    # list on the context, append a CopiedModuleRecord and let Phase B
    # replay it later — no disk write, no state mutation here.
    # When the carrier is absent (test-only direct generator drives),
    # compute and apply immediately: file is written but state is not
    # tracked from this path.
    record = compute_copy_record(
        source_file,
        module_path,
        custom_functions_dir,
        context,
        inline_source=inline_source,
    )

    phase_a_records = context.get("phase_a_records")
    if phase_a_records is not None:
        phase_a_records.append(record)
        return Path(module_path).stem

    python_copier = context.get("python_file_copier") or PythonFileCopier()
    python_copier.apply_copy_record(record)
    return Path(module_path).stem

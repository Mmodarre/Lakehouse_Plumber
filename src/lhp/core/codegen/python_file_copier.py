"""Thread-safe Python file copier for parallel flowgroup processing."""

import logging
import threading
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from ...errors import ErrorCategory, LHPValidationError, PythonFunctionConflictError
from ...models.processing import CopiedModuleRecord
from ..loaders.external_file_loader import resolve_external_file_path

logger = logging.getLogger(__name__)


def _register_copy(
    copied_files: Dict[str, str],
    dest_key: str,
    source_path: str,
    log: logging.Logger,
) -> bool:
    """Decide whether a module copy should proceed, mutating the registry.

    Shared dedup + conflict core for both the write path
    (:meth:`PythonFileCopier.copy_python_file`) and the no-write planning
    path (:meth:`PythonFileCopier.plan`). Pure with respect to the
    filesystem: the only effect is registering ``dest_key`` in
    ``copied_files`` on a first-seen destination. Callers hold any required
    lock around this function; it does no locking itself.

    Args:
        copied_files: Destination-path -> normalized-source-path registry.
            Mutated in place when ``dest_key`` is seen for the first time.
        dest_key: ``str(dest_path)`` for the module being copied.
        source_path: Original (possibly Windows-style) source path.
        log: Logger used for the dedup-skip / copying debug messages, so the
            write path's observable logging is preserved when this is reused.

    Returns:
        True if the destination is newly registered and the caller should
        write it; False if an identical source already claimed it (dedup
        skip).

    Raises:
        PythonFunctionConflictError: If a *different* source already claimed
            ``dest_key`` (code 019).
    """
    if dest_key in copied_files:
        existing_source = copied_files[dest_key]
        # Normalize backslashes so Windows native paths and backslash string
        # literals compare equal.
        normalized_existing = existing_source.replace("\\", "/")
        normalized_new = source_path.replace("\\", "/")
        if normalized_existing != normalized_new:
            raise PythonFunctionConflictError(
                destination=dest_key,
                existing_source=existing_source,
                new_source=source_path,
            )
        log.debug(
            f"Skipping Python file copy (already copied): {source_path} → {Path(dest_key).name}"
        )
        return False

    copied_files[dest_key] = source_path.replace("\\", "/")
    log.debug(f"Copying Python file: {source_path} → {Path(dest_key).name}")
    return True


def _register_init(
    copied_files: Dict[str, str],
    custom_functions_dir: Path,
) -> bool:
    """Decide whether the package ``__init__.py`` should be written.

    Shared init-registration core for both the write path
    (:meth:`PythonFileCopier.ensure_init_file`) and the no-write planning
    path (:meth:`PythonFileCopier.plan`). Registers the init key in
    ``copied_files`` on first sight; no filesystem effect. Callers hold any
    required lock around this function.

    Args:
        copied_files: Registry shared with :func:`_register_copy`. Mutated in
            place when the init key is seen for the first time.
        custom_functions_dir: Directory whose ``__init__.py`` is being
            ensured.

    Returns:
        True if the init file is newly registered and should be written;
        False if it was already registered.
    """
    init_key = str(custom_functions_dir / "__init__.py")
    if init_key in copied_files:
        return False  # Already created
    copied_files[init_key] = "__init__"
    return True


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

    def __init__(self) -> None:
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
        with self._lock:
            should_write = _register_copy(
                self._copied_files, str(dest_path), source_path, self._logger
            )

        if not should_write:
            return False

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
        with self._lock:
            should_write = _register_init(self._copied_files, custom_functions_dir)

        if not should_write:
            return  # Already created

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

    def plan(
        self, records: Sequence["CopiedModuleRecord"]
    ) -> Tuple["CopiedModuleRecord", ...]:
        """Compute which records would be written, with zero disk writes.

        Runs the exact dedup + conflict-detection rule used by the write path
        (:meth:`apply_copy_record` -> :meth:`ensure_init_file` /
        :meth:`copy_python_file`) over ``records`` in order, against a private
        throwaway registry. No file I/O occurs, and ``self`` is not mutated:
        this neither writes files nor touches the instance registry returned
        by :meth:`get_copied_files`.

        For each record, the init-file registration is applied first (mirroring
        :meth:`apply_copy_record`), then the module-copy decision. A record is
        included in the result only when its module copy is newly registered;
        records suppressed by dedup (an identical source already claimed the
        same destination) are dropped. A cross-source conflict raises, exactly
        as the write path would.

        Args:
            records: Phase-A records to plan, in the order they would be
                replayed (matching ``PipelineProcessor._apply_copy_records``).

        Returns:
            The deduped records that would be written, in input order. The
            result is deterministic: the same input always yields the same
            tuple.

        Raises:
            PythonFunctionConflictError: code 019, when two records with
                different sources target the same destination.
        """
        registry: Dict[str, str] = {}
        planned: List["CopiedModuleRecord"] = []
        for record in records:
            _register_init(registry, record.custom_functions_dir)
            if _register_copy(
                registry, str(record.dest_path), record.source_path, logger
            ):
                planned.append(record)
        return tuple(planned)


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
    from ...utils.file_header import build_lhp_source_header

    module_name = Path(module_path).stem
    dest_file = custom_functions_dir / f"{module_name}.py"

    if inline_source is not None:
        original_content = inline_source
    else:
        assert (
            source_file is not None
        ), "compute_copy_record requires either source_file or inline_source"
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

    # When the caller supplies a phase_a_records list on the context, append
    # a CopiedModuleRecord and let the replay step handle disk writes — no
    # disk write, no state mutation here. When the carrier is absent
    # (test-only direct generator drives), compute and apply immediately:
    # file is written but state is not tracked from this path.
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

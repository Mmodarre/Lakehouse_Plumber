"""Thread-safe Python file copier for parallel flowgroup processing."""

import logging
import threading
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from ...errors import ErrorFactory, PythonFunctionConflictError, codes
from ...models.processing import CopiedModuleRecord
from ...utils.file_header import write_normalized
from ..loaders.external_file_loader import resolve_external_file_path
from .imports import parse_user_module
from .python_dependency_resolver import resolve_local_closure
from .python_import_rewriter import rewrite_local_imports

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
        write_normalized(dest_path, content)
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
        write_normalized(init_file, "# Generated package for custom Python functions\n")
        self._logger.debug(f"Created __init__.py in {custom_functions_dir}")

    def apply_copy_record(
        self,
        record: "CopiedModuleRecord",
    ) -> None:
        """Replay a Phase-A-captured copy: ensure init, copy module.

        Calls :meth:`ensure_init_file`, then :meth:`copy_python_file` (both
        lock-protected for intra-pipeline dedup within this worker process).
        When dedup suppresses the write (same record re-applied), this is a
        no-op. The set of written destinations is tracked in the instance
        registry (``self._copied_files``).

        Args:
            record: One record from :func:`compute_copy_records`.
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
        this neither writes files nor touches the instance registry
        (``self._copied_files``).

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


def _apply_substitution(content: str, context: dict) -> str:
    """Run body substitution and mirror discovered secret refs onto ``context``.

    No-op when no ``substitution_manager`` is present. The manager is the
    canonical secret-reference source; ``context["secret_references"]`` mirrors
    them for downstream consumers. Both are per-flowgroup (per-worker), so the
    mutation is thread-safe and the repeated ``set.update`` across closure files
    is idempotent.
    """
    substitution_mgr = context.get("substitution_manager")
    if substitution_mgr is None:
        return content
    processed: str = substitution_mgr._process_string(content)
    secret_target = context.get("secret_references")
    if secret_target is not None:
        secret_target.update(substitution_mgr.secret_references)
    return processed


def _build_module_content(
    source_path: Path,
    header_path: str,
    root: Path,
    context: dict,
    build_header: Callable[[str], str],
) -> str:
    """Read, rewrite-then-substitute, and prepend the header for one module.

    Ordering is load-bearing for the byte-offset import surgery in
    :func:`rewrite_local_imports`: it consumes offsets from a tree parsed from
    the file's *original* (un-substituted) text — exactly the tree
    :func:`parse_user_module` cached. So this rewrites the original (cache hit,
    no re-parse), THEN substitutes (which may shift byte lengths), THEN prepends
    the header. Substituting first could shift the import spans out from under
    the cached offsets and corrupt the rewrite.
    """
    cache = context.get("source_parse_cache")
    original = source_path.read_text(encoding="utf-8")
    tree = parse_user_module(source_path, cache=cache)
    rewritten = rewrite_local_imports(original, tree, root)
    return build_header(header_path) + _apply_substitution(rewritten, context)


def compute_copy_records(
    source_file: Optional[Path],
    module_path: str,
    custom_functions_dir: Path,
    context: dict,
    *,
    inline_source: Optional[str] = None,
) -> list[CopiedModuleRecord]:
    """Phase A pure compute: the entry module plus its local-helper closure.

    Returns the :class:`CopiedModuleRecord` list for ``module_path`` without
    touching the filesystem (writes) or the state manager — worker-thread safe;
    the only I/O is reading the source files. Absolute-local imports are
    prefix-rewritten to ``custom_python_functions.``; relative/external imports
    are preserved.

    Record identity:

    - Entry: flat ``dest = custom_functions_dir / f"{stem}.py"``,
      ``source_path = module_path`` (the user-facing string).
    - Closure: ``dest = custom_functions_dir / rel_path`` (structure mirrored),
      ``source_path = str(resolved_helper_path)`` — the REAL on-disk path, so a
      helper shared by two entries dedups and a cross-source clash raises
      ``LHP-VAL-019`` via :func:`_register_copy`.

    Header paths are project-root-relative: the entry uses ``module_path``; each
    helper uses ``Path(module_path).parent / rel_path`` (``module_path``'s dir is
    the closure root ``R`` relative to the project root; ``rel_path`` is relative
    to ``R``). This string join is used rather than
    ``resolved_path.relative_to(project_root)`` because the resolver fully
    resolves helper paths while ``spec_dir`` is not, so ``relative_to`` would
    spuriously fail (e.g. ``/var`` vs ``/private/var`` on macOS).

    When ``source_file is None`` (the ``inline_source`` / ``auxiliary_files``
    synthetic-flowgroup path — no disk root, no local-helper concept), returns a
    single entry record with NO closure discovery and NO rewrite, byte-identical
    to the pre-closure behavior.

    Args:
        source_file: Path to the user's source ``.py`` file (the closure entry;
            its resolved parent is the import root ``R``). ``None`` ⇒ inline.
        module_path: User-facing project-root-relative path; the LHP-SOURCE
            header and the entry record's ``source_path``.
        custom_functions_dir: Destination directory for the copied modules.
        context: Reads ``substitution_manager``, ``secret_references``,
            ``source_parse_cache``.
        inline_source: Pre-loaded source; skips the disk read and closure
            discovery (synthetic flowgroups, e.g. monitoring).

    Returns:
        The entry record followed by closure records (helpers + any synthesized
        namespace ``__init__.py``), in resolver order.
    """
    from ...utils.file_header import build_lhp_source_header, helper_header_path

    dest_file = custom_functions_dir / f"{Path(module_path).stem}.py"

    if source_file is None:
        assert inline_source is not None, (
            "compute_copy_records requires either source_file or inline_source"
        )
        content = build_lhp_source_header(module_path) + _apply_substitution(
            inline_source, context
        )
        return [
            CopiedModuleRecord(
                source_path=module_path,
                dest_path=dest_file,
                content=content,
                module_path=module_path,
                custom_functions_dir=custom_functions_dir,
            )
        ]

    # Resolve the import root once. The resolver resolves every discovered
    # helper path (``Path.resolve()``) before taking it ``relative_to(root)``,
    # so ``root`` must be the resolved entry directory or that subtraction
    # raises ``ValueError`` whenever a symlink sits on the path (e.g. the
    # macOS ``/tmp`` -> ``/private/tmp`` link, or a symlinked project dir).
    # The rewriter only probes ``root / seg`` on disk, which is symlink-
    # transparent, so a resolved root is correct there too.
    root = source_file.resolve().parent
    records: List[CopiedModuleRecord] = [
        CopiedModuleRecord(
            source_path=module_path,
            dest_path=dest_file,
            content=_build_module_content(
                source_file, module_path, root, context, build_lhp_source_header
            ),
            module_path=module_path,
            custom_functions_dir=custom_functions_dir,
        )
    ]

    closure = resolve_local_closure(
        source_file, root, cache=context.get("source_parse_cache")
    )
    for rm in closure:
        header_path = helper_header_path(module_path, rm.rel_path)
        if rm.is_synthesized_init:
            content = build_lhp_source_header(header_path)
        else:
            content = _build_module_content(
                rm.source_path, header_path, root, context, build_lhp_source_header
            )
        records.append(
            CopiedModuleRecord(
                source_path=str(rm.source_path),
                dest_path=custom_functions_dir / rm.rel_path,
                content=content,
                module_path=module_path,
                custom_functions_dir=custom_functions_dir,
            )
        )

    return records


def copy_user_module_for_pipeline(
    module_path: str,
    context: dict,
    *,
    component_label: str,
) -> str:
    """Resolve, validate, and copy a user Python module into the pipeline output.

    Shared by python LOAD/TRANSFORM and the custom_datasource/custom_sink
    generators so all four emit identical error messages. Resolves
    ``module_path`` relative to ``context["spec_dir"]`` (or CWD) via
    :func:`resolve_external_file_path`, validates flowgroup context, then
    delegates to :func:`compute_copy_records` (entry + transitive helper
    closure) and replays the records. In dry-run (``output_dir is None``) the
    copy is skipped and the leaf name is still returned so import lines render.

    Synthetic flowgroups (e.g. monitoring) may pre-populate
    ``context["auxiliary_files"]`` (``{module_path: source_str}``); a match
    there skips the on-disk lookup and copies the inline source directly (no
    closure discovery).

    Args:
        module_path: User-facing path to the module file (e.g.
            ``"loaders/my_loader.py"``). The caller guarantees the ``.py``
            suffix; ``Path(module_path).stem`` is the import-time module name.
        context: Reads ``spec_dir``, ``flowgroup``, ``output_dir``,
            ``python_file_copier``, ``auxiliary_files``, ``phase_a_records``,
            plus the keys :func:`compute_copy_records` consumes.
        component_label: Label inserted into error messages, e.g.
            ``"Python load action"``, ``"Custom data source"``.

    Returns:
        The leaf module name (``"my_loader"`` for ``"loaders/my_loader.py"``),
        for use in ``from custom_python_functions.<name> import ...``.

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
        raise ErrorFactory.validation_error(
            codes.VAL_015,
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
    # the records and let the replay step handle disk writes — no disk write,
    # no state mutation here. When the carrier is absent (test-only direct
    # generator drives), compute and apply immediately: files are written but
    # state is not tracked from this path. Either way, the entry's leaf module
    # name is returned unchanged so import lines never churn.
    records = compute_copy_records(
        source_file,
        module_path,
        custom_functions_dir,
        context,
        inline_source=inline_source,
    )

    phase_a_records = context.get("phase_a_records")
    if phase_a_records is not None:
        phase_a_records.extend(records)
        return Path(module_path).stem

    python_copier = context.get("python_file_copier") or PythonFileCopier()
    for record in records:
        python_copier.apply_copy_record(record)
    return Path(module_path).stem

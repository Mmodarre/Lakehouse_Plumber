"""State persistence service for LakehousePlumber.

Sole on-disk format is **per-pipeline shards**: ``.lhp_state/_global.json``
plus one ``.lhp_state/<pipeline>.json`` per pipeline. Workers write their own
shard atomically via ``os.replace``; the main thread writes ``_global.json``
once at end of batch.

:meth:`StatePersistence.maybe_remove_legacy_state` deletes any leftover
monolithic ``.lhp_state.json`` after a fully-successful batch, so projects
upgrading from older versions converge cleanly.

All atomic writes go through :func:`_atomic_write_json` so the temp-file +
``os.replace`` + ``fsync`` invariant is consistent for every shard.
"""

import json
import logging
import os
import tempfile
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

from ...utils.error_formatter import ErrorCategory, LHPFileError

# Import state models from separate module
from ..state_models import (
    DependencyInfo,
    FileState,
    GlobalDependencies,
    GlobalStatePayload,
    PipelineStatePayload,
)

# FileState keys from older LHP versions that are no longer part of the schema.
# Presence of any of these in a loaded state file indicates the file was
# written by a pre-migration LHP and cannot be read safely.
_LEGACY_FILE_STATE_KEYS = frozenset({"file_composite_checksum", "generation_context"})

# Filename of the project-wide shard. Per-pipeline shards live alongside it as
# ``<pipeline>.json`` within the same ``.lhp_state/`` directory.
_GLOBAL_SHARD_NAME = "_global.json"

# Directory name (under project_root) where the new format lives.
_STATE_DIR_NAME = ".lhp_state"

# Filename of the legacy monolithic state file (under project_root).
_LEGACY_STATE_FILENAME = ".lhp_state.json"

# Sentinel used by :meth:`StatePersistence.load_pipeline_shard` and
# :meth:`StatePersistence.load_global` to signal "incompatible shard format" via
# a hard error rather than silently regenerating.
_SUPPORTED_SHARD_SCHEMA_VERSION = "2"


def _atomic_write_json(path: Path, payload: Dict[str, Any], logger: logging.Logger) -> None:
    """Write ``payload`` to ``path`` atomically via tempfile + ``os.replace``.

    Used by both monolithic (legacy) and shard write paths. Guarantees the
    target either reflects the previous contents or the new contents — never a
    half-written file — even on process kill mid-write. The temp file is on
    the same volume as the target (required for atomic rename on Windows).

    Args:
        path: Final destination path. Parent directory is created if missing.
        payload: JSON-serializable mapping. Caller is responsible for any
            ``last_updated``/timestamp fields; this helper does not mutate it.
        logger: Logger to use for debug/warning messages.

    Raises:
        LHPFileError: If write fails. The destination is left untouched. The
            error title is "Failed to save state file" to preserve the
            user-facing wording from before the shared-helper refactor.
    """
    parent = path.parent
    tmp_path: Optional[Path] = None
    try:
        parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=path.name + ".",
            suffix=".tmp",
            dir=str(parent),
        )
        tmp_path = Path(tmp_name)
        with os.fdopen(fd, "w") as f:
            json.dump(payload, f, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, path)
        tmp_path = None  # ownership transferred; nothing to clean up

        logger.debug(f"Atomically wrote {path}")

    except Exception as e:
        raise LHPFileError(
            category=ErrorCategory.IO,
            code_number="006",
            title="Failed to save state file",
            details=f"Could not write state file {path}: {e}",
            suggestions=[
                "Check file permissions on the project directory",
                "Ensure there is enough disk space available",
                f"Verify the directory exists: {path.parent}",
            ],
            context={"State File": str(path)},
        ) from e
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                logger.warning(
                    f"Could not remove temporary state file {tmp_path}"
                )


def _file_state_from_dict(file_state_data: Dict[str, Any], context_path: Path) -> FileState:
    """Reconstruct a :class:`FileState` from a JSON-decoded dict.

    Centralizes the legacy-key rejection, forward-compatible defaults, and the
    nested ``file_dependencies`` ``DependencyInfo`` reconstruction so both the
    monolithic and shard load paths share one implementation.

    Args:
        file_state_data: Dict decoded from JSON.
        context_path: Path of the file being loaded (used only for error
            messages — points the user at the offending shard or legacy file).

    Returns:
        Hydrated :class:`FileState` instance.

    Raises:
        LHPFileError: If the dict contains legacy keys from a pre-migration
            schema version.
    """
    legacy_keys = _LEGACY_FILE_STATE_KEYS & set(file_state_data.keys())
    if legacy_keys:
        raise LHPFileError(
            category=ErrorCategory.IO,
            code_number="008",
            title="Incompatible state file format",
            details=(
                f"{context_path} was written by an older version of LHP and "
                f"is no longer compatible (legacy keys: {sorted(legacy_keys)})."
                " This can happen after upgrading LHP to a version that "
                "changed the state schema."
            ),
            suggestions=[
                f"Delete the state file: rm {context_path}",
                "Re-run `lhp generate` — a fresh state file will be created "
                "on the next run",
            ],
            context={"State File": str(context_path)},
        )

    # Forward-compatible defaults for fields added across versions
    if "source_yaml_checksum" not in file_state_data:
        file_state_data["source_yaml_checksum"] = ""
    if "file_dependencies" not in file_state_data:
        file_state_data["file_dependencies"] = None
    if "artifact_type" not in file_state_data:
        file_state_data["artifact_type"] = None

    # ``used_substitution_keys`` was a write-only field used by an earlier
    # granular-tracking experiment that never drove selective regen. Drop it
    # silently so legacy state files load cleanly; on the next save the key
    # is gone.
    file_state_data.pop("used_substitution_keys", None)

    # Convert file_dependencies from dict to DependencyInfo objects
    if file_state_data["file_dependencies"]:
        file_deps = {}
        for dep_path, dep_info in file_state_data["file_dependencies"].items():
            normalized_dep_key = dep_path.replace("\\", "/")
            file_deps[normalized_dep_key] = DependencyInfo(**dep_info)
        file_state_data["file_dependencies"] = file_deps

    return FileState(**file_state_data)


def _global_dependencies_from_dict(
    raw: Dict[str, Dict[str, Any]]
) -> Dict[str, GlobalDependencies]:
    """Reconstruct the ``{env: GlobalDependencies}`` map from a JSON-decoded dict."""
    result: Dict[str, GlobalDependencies] = {}
    for env_name, global_deps in raw.items():
        substitution_file = None
        project_config = None
        if global_deps.get("substitution_file"):
            substitution_file = DependencyInfo(**global_deps["substitution_file"])
        if global_deps.get("project_config"):
            project_config = DependencyInfo(**global_deps["project_config"])
        result[env_name] = GlobalDependencies(
            substitution_file=substitution_file,
            project_config=project_config,
        )
    return result


class StatePersistence:
    """
    Service for state file I/O operations and serialization.

    Handles loading and saving .lhp_state.json files with backward compatibility,
    backup management, and proper error handling.
    """

    def __init__(self, project_root: Path, state_file_name: str = ".lhp_state.json"):
        """
        Initialize state persistence service.

        Args:
            project_root: Root directory of the LakehousePlumber project
            state_file_name: Name of the state file (default: .lhp_state.json)
        """
        self.project_root = project_root
        self.state_file = project_root / state_file_name
        self.logger = logging.getLogger(__name__)

    def state_file_exists(self) -> bool:
        """
        Check if the state file exists on the filesystem.

        Returns:
            True if state file exists, False otherwise
        """
        return self.state_file.exists()

    def backup_state_file(self) -> Optional[Path]:
        """
        Create a backup copy of the state file.

        Returns:
            Path to backup file if created, None if no state file exists
        """
        if not self.state_file_exists():
            return None

        try:
            backup_path = self.state_file.with_suffix(
                f".json.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            )

            with open(self.state_file, "r") as src:
                with open(backup_path, "w") as dst:
                    dst.write(src.read())

            self.logger.info(f"Created state backup: {backup_path}")
            return backup_path

        except Exception as e:
            self.logger.warning(f"Failed to create state backup: {e}")
            return None

    def get_state_file_path(self) -> Path:
        """
        Get the path to the state file.

        Returns:
            Path to the state file
        """
        return self.state_file

    def get_state_file_info(self) -> dict:
        """
        Get information about the state file.

        Returns:
            Dictionary with file information
        """
        if not self.state_file_exists():
            return {
                "exists": False,
                "path": str(self.state_file),
                "size": 0,
                "last_modified": None,
            }

        try:
            stat = self.state_file.stat()
            return {
                "exists": True,
                "path": str(self.state_file),
                "size": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat(),
            }
        except Exception as e:
            self.logger.warning(f"Failed to get state file info: {e}")
            return {
                "exists": True,
                "path": str(self.state_file),
                "size": 0,
                "last_modified": None,
            }

    # ------------------------------------------------------------------
    # Per-pipeline shard format (``.lhp_state/<pipeline>.json`` plus
    # ``.lhp_state/_global.json``). The instance methods above support the
    # legacy detect-and-remove path only.
    # ------------------------------------------------------------------

    @staticmethod
    def _shard_path(state_dir: Path, pipeline_name: str) -> Path:
        """Return the on-disk path for ``<pipeline_name>``'s shard.

        The pipeline name is used verbatim as the filename stem. ``_global``
        is reserved for the project-wide shard.

        Raises:
            ValueError: If ``pipeline_name`` collides with the reserved
                ``_global`` filename stem.
        """
        if pipeline_name == "_global":
            raise ValueError(
                "Pipeline name '_global' is reserved for the project-wide "
                "shard. Rename the pipeline to avoid the collision."
            )
        return state_dir / f"{pipeline_name}.json"

    @staticmethod
    def save_pipeline_shard(
        state_dir: Path,
        pipeline_name: str,
        payload: PipelineStatePayload,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Atomically write ``<pipeline_name>``'s shard to ``state_dir``.

        Workers call this once per successful pipeline. The shard is keyed by
        pipeline name and contains every environment's entries for that
        pipeline (multi-env co-located inside one shard).

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory. Created on
                first write.
            pipeline_name: Identifier used as the shard's filename stem.
            payload: :class:`PipelineStatePayload` to serialize.
            logger: Optional logger; defaults to this module's logger.
        """
        log = logger or logging.getLogger(__name__)
        path = StatePersistence._shard_path(state_dir, pipeline_name)
        payload_dict = asdict(payload)
        _atomic_write_json(path, payload_dict, log)

    @staticmethod
    def load_pipeline_shard(
        state_dir: Path,
        pipeline_name: str,
    ) -> Optional[PipelineStatePayload]:
        """Load ``<pipeline_name>``'s shard from ``state_dir``.

        Returns ``None`` when the shard does not exist — callers treat this as
        a "fresh pipeline" signal (no prior generation). Returning ``None`` is
        deliberately distinct from raising on incompatible-schema, which would
        require user intervention.

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory.
            pipeline_name: Identifier used as the shard's filename stem.

        Returns:
            Hydrated :class:`PipelineStatePayload` or ``None`` if the shard
            does not exist.

        Raises:
            LHPFileError: If the shard exists but is malformed (invalid JSON),
                has an incompatible ``schema_version``, or contains legacy
                ``FileState`` keys.
        """
        path = StatePersistence._shard_path(state_dir, pipeline_name)
        if not path.exists():
            return None

        try:
            with open(path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Malformed pipeline state shard",
                details=(
                    f"{path} exists but is not valid JSON "
                    f"(at line {e.lineno}, column {e.colno}): {e.msg}"
                ),
                suggestions=[
                    f"Inspect {path} for manual edits or truncation",
                    f"If the file cannot be repaired, delete it: rm {path}",
                    "Re-run `lhp generate` to regenerate the shard",
                ],
                context={"State File": str(path)},
            ) from e
        except OSError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Could not read pipeline state shard",
                details=f"I/O error reading {path}: {e}",
                suggestions=[
                    "Check file permissions on the project directory",
                    f"Verify the file is accessible: {path}",
                ],
                context={"State File": str(path)},
            ) from e

        schema_version = data.get("schema_version")
        if schema_version != _SUPPORTED_SHARD_SCHEMA_VERSION:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Incompatible state shard format",
                details=(
                    f"{path} has schema_version={schema_version!r}, but this "
                    f"version of LHP only supports "
                    f"{_SUPPORTED_SHARD_SCHEMA_VERSION!r}."
                ),
                suggestions=[
                    f"Delete the state directory: rm -rf {state_dir}",
                    "Re-run `lhp generate` to regenerate state shards",
                ],
                context={"State File": str(path)},
            )

        environments: Dict[str, Dict[str, FileState]] = {}
        for env_name, env_files in data.get("environments", {}).items():
            environments[env_name] = {}
            for file_path, file_state_data in env_files.items():
                normalized_key = file_path.replace("\\", "/")
                environments[env_name][normalized_key] = _file_state_from_dict(
                    file_state_data, path
                )

        return PipelineStatePayload(
            pipeline=data.get("pipeline", pipeline_name),
            schema_version=schema_version,
            environments=environments,
        )

    @staticmethod
    def save_global(
        state_dir: Path,
        payload: GlobalStatePayload,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Atomically write ``_global.json`` to ``state_dir``.

        Called once per batch by the main thread at end of ``lhp generate``.
        Updates ``payload.last_updated`` to the current timestamp before
        serialization (in-place mutation; the payload is non-frozen by design).

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory.
            payload: :class:`GlobalStatePayload` to serialize.
            logger: Optional logger; defaults to this module's logger.
        """
        log = logger or logging.getLogger(__name__)
        payload.last_updated = datetime.now().isoformat()
        path = state_dir / _GLOBAL_SHARD_NAME
        payload_dict = asdict(payload)
        _atomic_write_json(path, payload_dict, log)

    @staticmethod
    def load_global(state_dir: Path) -> Optional[GlobalStatePayload]:
        """Load ``_global.json`` from ``state_dir``.

        Returns ``None`` when the file does not exist — the project has not yet
        been generated on the new format.

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory.

        Returns:
            Hydrated :class:`GlobalStatePayload` or ``None`` if absent.

        Raises:
            LHPFileError: If the file exists but is malformed or has an
                incompatible ``schema_version``.
        """
        path = state_dir / _GLOBAL_SHARD_NAME
        if not path.exists():
            return None

        try:
            with open(path, "r") as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Malformed global state shard",
                details=(
                    f"{path} exists but is not valid JSON "
                    f"(at line {e.lineno}, column {e.colno}): {e.msg}"
                ),
                suggestions=[
                    f"Inspect {path} for manual edits or truncation",
                    f"If the file cannot be repaired, delete it: rm {path}",
                    "Re-run `lhp generate` to regenerate state",
                ],
                context={"State File": str(path)},
            ) from e
        except OSError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Could not read global state shard",
                details=f"I/O error reading {path}: {e}",
                suggestions=[
                    "Check file permissions on the project directory",
                    f"Verify the file is accessible: {path}",
                ],
                context={"State File": str(path)},
            ) from e

        schema_version = data.get("schema_version")
        if schema_version != _SUPPORTED_SHARD_SCHEMA_VERSION:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Incompatible state shard format",
                details=(
                    f"{path} has schema_version={schema_version!r}, but this "
                    f"version of LHP only supports "
                    f"{_SUPPORTED_SHARD_SCHEMA_VERSION!r}."
                ),
                suggestions=[
                    f"Delete the state directory: rm -rf {state_dir}",
                    "Re-run `lhp generate` to regenerate state shards",
                ],
                context={"State File": str(path)},
            )

        global_deps = _global_dependencies_from_dict(
            data.get("global_dependencies", {})
        )

        return GlobalStatePayload(
            schema_version=schema_version,
            version=data.get("version", "1.0"),
            last_updated=data.get("last_updated", ""),
            global_dependencies=global_deps,
            last_generation_context=data.get("last_generation_context", {}),
        )

    @staticmethod
    def load_all_pipeline_shards(
        state_dir: Path,
        environment: str,
    ) -> Dict[str, FileState]:
        """Merge ``environment``'s entries from every per-pipeline shard.

        Aggregate consumers (``lhp state``, ``lhp stats``, staleness analysis,
        orphan-file scan) call this in place of the old
        ``state.environments[env]`` lookup. Returns the same dict shape so
        caller iteration logic is unchanged.

        Shard filenames whose stem starts with ``_`` are reserved (e.g.
        ``_global.json``) and are skipped. Per-pipeline shards living in
        ``state_dir`` that do not contain entries for ``environment`` are
        loaded but contribute no entries to the merged result.

        Args:
            state_dir: ``<project_root>/.lhp_state`` directory. If absent,
                returns an empty dict.
            environment: Environment name to slice from each shard.

        Returns:
            Dict[relative_generated_path -> FileState]. Empty when no shards
            exist for ``state_dir`` or none contain entries for
            ``environment``.

        Raises:
            LHPFileError: If any shard is malformed (propagated from
                :meth:`load_pipeline_shard`).
        """
        if not state_dir.exists() or not state_dir.is_dir():
            return {}

        merged: Dict[str, FileState] = {}
        for shard_path in sorted(state_dir.glob("*.json")):
            stem = shard_path.stem
            if stem.startswith("_"):
                # Reserved (e.g. ``_global.json``). Skip.
                continue
            payload = StatePersistence.load_pipeline_shard(state_dir, stem)
            if payload is None:
                # Should not happen — glob returned the file, but a race could.
                continue
            env_files = payload.environments.get(environment, {})
            merged.update(env_files)
        return merged

    @staticmethod
    def maybe_remove_legacy_state(
        project_root: Path,
        batch_succeeded: bool,
        logger: Optional[logging.Logger] = None,
    ) -> bool:
        """Remove ``.lhp_state.json`` (legacy monolith) iff the batch succeeded.

        Auto-deletion is gated on ``batch_succeeded=True`` so partial-failure
        runs leave the legacy file in place as a safety net: a re-run can fall
        back to it if the new format is corrupt.

        Args:
            project_root: Project root directory. The legacy file is expected
                at ``project_root / ".lhp_state.json"``.
            batch_succeeded: True iff every pipeline in the batch succeeded.
                When False this method is a no-op.
            logger: Optional logger; defaults to this module's logger.

        Returns:
            True if the legacy file was actually removed; False otherwise
            (file absent, or removal skipped due to partial failure).
        """
        log = logger or logging.getLogger(__name__)
        legacy_path = project_root / _LEGACY_STATE_FILENAME
        if not legacy_path.exists():
            return False
        if not batch_succeeded:
            log.warning(
                f"Legacy {legacy_path} retained due to partial-batch failure; "
                "delete manually after a clean run."
            )
            return False
        try:
            legacy_path.unlink()
            log.info(f"Removed legacy state file {legacy_path} after clean batch.")
            return True
        except OSError as e:
            log.warning(f"Could not remove legacy state file {legacy_path}: {e}")
            return False

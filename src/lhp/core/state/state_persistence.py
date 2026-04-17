"""State persistence service for LakehousePlumber."""

import json
import logging
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Optional

from ...utils.error_formatter import ErrorCategory, LHPFileError

# Import state models from separate module
from ..state_models import DependencyInfo, FileState, GlobalDependencies, ProjectState

# FileState keys from older LHP versions that are no longer part of the schema.
# Presence of any of these in a loaded state file indicates the file was
# written by a pre-migration LHP and cannot be read safely.
_LEGACY_FILE_STATE_KEYS = frozenset({"file_composite_checksum", "generation_context"})


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

    def load_state(self):
        """
        Load state from file.

        Returns:
            ProjectState object loaded from file, or a fresh empty state if
            the state file does not exist.

        Raises:
            LHPFileError: If the state file exists but cannot be loaded
                (incompatible schema, malformed JSON, or I/O failure). We
                intentionally do NOT silently discard a user's state — a
                corrupted or obsolete state file should surface to the user
                with actionable guidance rather than be clobbered on save.
        """
        if not self.state_file.exists():
            return ProjectState()

        try:
            with open(self.state_file, "r") as f:
                state_data = json.load(f)
        except json.JSONDecodeError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Malformed state file",
                details=(
                    f"{self.state_file} exists but is not valid JSON "
                    f"(at line {e.lineno}, column {e.colno}): {e.msg}"
                ),
                suggestions=[
                    f"Inspect {self.state_file} for manual edits or truncation",
                    f"If the file cannot be repaired, delete it: rm {self.state_file}",
                    "Re-run `lhp generate` — a fresh state file will be created",
                ],
                context={"State File": str(self.state_file)},
            ) from e
        except OSError as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Could not read state file",
                details=f"I/O error reading {self.state_file}: {e}",
                suggestions=[
                    "Check file permissions on the project directory",
                    f"Verify the file is accessible: {self.state_file}",
                ],
                context={"State File": str(self.state_file)},
            ) from e

        try:
            # Convert dict back to dataclasses
            environments = {}
            for env_name, env_files in state_data.get("environments", {}).items():
                environments[env_name] = {}
                for file_path, file_state in env_files.items():
                    # Reject state files written by a pre-migration LHP. These
                    # carry per-file keys we've since removed; deserializing
                    # would either crash in FileState(**file_state) or silently
                    # drop the fields. Prefer a clear, actionable error.
                    legacy_keys = _LEGACY_FILE_STATE_KEYS & set(file_state.keys())
                    if legacy_keys:
                        raise LHPFileError(
                            category=ErrorCategory.IO,
                            code_number="008",
                            title="Incompatible state file format",
                            details=(
                                f"{self.state_file} was written by an older "
                                "version of LHP and is no longer compatible "
                                f"(legacy keys: {sorted(legacy_keys)}). This "
                                "can happen after upgrading LHP to a version "
                                "that changed the state schema."
                            ),
                            suggestions=[
                                f"Delete the state file: rm {self.state_file}",
                                "Re-run `lhp generate` — a fresh state file "
                                "will be created on the next run",
                            ],
                            context={"State File": str(self.state_file)},
                        )

                    # Forward-compatible defaults for fields added across versions
                    if "source_yaml_checksum" not in file_state:
                        file_state["source_yaml_checksum"] = ""
                    if "file_dependencies" not in file_state:
                        file_state["file_dependencies"] = None
                    if "artifact_type" not in file_state:
                        file_state["artifact_type"] = None

                    # Convert file_dependencies from dict to DependencyInfo objects
                    if file_state["file_dependencies"]:
                        file_deps = {}
                        for dep_path, dep_info in file_state[
                            "file_dependencies"
                        ].items():
                            normalized_dep_key = dep_path.replace("\\", "/")
                            file_deps[normalized_dep_key] = DependencyInfo(
                                **dep_info
                            )
                        file_state["file_dependencies"] = file_deps

                    normalized_key = file_path.replace("\\", "/")
                    environments[env_name][normalized_key] = FileState(**file_state)

            # Handle global dependencies
            global_dependencies = {}
            if "global_dependencies" in state_data:
                for env_name, global_deps in state_data[
                    "global_dependencies"
                ].items():
                    substitution_file = None
                    project_config = None

                    if (
                        "substitution_file" in global_deps
                        and global_deps["substitution_file"]
                    ):
                        substitution_file = DependencyInfo(
                            **global_deps["substitution_file"]
                        )
                    if (
                        "project_config" in global_deps
                        and global_deps["project_config"]
                    ):
                        project_config = DependencyInfo(
                            **global_deps["project_config"]
                        )

                    global_dependencies[env_name] = GlobalDependencies(
                        substitution_file=substitution_file,
                        project_config=project_config,
                    )

            loaded_state = ProjectState(
                version=state_data.get("version", "1.0"),
                last_updated=state_data.get("last_updated", ""),
                environments=environments,
                global_dependencies=global_dependencies,
                last_generation_context=state_data.get(
                    "last_generation_context", {}
                ),
            )
        except LHPFileError:
            raise
        except (TypeError, KeyError, AttributeError) as e:
            # Schema violation (missing required fields, wrong types, etc.).
            # Prefer an actionable error over silently discarding the user's
            # state.
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="008",
                title="Malformed state file",
                details=(
                    f"{self.state_file} is valid JSON but does not match the "
                    f"expected LHP state schema: {e}"
                ),
                suggestions=[
                    f"Inspect {self.state_file} for manual edits",
                    f"If the file cannot be repaired, delete it: rm {self.state_file}",
                    "Re-run `lhp generate` — a fresh state file will be created",
                ],
                context={"State File": str(self.state_file)},
            ) from e

        self.logger.info(f"Loaded state from {self.state_file}")
        return loaded_state

    def save_state(self, state) -> None:
        """
        Save current state to file.

        Args:
            state: ProjectState to save

        Raises:
            Exception: If save operation fails
        """
        try:
            # Convert to dict for JSON serialization
            state_dict = asdict(state)
            state_dict["last_updated"] = datetime.now().isoformat()

            with open(self.state_file, "w") as f:
                json.dump(state_dict, f, indent=2, sort_keys=True)

            self.logger.debug(f"Saved state to {self.state_file}")

        except Exception as e:
            raise LHPFileError(
                category=ErrorCategory.IO,
                code_number="006",
                title="Failed to save state file",
                details=f"Could not write state file {self.state_file}: {e}",
                suggestions=[
                    "Check file permissions on the project directory",
                    "Ensure there is enough disk space available",
                    f"Verify the directory exists: {self.state_file.parent}",
                ],
                context={"State File": str(self.state_file)},
            ) from e

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

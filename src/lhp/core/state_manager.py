"""State management for LakehousePlumber generated files."""

import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, Set, List, Optional, Any
from datetime import datetime
from dataclasses import dataclass, asdict
from collections import defaultdict


@dataclass
class FileState:
    """Represents the state of a generated file."""

    source_yaml: str  # Path to the YAML file that generated this
    generated_path: str  # Path to the generated file
    checksum: str  # SHA256 checksum of the generated file
    source_yaml_checksum: str  # SHA256 checksum of the source YAML file
    timestamp: str  # When it was generated
    environment: str  # Environment it was generated for
    pipeline: str  # Pipeline name
    flowgroup: str  # FlowGroup name


@dataclass
class ProjectState:
    """Represents the complete state of a project."""

    version: str = "1.0"
    last_updated: str = ""
    environments: Dict[str, Dict[str, FileState]] = (
        None  # env -> file_path -> FileState
    )

    def __post_init__(self):
        if self.environments is None:
            self.environments = {}


class StateManager:
    """Manages state of generated files for cleanup operations."""

    def __init__(self, project_root: Path, state_file_name: str = ".lhp_state.json"):
        """Initialize state manager.

        Args:
            project_root: Root directory of the LakehousePlumber project
            state_file_name: Name of the state file (default: .lhp_state.json)
        """
        self.project_root = project_root
        self.state_file = project_root / state_file_name
        self.logger = logging.getLogger(__name__)
        self._state: Optional[ProjectState] = None

        # Load existing state
        self._load_state()

    def _get_include_patterns(self) -> List[str]:
        """Get include patterns from project configuration.
        
        Returns:
            List of include patterns, or empty list if none specified
        """
        try:
            from .project_config_loader import ProjectConfigLoader
            config_loader = ProjectConfigLoader(self.project_root)
            project_config = config_loader.load_project_config()
            
            if project_config and project_config.include:
                return project_config.include
            else:
                # No include patterns specified, return empty list (no filtering)
                return []
        except Exception as e:
            self.logger.warning(f"Could not load project config for include patterns: {e}")
            return []

    def _load_state(self):
        """Load state from file."""
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    state_data = json.load(f)

                # Convert dict back to dataclass
                environments = {}
                for env_name, env_files in state_data.get("environments", {}).items():
                    environments[env_name] = {}
                    for file_path, file_state in env_files.items():
                        # Handle backward compatibility - add source_yaml_checksum if missing
                        if "source_yaml_checksum" not in file_state:
                            file_state["source_yaml_checksum"] = ""
                        environments[env_name][file_path] = FileState(**file_state)

                self._state = ProjectState(
                    version=state_data.get("version", "1.0"),
                    last_updated=state_data.get("last_updated", ""),
                    environments=environments,
                )

                self.logger.info(f"Loaded state from {self.state_file}")

            except Exception as e:
                self.logger.warning(f"Failed to load state file {self.state_file}: {e}")
                self._state = ProjectState()
        else:
            self._state = ProjectState()

    def _save_state(self):
        """Save current state to file."""
        try:
            # Convert to dict for JSON serialization
            state_dict = asdict(self._state)
            state_dict["last_updated"] = datetime.now().isoformat()

            with open(self.state_file, "w") as f:
                json.dump(state_dict, f, indent=2, sort_keys=True)

            self.logger.debug(f"Saved state to {self.state_file}")

        except Exception as e:
            self.logger.error(f"Failed to save state file {self.state_file}: {e}")
            raise

    def _calculate_checksum(self, file_path: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return ""

    def track_generated_file(
        self,
        generated_path: Path,
        source_yaml: Path,
        environment: str,
        pipeline: str,
        flowgroup: str,
    ):
        """Track a generated file in the state.

        Args:
            generated_path: Path to the generated file
            source_yaml: Path to the source YAML file
            environment: Environment name
            pipeline: Pipeline name
            flowgroup: FlowGroup name
        """
        # Calculate relative paths from project root
        try:
            rel_generated = generated_path.relative_to(self.project_root)
            rel_source = source_yaml.relative_to(self.project_root)
        except ValueError:
            # Handle absolute paths
            rel_generated = str(generated_path)
            rel_source = str(source_yaml)

        # Calculate checksums for both generated and source files
        generated_checksum = self._calculate_checksum(generated_path)
        source_checksum = self._calculate_checksum(source_yaml)

        # Create file state
        file_state = FileState(
            source_yaml=str(rel_source),
            generated_path=str(rel_generated),
            checksum=generated_checksum,
            source_yaml_checksum=source_checksum,
            timestamp=datetime.now().isoformat(),
            environment=environment,
            pipeline=pipeline,
            flowgroup=flowgroup,
        )

        # Ensure environment exists in state
        if environment not in self._state.environments:
            self._state.environments[environment] = {}

        # Track the file
        self._state.environments[environment][str(rel_generated)] = file_state

        self.logger.debug(f"Tracked generated file: {rel_generated} from {rel_source}")

    def get_generated_files(self, environment: str) -> Dict[str, FileState]:
        """Get all generated files for an environment.

        Args:
            environment: Environment name

        Returns:
            Dictionary mapping file paths to FileState objects
        """
        return self._state.environments.get(environment, {})

    def get_files_by_source(
        self, source_yaml: Path, environment: str
    ) -> List[FileState]:
        """Get all files generated from a specific source YAML.

        Args:
            source_yaml: Path to the source YAML file
            environment: Environment name

        Returns:
            List of FileState objects for files generated from this source
        """
        try:
            rel_source = str(source_yaml.relative_to(self.project_root))
        except ValueError:
            rel_source = str(source_yaml)

        env_files = self._state.environments.get(environment, {})
        return [
            file_state
            for file_state in env_files.values()
            if file_state.source_yaml == rel_source
        ]

    def find_orphaned_files(self, environment: str) -> List[FileState]:
        """Find generated files whose source YAML files no longer exist or don't match include patterns.

        A file is considered orphaned if:
        1. The source YAML file doesn't exist anymore, OR
        2. The source YAML file exists but doesn't match the current include patterns

        Args:
            environment: Environment name

        Returns:
            List of FileState objects for orphaned files
        """
        orphaned = []
        env_files = self._state.environments.get(environment, {})

        # Get current YAML files that match include patterns
        current_yaml_files = self.get_current_yaml_files()
        current_yaml_paths = {
            str(f.relative_to(self.project_root)) for f in current_yaml_files
        }

        for file_state in env_files.values():
            source_path = self.project_root / file_state.source_yaml
            
            # Check if source file doesn't exist
            if not source_path.exists():
                orphaned.append(file_state)
                self.logger.debug(f"File orphaned - source doesn't exist: {file_state.source_yaml}")
            # Check if source file exists but doesn't match current include patterns
            elif file_state.source_yaml not in current_yaml_paths:
                orphaned.append(file_state)
                self.logger.debug(f"File orphaned - doesn't match include patterns: {file_state.source_yaml}")

        return orphaned

    def find_stale_files(self, environment: str) -> List[FileState]:
        """Find generated files whose source YAML files have changed.

        Args:
            environment: Environment name

        Returns:
            List of FileState objects for stale files (YAML changed)
        """
        stale = []
        env_files = self._state.environments.get(environment, {})

        for file_state in env_files.values():
            source_path = self.project_root / file_state.source_yaml
            if source_path.exists():
                current_checksum = self._calculate_checksum(source_path)
                # If checksum is empty (backward compatibility) or different, it's stale
                if (
                    not file_state.source_yaml_checksum
                    or current_checksum != file_state.source_yaml_checksum
                ):
                    stale.append(file_state)

        return stale

    def find_new_yaml_files(self, environment: str, pipeline: str = None) -> List[Path]:
        """Find YAML files that exist but are not tracked in state.

        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by

        Returns:
            List of Path objects for new YAML files
        """
        current_yamls = self.get_current_yaml_files(pipeline)
        tracked_sources = set()

        env_files = self._state.environments.get(environment, {})
        for file_state in env_files.values():
            if not pipeline or file_state.pipeline == pipeline:
                tracked_sources.add(self.project_root / file_state.source_yaml)

        return [
            yaml_file for yaml_file in current_yamls if yaml_file not in tracked_sources
        ]

    def get_files_needing_generation(
        self, environment: str, pipeline: str = None
    ) -> Dict[str, List]:
        """Get all files that need generation (new, stale, or untracked).

        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by

        Returns:
            Dictionary with 'new', 'stale', and 'up_to_date' lists
        """
        # Find stale files (YAML changed)
        stale_files = self.find_stale_files(environment)
        if pipeline:
            stale_files = [f for f in stale_files if f.pipeline == pipeline]

        # Find new YAML files (not tracked)
        new_files = self.find_new_yaml_files(environment, pipeline)

        # Find up-to-date files
        all_tracked = self.get_generated_files(environment)
        if pipeline:
            all_tracked = {
                path: state
                for path, state in all_tracked.items()
                if state.pipeline == pipeline
            }

        up_to_date = []
        for file_state in all_tracked.values():
            source_path = self.project_root / file_state.source_yaml
            if (
                source_path.exists()
                and file_state.source_yaml_checksum
                and self._calculate_checksum(source_path)
                == file_state.source_yaml_checksum
            ):
                up_to_date.append(file_state)

        return {"new": new_files, "stale": stale_files, "up_to_date": up_to_date}

    def cleanup_orphaned_files(
        self, environment: str, dry_run: bool = False
    ) -> List[str]:
        """Remove generated files whose source YAML files no longer exist.

        Args:
            environment: Environment name
            dry_run: If True, only return what would be deleted without actually deleting

        Returns:
            List of file paths that were (or would be) deleted
        """
        orphaned_files = self.find_orphaned_files(environment)
        deleted_files = []

        for file_state in orphaned_files:
            generated_path = self.project_root / file_state.generated_path

            if dry_run:
                deleted_files.append(str(file_state.generated_path))
                self.logger.info(f"Would delete: {file_state.generated_path}")
            else:
                try:
                    if generated_path.exists():
                        generated_path.unlink()
                        deleted_files.append(str(file_state.generated_path))
                        self.logger.info(
                            f"Deleted orphaned file: {file_state.generated_path}"
                        )

                    # Remove from state
                    del self._state.environments[environment][file_state.generated_path]

                except Exception as e:
                    self.logger.error(
                        f"Failed to delete {file_state.generated_path}: {e}"
                    )

        # Clean up empty directories
        if not dry_run and deleted_files:
            self._cleanup_empty_directories(environment)
            self._save_state()

        return deleted_files

    def _cleanup_empty_directories(self, environment: str):
        """Remove empty directories in the generated output path."""
        output_dirs = set()

        # Collect all output directories for this environment
        for file_state in self._state.environments.get(environment, {}).values():
            output_path = self.project_root / file_state.generated_path
            output_dirs.add(output_path.parent)

        # Also check recently deleted files' directories
        # (This is approximate - we check common patterns)
        base_output_dir = self.project_root / "generated"
        if base_output_dir.exists():
            for pipeline_dir in base_output_dir.iterdir():
                if pipeline_dir.is_dir():
                    output_dirs.add(pipeline_dir)

        # Remove empty directories (from deepest to shallowest)
        for dir_path in sorted(output_dirs, key=lambda x: len(x.parts), reverse=True):
            try:
                if (
                    dir_path.exists()
                    and dir_path.is_dir()
                    and not any(dir_path.iterdir())
                ):
                    dir_path.rmdir()
                    self.logger.info(f"Removed empty directory: {dir_path}")
            except Exception as e:
                self.logger.debug(f"Could not remove directory {dir_path}: {e}")

    def get_current_yaml_files(self, pipeline: str = None) -> Set[Path]:
        """Get all current YAML files in the pipelines directory.

        Args:
            pipeline: Optional pipeline name to filter by

        Returns:
            Set of Path objects for all YAML files
        """
        yaml_files = set()
        pipelines_dir = self.project_root / "pipelines"

        if not pipelines_dir.exists():
            return yaml_files

        if pipeline:
            # Get YAML files for specific pipeline
            pipeline_dir = pipelines_dir / pipeline
            if pipeline_dir.exists():
                yaml_files.update(pipeline_dir.rglob("*.yaml"))
                yaml_files.update(pipeline_dir.rglob("*.yml"))
        else:
            # Get all YAML files
            yaml_files.update(pipelines_dir.rglob("*.yaml"))
            yaml_files.update(pipelines_dir.rglob("*.yml"))

        # Apply include filtering if patterns are specified
        include_patterns = self._get_include_patterns()
        if include_patterns:
            # Filter files based on include patterns
            from ..utils.file_pattern_matcher import discover_files_with_patterns
            
            # Convert absolute paths to relative paths for pattern matching
            yaml_files_list = list(yaml_files)
            filtered_files = discover_files_with_patterns(pipelines_dir, include_patterns)
            
            # Convert back to set of absolute paths
            yaml_files = set(filtered_files)

        return yaml_files

    def compare_with_current_state(
        self, environment: str, pipeline: str = None
    ) -> Dict[str, Any]:
        """Compare current YAML files with tracked state to find changes.

        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by

        Returns:
            Dictionary with 'added', 'removed', and 'modified' file lists
        """
        current_yamls = self.get_current_yaml_files(pipeline)
        current_yaml_paths = {
            str(f.relative_to(self.project_root)) for f in current_yamls
        }

        # Get tracked source files for this environment
        tracked_sources = set()
        for file_state in self._state.environments.get(environment, {}).values():
            tracked_sources.add(file_state.source_yaml)

        # Filter by pipeline if specified
        if pipeline:
            tracked_sources = {
                file_state.source_yaml
                for file_state in self._state.environments.get(environment, {}).values()
                if file_state.pipeline == pipeline
            }

        return {
            "added": list(current_yaml_paths - tracked_sources),
            "removed": list(tracked_sources - current_yaml_paths),
            "existing": list(current_yaml_paths & tracked_sources),
        }

    def save(self):
        """Save the current state to file."""
        self._save_state()

    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about the current state.

        Returns:
            Dictionary with statistics about tracked files
        """
        stats = {
            "total_environments": len(self._state.environments),
            "environments": {},
        }

        for env_name, env_files in self._state.environments.items():
            pipelines = defaultdict(int)
            flowgroups = defaultdict(int)

            for file_state in env_files.values():
                pipelines[file_state.pipeline] += 1
                flowgroups[file_state.flowgroup] += 1

            stats["environments"][env_name] = {
                "total_files": len(env_files),
                "pipelines": dict(pipelines),
                "flowgroups": dict(flowgroups),
            }

        return stats

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
class DependencyInfo:
    """Information about a dependency file."""
    
    path: str              # Relative path to dependency file
    checksum: str          # SHA256 checksum of dependency
    type: str             # 'preset', 'template', 'substitution', 'project_config'
    last_modified: str    # ISO timestamp of last modification


@dataclass
class GlobalDependencies:
    """Dependencies that affect all files in scope."""
    
    substitution_file: Optional[DependencyInfo] = None  # Per environment
    project_config: Optional[DependencyInfo] = None     # Global across environments


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
    
    # New dependency tracking fields
    file_dependencies: Optional[Dict[str, DependencyInfo]] = None
    file_composite_checksum: str = ""


@dataclass
class ProjectState:
    """Represents the complete state of a project."""

    version: str = "1.0"
    last_updated: str = ""
    environments: Dict[str, Dict[str, FileState]] = (
        None  # env -> file_path -> FileState
    )
    
    # Global dependencies per environment
    global_dependencies: Optional[Dict[str, GlobalDependencies]] = None  # env -> GlobalDependencies

    def __post_init__(self):
        if self.environments is None:
            self.environments = {}
        if self.global_dependencies is None:
            self.global_dependencies = {}


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
        
        # Initialize dependency resolver
        from .state_dependency_resolver import StateDependencyResolver
        self.dependency_resolver = StateDependencyResolver(project_root)

        # Load existing state
        self._load_state()

    def state_file_exists(self) -> bool:
        """Check if the state file exists on the filesystem.
        
        Returns:
            True if state file exists, False otherwise
        """
        return self.state_file.exists()

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
                        # Handle backward compatibility - add missing fields
                        if "source_yaml_checksum" not in file_state:
                            file_state["source_yaml_checksum"] = ""
                        if "file_dependencies" not in file_state:
                            file_state["file_dependencies"] = None
                        if "file_composite_checksum" not in file_state:
                            file_state["file_composite_checksum"] = ""
                        
                        # Convert file_dependencies from dict to DependencyInfo objects
                        if file_state["file_dependencies"]:
                            file_deps = {}
                            for dep_path, dep_info in file_state["file_dependencies"].items():
                                file_deps[dep_path] = DependencyInfo(**dep_info)
                            file_state["file_dependencies"] = file_deps
                        
                        environments[env_name][file_path] = FileState(**file_state)

                # Handle global dependencies
                global_dependencies = {}
                if "global_dependencies" in state_data:
                    for env_name, global_deps in state_data["global_dependencies"].items():
                        substitution_file = None
                        project_config = None
                        
                        if "substitution_file" in global_deps and global_deps["substitution_file"]:
                            substitution_file = DependencyInfo(**global_deps["substitution_file"])
                        if "project_config" in global_deps and global_deps["project_config"]:
                            project_config = DependencyInfo(**global_deps["project_config"])
                        
                        global_dependencies[env_name] = GlobalDependencies(
                            substitution_file=substitution_file,
                            project_config=project_config
                        )

                self._state = ProjectState(
                    version=state_data.get("version", "1.0"),
                    last_updated=state_data.get("last_updated", ""),
                    environments=environments,
                    global_dependencies=global_dependencies
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
        """Track a generated file in the state with dependency resolution.

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
        # Resolve paths relative to project_root if they're not absolute
        resolved_generated_path = self.project_root / generated_path if not generated_path.is_absolute() else generated_path
        resolved_source_yaml = self.project_root / source_yaml if not source_yaml.is_absolute() else source_yaml
        
        generated_checksum = self._calculate_checksum(resolved_generated_path)
        source_checksum = self._calculate_checksum(resolved_source_yaml)

        # Resolve file-specific dependencies
        # Ensure rel_source is a Path object
        rel_source_path = Path(rel_source) if isinstance(rel_source, str) else rel_source
        file_dependencies = self.dependency_resolver.resolve_file_dependencies(
            rel_source_path, environment
        )
        
        # Calculate composite checksum for all dependencies
        dep_paths = [str(rel_source)] + list(file_dependencies.keys())
        composite_checksum = self.dependency_resolver.calculate_composite_checksum(dep_paths)

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
            file_dependencies=file_dependencies,
            file_composite_checksum=composite_checksum,
        )

        # Ensure environment exists in state
        if environment not in self._state.environments:
            self._state.environments[environment] = {}

        # Track the file
        self._state.environments[environment][str(rel_generated)] = file_state

        # Update global dependencies for this environment
        self._update_global_dependencies(environment)

        self.logger.debug(f"Tracked generated file: {rel_generated} from {rel_source} with {len(file_dependencies)} dependencies")

    def _update_global_dependencies(self, environment: str):
        """Update global dependencies for an environment.
        
        Args:
            environment: Environment name
        """
        try:
            # Resolve global dependencies
            global_deps = self.dependency_resolver.resolve_global_dependencies(environment)
            
            # Convert to GlobalDependencies object
            substitution_file = None
            project_config = None
            
            for dep_path, dep_info in global_deps.items():
                if dep_info.type == "substitution":
                    substitution_file = dep_info
                elif dep_info.type == "project_config":
                    project_config = dep_info
            
            # Ensure global_dependencies exists in state
            if self._state.global_dependencies is None:
                self._state.global_dependencies = {}
            
            # Update global dependencies for this environment
            self._state.global_dependencies[environment] = GlobalDependencies(
                substitution_file=substitution_file,
                project_config=project_config
            )
            
            self.logger.debug(f"Updated global dependencies for environment: {environment}")
            
        except Exception as e:
            self.logger.warning(f"Failed to update global dependencies for {environment}: {e}")

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
        2. The source YAML file exists but doesn't match the current include patterns, OR
        3. The source YAML file exists but pipeline/flowgroup fields have changed (different expected path)

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
            else:
                # Check if pipeline/flowgroup fields have changed (different expected path)
                try:
                    from ..parsers.yaml_parser import YAMLParser
                    parser = YAMLParser()
                    current_flowgroup = parser.parse_flowgroup(source_path)
                    
                    # Check if the expected generated file path has changed
                    expected_pipeline = current_flowgroup.pipeline
                    expected_flowgroup = current_flowgroup.flowgroup
                    
                    # Compare with stored values
                    if (file_state.pipeline != expected_pipeline or 
                        file_state.flowgroup != expected_flowgroup):
                        orphaned.append(file_state)
                        self.logger.debug(f"File orphaned - pipeline/flowgroup changed: {file_state.source_yaml} "
                                        f"(was: {file_state.pipeline}/{file_state.flowgroup}, "
                                        f"now: {expected_pipeline}/{expected_flowgroup})")
                except Exception as e:
                    # If we can't parse the YAML, don't consider it orphaned
                    # The file exists and matches include patterns, so it's still valid
                    self.logger.debug(f"Could not parse YAML {file_state.source_yaml} for orphan check: {e}")
                    # Don't add to orphaned list - let normal processing handle it

        return orphaned

    def find_stale_files(self, environment: str) -> List[FileState]:
        """Find generated files that need regeneration due to dependency changes.

        This enhanced method checks for staleness due to:
        1. Source YAML file changes
        2. Global dependency changes (substitution files, project config)
        3. File-specific dependency changes (presets, templates)

        Args:
            environment: Environment name

        Returns:
            List of FileState objects for stale files
        """
        stale = []
        env_files = self._state.environments.get(environment, {})

        if not env_files:
            return stale

        # Check for global dependency changes
        global_deps_changed = self._check_global_dependencies_changed(environment)
        
        if global_deps_changed:
            # If global dependencies changed, ALL files in the environment are stale
            self.logger.debug(f"Global dependencies changed for {environment} - marking all files as stale")
            return list(env_files.values())

        # Check individual files for staleness
        for file_state in env_files.values():
            source_path = self.project_root / file_state.source_yaml
            
            if not source_path.exists():
                # Source file doesn't exist - this will be handled by find_orphaned_files
                continue
                
            # Check if source YAML has changed
            current_source_checksum = self._calculate_checksum(source_path)
            source_changed = (
                not file_state.source_yaml_checksum
                or current_source_checksum != file_state.source_yaml_checksum
            )
            
            # Check if file-specific dependencies have changed
            file_deps_changed = self._check_file_dependencies_changed(file_state, environment)
            
            if source_changed or file_deps_changed:
                stale.append(file_state)
                reason = []
                if source_changed:
                    reason.append("source YAML changed")
                if file_deps_changed:
                    reason.append("file dependencies changed")
                self.logger.debug(f"File {file_state.generated_path} is stale: {', '.join(reason)}")

        return stale

    def _check_global_dependencies_changed(self, environment: str) -> bool:
        """Check if global dependencies have changed for an environment.
        
        Args:
            environment: Environment name
            
        Returns:
            True if global dependencies have changed, False otherwise
        """
        try:
            # Get current global dependencies
            current_global_deps = self.dependency_resolver.resolve_global_dependencies(environment)
            
            # Get stored global dependencies
            stored_global_deps = self._state.global_dependencies.get(environment) if self._state.global_dependencies else None
            
            # If no stored global dependencies, consider as changed (first time)
            if not stored_global_deps:
                return bool(current_global_deps)  # Changed if there are any global dependencies
            
            # Compare substitution file
            if self._dependency_changed(
                stored_global_deps.substitution_file, 
                current_global_deps.get(f"substitutions/{environment}.yaml")
            ):
                self.logger.debug(f"Substitution file changed for {environment}")
                return True
            
            # Compare project config
            if self._dependency_changed(
                stored_global_deps.project_config,
                current_global_deps.get("lhp.yaml")
            ):
                self.logger.debug(f"Project config changed for {environment}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Failed to check global dependencies for {environment}: {e}")
            return True  # Assume changed on error to be safe

    def _check_file_dependencies_changed(self, file_state: FileState, environment: str) -> bool:
        """Check if file-specific dependencies have changed.
        
        Args:
            file_state: FileState object to check
            environment: Environment name
            
        Returns:
            True if file dependencies have changed, False otherwise
        """
        try:
            # Get current file dependencies
            source_path = self.project_root / file_state.source_yaml
            if not source_path.exists():
                return False  # Will be handled as orphaned
            
            current_file_deps = self.dependency_resolver.resolve_file_dependencies(
                source_path, environment
            )
            
            # Get stored file dependencies
            stored_file_deps = file_state.file_dependencies or {}
            
            # Compare dependency sets
            current_dep_paths = set(current_file_deps.keys())
            stored_dep_paths = set(stored_file_deps.keys())
            
            # Check for added or removed dependencies
            if current_dep_paths != stored_dep_paths:
                self.logger.debug(f"File dependencies changed for {file_state.generated_path}: added={current_dep_paths - stored_dep_paths}, removed={stored_dep_paths - current_dep_paths}")
                return True
            
            # Check for changes in existing dependencies
            for dep_path in current_dep_paths:
                if self._dependency_changed(stored_file_deps.get(dep_path), current_file_deps.get(dep_path)):
                    self.logger.debug(f"File dependency {dep_path} changed for {file_state.generated_path}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Failed to check file dependencies for {file_state.generated_path}: {e}")
            return True  # Assume changed on error to be safe

    def _dependency_changed(self, stored_dep: Optional[DependencyInfo], current_dep: Optional[DependencyInfo]) -> bool:
        """Check if a dependency has changed.
        
        Args:
            stored_dep: Stored dependency info (can be None)
            current_dep: Current dependency info (can be None)
            
        Returns:
            True if dependency has changed, False otherwise
        """
        # If one exists and the other doesn't, it's changed
        if bool(stored_dep) != bool(current_dep):
            return True
        
        # If both are None, no change
        if not stored_dep and not current_dep:
            return False
        
        # Both exist - compare checksums
        return stored_dep.checksum != current_dep.checksum

    def get_detailed_staleness_info(self, environment: str) -> Dict[str, Any]:
        """Get detailed information about which dependencies changed for each file.
        
        Args:
            environment: Environment name
            
        Returns:
            Dictionary with detailed staleness information
        """
        result = {
            "global_changes": [],
            "files": {}
        }
        
        env_files = self._state.environments.get(environment, {})
        if not env_files:
            return result
        
        # Check for global dependency changes
        global_deps_changed = self._check_global_dependencies_changed(environment)
        
        if global_deps_changed:
            # Determine which global dependencies changed
            try:
                current_global_deps = self.dependency_resolver.resolve_global_dependencies(environment)
                stored_global_deps = self._state.global_dependencies.get(environment) if self._state.global_dependencies else None
                
                if not stored_global_deps:
                    if f"substitutions/{environment}.yaml" in current_global_deps:
                        result["global_changes"].append(f"Substitution file (substitutions/{environment}.yaml) added")
                    if "lhp.yaml" in current_global_deps:
                        result["global_changes"].append("Project config (lhp.yaml) added")
                else:
                    # Compare specific global dependencies
                    if self._dependency_changed(
                        stored_global_deps.substitution_file,
                        current_global_deps.get(f"substitutions/{environment}.yaml")
                    ):
                        result["global_changes"].append(f"Substitution file (substitutions/{environment}.yaml) changed")
                    
                    if self._dependency_changed(
                        stored_global_deps.project_config,
                        current_global_deps.get("lhp.yaml")
                    ):
                        result["global_changes"].append("Project config (lhp.yaml) changed")
                        
            except Exception as e:
                self.logger.warning(f"Failed to analyze global dependency changes: {e}")
                result["global_changes"].append("Global dependencies changed (details unavailable)")
        
        # If global dependencies changed, all files are stale
        if global_deps_changed:
            for file_state in env_files.values():
                result["files"][file_state.generated_path] = {
                    "reasons": ["Global dependencies changed"],
                    "details": result["global_changes"]
                }
        else:
            # Check individual files for staleness
            for file_state in env_files.values():
                reasons = []
                details = []
                
                source_path = self.project_root / file_state.source_yaml
                
                if not source_path.exists():
                    # This will be handled by find_orphaned_files
                    continue
                
                # Check if source YAML has changed
                current_source_checksum = self._calculate_checksum(source_path)
                source_changed = (
                    not file_state.source_yaml_checksum
                    or current_source_checksum != file_state.source_yaml_checksum
                )
                
                if source_changed:
                    reasons.append("Source YAML changed")
                    details.append(f"Source file {file_state.source_yaml} was modified")
                
                # Check file-specific dependencies
                file_deps_info = self._get_file_dependency_changes(file_state, environment)
                if file_deps_info:
                    reasons.append("File dependencies changed")
                    details.extend(file_deps_info)
                
                # Only add to result if there are changes
                if reasons:
                    result["files"][file_state.generated_path] = {
                        "reasons": reasons,
                        "details": details
                    }
        
        return result

    def _get_file_dependency_changes(self, file_state: FileState, environment: str) -> List[str]:
        """Get detailed information about file dependency changes.
        
        Args:
            file_state: FileState object to check
            environment: Environment name
            
        Returns:
            List of detailed change descriptions
        """
        changes = []
        
        try:
            # Get current file dependencies
            source_path = self.project_root / file_state.source_yaml
            if not source_path.exists():
                return changes
            
            current_file_deps = self.dependency_resolver.resolve_file_dependencies(
                source_path, environment
            )
            
            # Get stored file dependencies
            stored_file_deps = file_state.file_dependencies or {}
            
            # Compare dependency sets
            current_dep_paths = set(current_file_deps.keys())
            stored_dep_paths = set(stored_file_deps.keys())
            
            # Check for added dependencies
            added_deps = current_dep_paths - stored_dep_paths
            for dep_path in added_deps:
                dep_info = current_file_deps[dep_path]
                changes.append(f"Added {dep_info.type}: {dep_path}")
            
            # Check for removed dependencies
            removed_deps = stored_dep_paths - current_dep_paths
            for dep_path in removed_deps:
                dep_info = stored_file_deps[dep_path]
                changes.append(f"Removed {dep_info.type}: {dep_path}")
            
            # Check for changes in existing dependencies
            for dep_path in current_dep_paths & stored_dep_paths:
                stored_dep = stored_file_deps[dep_path]
                current_dep = current_file_deps[dep_path]
                
                if stored_dep.checksum != current_dep.checksum:
                    changes.append(f"Modified {current_dep.type}: {dep_path}")
            
        except Exception as e:
            self.logger.warning(f"Failed to analyze file dependencies for {file_state.generated_path}: {e}")
            changes.append("File dependencies changed (details unavailable)")
        
        return changes

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

    def scan_generated_directory(self, output_dir: Path) -> Set[Path]:
        """
        Scan the generated directory for all Python files.
        
        Args:
            output_dir: Output directory to scan (typically 'generated/')
            
        Returns:
            Set of absolute paths to all .py files found in the directory tree
        """
        if not output_dir.exists():
            return set()
            
        python_files = set()
        
        # Recursively find all .py files
        try:
            for py_file in output_dir.rglob("*.py"):
                if py_file.is_file():
                    python_files.add(py_file.resolve())
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Failed to scan directory {output_dir}: {e}")
            
        return python_files
    
    def is_lhp_generated_file(self, file_path: Path) -> bool:
        """
        Check if a Python file was generated by LakehousePlumber.
        
        Safety check to ensure we only remove files we created.
        
        Args:
            file_path: Path to the Python file to check
            
        Returns:
            True if file has LHP generation header, False otherwise
        """
        if not file_path.exists() or file_path.suffix != '.py':
            return False
            
        try:
            # Read first few lines to check for LHP header
            with open(file_path, 'r', encoding='utf-8') as f:
                # Check first 5 lines for LHP header comment
                for i, line in enumerate(f):
                    if i >= 5:  # Only check first 5 lines
                        break
                    if "Generated by LakehousePlumber" in line:
                        return True
                        
        except (OSError, UnicodeDecodeError) as e:
            self.logger.warning(f"Failed to read file {file_path}: {e}")
            
        return False

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
            self._cleanup_empty_directories(environment, deleted_files)
            self._save_state()

        return deleted_files

    def _cleanup_empty_directories(self, environment: str, deleted_files: List[str] = None):
        """Remove empty directories in the generated output path."""
        output_dirs = set()

        # Collect all output directories for this environment (remaining files)
        for file_state in self._state.environments.get(environment, {}).values():
            output_path = self.project_root / file_state.generated_path
            output_dirs.add(output_path.parent)

        # Add directories of recently deleted files
        if deleted_files:
            base_generated_dir = self.project_root / "generated"
            for deleted_file in deleted_files:
                deleted_path = self.project_root / deleted_file
                
                # Only process files within the generated directory
                try:
                    if deleted_path.is_relative_to(base_generated_dir):
                        # Add immediate parent
                        output_dirs.add(deleted_path.parent)
                        
                        # Add parent directories up to (but not including) generated/
                        parent = deleted_path.parent
                        while (parent != base_generated_dir and 
                               parent.is_relative_to(base_generated_dir)):
                            output_dirs.add(parent)
                            parent = parent.parent
                except ValueError:
                    # Path is not relative to generated directory, skip
                    self.logger.debug(f"Skipping cleanup for file outside generated/: {deleted_file}")
                    continue

        # Also check common output directories (only within generated/)
        base_output_dir = self.project_root / "generated"
        if base_output_dir.exists():
            for item in base_output_dir.rglob("*"):
                if item.is_dir():
                    output_dirs.add(item)

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

        # Get all YAML files first
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

        # Filter by pipeline field content if specific pipeline requested
        if pipeline:
            pipeline_filtered_files = set()
            from ..parsers.yaml_parser import YAMLParser
            parser = YAMLParser()
            
            for yaml_file in yaml_files:
                try:
                    flowgroup = parser.parse_flowgroup(yaml_file)
                    if flowgroup.pipeline == pipeline:
                        pipeline_filtered_files.add(yaml_file)
                except Exception as e:
                    self.logger.debug(f"Could not parse {yaml_file} for pipeline filtering: {e}")
                    # Skip files that can't be parsed
                    continue
            
            yaml_files = pipeline_filtered_files

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

    def calculate_expected_files(self, output_dir: Path, env: str = None) -> Set[Path]:
        """
        Calculate what Python files should exist based on current YAML configuration.
        
        Uses the same logic as the orchestrator to discover flowgroups and map them
        to their expected output file paths.
        
        Args:
            output_dir: Output directory (typically 'generated/')
            env: Environment name (used for logging context)
            
        Returns:
            Set of absolute paths to files that should exist based on current config
        """
        expected_files = set()
        
        try:
            # Import here to avoid circular imports
            from .orchestrator import ActionOrchestrator
            
            # Use orchestrator to discover all flowgroups using current include patterns
            orchestrator = ActionOrchestrator(self.project_root)
            all_flowgroups = orchestrator.discover_all_flowgroups()
            
            # Map each flowgroup to its expected output file path
            for flowgroup in all_flowgroups:
                # File path pattern: {output_dir}/{pipeline}/{flowgroup}.py
                expected_file = output_dir / flowgroup.pipeline / f"{flowgroup.flowgroup}.py"
                expected_files.add(expected_file.resolve())
                
                self.logger.debug(f"Expected file: {expected_file} (from {flowgroup.pipeline}/{flowgroup.flowgroup})")
                
        except Exception as e:
            self.logger.warning(f"Failed to calculate expected files: {e}")
            
        if env:
            self.logger.debug(f"Calculated {len(expected_files)} expected files for environment '{env}'")
            
        return expected_files

    def cleanup_untracked_files(self, output_dir: Path, env: str) -> List[str]:
        """
        Perform filesystem-aware cleanup of untracked files.
        
        This is used for 'fresh start' scenarios when no state file exists,
        such as after cloning a repository or when state tracking is initialized.
        
        Args:
            output_dir: Output directory to clean (typically 'generated/')
            env: Environment name for logging context
            
        Returns:
            List of file paths that were deleted
        """
        deleted_files = []
        
        if not output_dir.exists():
            self.logger.debug(f"Output directory {output_dir} doesn't exist, nothing to cleanup")
            return deleted_files
        
        try:
            # 1. Scan filesystem for all Python files
            existing_files = self.scan_generated_directory(output_dir)
            
            # 2. Calculate what files should exist based on current config
            expected_files = self.calculate_expected_files(output_dir, env)
            
            # 3. Find orphaned files (exist but shouldn't)
            orphaned_files = existing_files - expected_files
            
            self.logger.debug(f"Fresh start cleanup analysis:")
            self.logger.debug(f"  Existing files: {len(existing_files)}")
            self.logger.debug(f"  Expected files: {len(expected_files)}")
            self.logger.debug(f"  Orphaned files: {len(orphaned_files)}")
            
            # 4. Safety check: only remove LHP-generated files
            lhp_orphaned_files = [
                f for f in orphaned_files 
                if self.is_lhp_generated_file(f)
            ]
            
            if lhp_orphaned_files:
                self.logger.info(f"🧹 Fresh start cleanup: removing {len(lhp_orphaned_files)} orphaned LHP file(s)")
                
                for file_path in lhp_orphaned_files:
                    try:
                        file_path.unlink()
                        deleted_files.append(str(file_path.relative_to(self.project_root)))
                        self.logger.debug(f"Deleted orphaned file: {file_path}")
                        
                        # Clean up empty directories
                        parent_dir = file_path.parent
                        try:
                            if parent_dir.exists() and not any(parent_dir.iterdir()):
                                parent_dir.rmdir()
                                self.logger.debug(f"Removed empty directory: {parent_dir}")
                        except OSError:
                            # Directory not empty or permission issue, ignore
                            pass
                            
                    except OSError as e:
                        self.logger.warning(f"Failed to delete {file_path}: {e}")
                        
            else:
                self.logger.debug("Fresh start cleanup: no orphaned LHP files found")
                
        except Exception as e:
            self.logger.error(f"Fresh start cleanup failed: {e}")
            
        return deleted_files

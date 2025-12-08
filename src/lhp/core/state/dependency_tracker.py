"""Dependency tracking service for LakehousePlumber state management."""

import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

# Import state models from separate module
from ..state_models import DependencyInfo, GlobalDependencies, FileState, ProjectState


class DependencyTracker:
    """
    Service for tracking file dependencies and checksums.
    
    Handles file registration, dependency resolution, checksum calculation,
    and provides file lookup functionality for the state management system.
    """
    
    def __init__(self, project_root: Path):
        """
        Initialize dependency tracker.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        
        # Initialize dependency resolver
        from ..state_dependency_resolver import StateDependencyResolver
        self.dependency_resolver = StateDependencyResolver(project_root)
    
    def track_generated_file(self, state: ProjectState, generated_path: Path, source_yaml: Path,
                           environment: str, pipeline: str, flowgroup: str, generation_context: str = "",
                           used_substitution_keys: Optional[List[str]] = None) -> None:
        """
        Track a generated file in the state with dependency resolution.
        
        Args:
            state: ProjectState to update
            generated_path: Path to the generated file
            source_yaml: Path to the source YAML file
            environment: Environment name
            pipeline: Pipeline name
            flowgroup: FlowGroup name
            generation_context: Optional context string for parameter-sensitive hashing
            used_substitution_keys: Optional list of substitution keys used during generation
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
        
        generated_checksum = self.calculate_checksum(resolved_generated_path)
        source_checksum = self.calculate_checksum(resolved_source_yaml)

        # Resolve file-specific dependencies
        # Ensure rel_source is a Path object
        rel_source_path = Path(rel_source) if isinstance(rel_source, str) else rel_source
        file_dependencies = self.dependency_resolver.resolve_file_dependencies(
            rel_source_path, environment, pipeline, flowgroup
        )
        
        # Calculate composite checksum for all dependencies
        dep_paths = [str(rel_source)] + list(file_dependencies.keys())
        # Include generation context for parameter-sensitive hashing
        if generation_context:
            dep_paths.append(generation_context)
        composite_checksum = self.dependency_resolver.calculate_composite_checksum(dep_paths)

        # Create file state (normalize paths for cross-platform state files)
        file_state = FileState(
            source_yaml=Path(str(rel_source)).as_posix(),
            generated_path=Path(str(rel_generated)).as_posix(),
            checksum=generated_checksum,
            source_yaml_checksum=source_checksum,
            timestamp=datetime.now().isoformat(),
            environment=environment,
            pipeline=pipeline,
            flowgroup=flowgroup,
            file_dependencies=file_dependencies,
            file_composite_checksum=composite_checksum,
            used_substitution_keys=used_substitution_keys
        )

        # Ensure environment exists in state
        if environment not in state.environments:
            state.environments[environment] = {}

        # Track the file (normalize dictionary key for cross-platform lookups)
        state.environments[environment][Path(str(rel_generated)).as_posix()] = file_state

        # Update global dependencies for this environment
        self.update_global_dependencies(state, environment)

        self.logger.debug(f"Tracked generated file: {rel_generated} from {rel_source} with {len(file_dependencies)} dependencies")

    def update_global_dependencies(self, state: ProjectState, environment: str) -> None:
        """
        Update global dependencies for an environment.
        
        Args:
            state: ProjectState to update
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
            if state.global_dependencies is None:
                state.global_dependencies = {}
            
            # Update global dependencies for this environment
            state.global_dependencies[environment] = GlobalDependencies(
                substitution_file=substitution_file,
                project_config=project_config
            )
            
            self.logger.debug(f"Updated global dependencies for environment: {environment}")
            
        except Exception as e:
            self.logger.warning(f"Failed to update global dependencies for {environment}: {e}")

    def get_generated_files(self, state: ProjectState, environment: str) -> Dict[str, FileState]:
        """
        Get all generated files for an environment.
        
        Args:
            state: ProjectState to query
            environment: Environment name
            
        Returns:
            Dictionary mapping file paths to FileState objects
        """
        return state.environments.get(environment, {})

    def get_files_by_source(self, state: ProjectState, source_yaml: Path, 
                          environment: str) -> List[FileState]:
        """
        Get all files generated from a specific source YAML.
        
        Args:
            state: ProjectState to query
            source_yaml: Path to the source YAML file
            environment: Environment name
            
        Returns:
            List of FileState objects for files generated from this source
        """
        try:
            rel_source = str(source_yaml.relative_to(self.project_root))
        except ValueError:
            rel_source = str(source_yaml)

        env_files = state.environments.get(environment, {})
        # Normalize paths for comparison to handle cross-platform differences
        return [
            file_state
            for file_state in env_files.values()
            if Path(file_state.source_yaml).as_posix() == Path(rel_source).as_posix()
        ]

    def calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.
        
        Args:
            file_path: Path to file for checksum calculation
            
        Returns:
            SHA256 hexdigest string, empty string if calculation fails
        """
        sha256_hash = hashlib.sha256()
        try:
            with open(file_path, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(chunk)
            return sha256_hash.hexdigest()
        except Exception as e:
            self.logger.warning(f"Failed to calculate checksum for {file_path}: {e}")
            return ""
    
    def get_file_dependencies_summary(self, state: ProjectState, environment: str) -> Dict[str, int]:
        """
        Get summary of dependencies across all tracked files.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            
        Returns:
            Dictionary with dependency statistics
        """
        env_files = state.environments.get(environment, {})
        dependency_counts = {}
        
        for file_state in env_files.values():
            if file_state.file_dependencies:
                dep_count = len(file_state.file_dependencies)
                dependency_counts[file_state.generated_path] = dep_count
        
        return dependency_counts
    
    def get_all_dependency_files(self, state: ProjectState, environment: str) -> set:
        """
        Get all unique dependency files across tracked files.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            
        Returns:
            Set of unique dependency file paths
        """
        all_deps = set()
        env_files = state.environments.get(environment, {})
        
        # Add source YAML files
        for file_state in env_files.values():
            all_deps.add(file_state.source_yaml)
            
            # Add file-specific dependencies
            if file_state.file_dependencies:
                all_deps.update(file_state.file_dependencies.keys())
        
        # Add global dependencies
        if state.global_dependencies and environment in state.global_dependencies:
            global_deps = state.global_dependencies[environment]
            if global_deps.substitution_file:
                all_deps.add(global_deps.substitution_file.path)
            if global_deps.project_config:
                all_deps.add(global_deps.project_config.path)
        
        return all_deps

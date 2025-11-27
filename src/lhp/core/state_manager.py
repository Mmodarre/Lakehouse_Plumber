"""State management for LakehousePlumber generated files."""

from __future__ import annotations

import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, Set, List, Optional, Any, TYPE_CHECKING
from datetime import datetime
from dataclasses import dataclass, asdict
from collections import defaultdict

# State services imports
from .state.state_persistence import StatePersistence
from .state.state_analyzer import StateAnalyzer
from .state.state_cleanup_service import StateCleanupService
from .state.dependency_tracker import DependencyTracker


# Import state models from separate module to avoid circular imports
from .state_models import DependencyInfo, GlobalDependencies, FileState, ProjectState

if TYPE_CHECKING:
    from ..parsers.yaml_parser import YAMLParser


class StateManager:
    """
    State management facade for LakehousePlumber generated files (Service-based architecture).
    
    Implements the data layer interface and coordinates specialized services for 
    persistence, analysis, cleanup, and dependency tracking while maintaining 
    the same public API for backward compatibility.
    """

    def __init__(self, project_root: Path, state_file_name: str = ".lhp_state.json", 
                 discoverer=None, yaml_parser: Optional[YAMLParser] = None):
        """
        Initialize state manager with service composition.

        Args:
            project_root: Root directory of the LakehousePlumber project
            state_file_name: Name of the state file (default: .lhp_state.json)
            discoverer: Optional FlowgroupDiscoverer for file discovery (fixes circular import from Phase 2)
            yaml_parser: Optional YAML parser for shared caching
        """
        self.project_root = project_root
        self.state_file = project_root / state_file_name
        self.logger = logging.getLogger(__name__)
        self.discoverer = discoverer  # From Phase 2 circular import fix
        
        # Initialize services with service composition
        self.persistence = StatePersistence(project_root, state_file_name)
        self.analyzer = StateAnalyzer(project_root, yaml_parser)
        self.cleaner = StateCleanupService(project_root)
        self.tracker = DependencyTracker(project_root)
        
        # Load existing state through persistence service
        self._state = self.persistence.load_state()

        self.logger.info(
            f"Initialized StateManager with service-based architecture: {project_root}"
        )

    # ============================================================================
    # PUBLIC API PROPERTIES (Facade Pattern)
    # ============================================================================
    
    @property 
    def state(self) -> ProjectState:
        """
        Get the current project state.
        
        Returns:
            Current ProjectState object
        """
        return self._state
    
    # ============================================================================
    # PUBLIC API METHODS (Delegate to Services)
    # ============================================================================

    def state_file_exists(self) -> bool:
        """
        Check if the state file exists on the filesystem.
        
        Returns:
            True if state file exists, False otherwise
        """
        return self.persistence.state_file_exists()

    def track_generated_file(self, generated_path: Path, source_yaml: Path,
                           environment: str, pipeline: str, flowgroup: str, 
                           generation_context: str = "", used_substitution_keys: Optional[List[str]] = None) -> None:
        """
        Track a generated file in the state with dependency resolution.
        
        Args:
            generated_path: Path to the generated file
            source_yaml: Path to the source YAML file
            environment: Environment name
            pipeline: Pipeline name
            flowgroup: FlowGroup name
            generation_context: Optional context string for parameter-sensitive hashing
            used_substitution_keys: Optional list of substitution keys used during generation
        """
        self.tracker.track_generated_file(
            self._state, generated_path, source_yaml, environment, pipeline, flowgroup, 
            generation_context, used_substitution_keys
        )

    def remove_generated_file(self, generated_path: Path, environment: str) -> bool:
        """
        Remove a generated file from state tracking with proper validation and logging.
        
        Args:
            generated_path: Path to the generated file to remove
            environment: Environment name
            
        Returns:
            True if file was removed, False if file was not tracked
        """
        # Convert to relative path for consistent state storage
        try:
            rel_generated = generated_path.relative_to(self.project_root)
        except ValueError:
            # File is outside project root, use absolute path
            rel_generated = generated_path
        
        file_path_str = str(rel_generated)
        
        # Validate environment exists
        if environment not in self._state.environments:
            self.logger.warning(f"Environment '{environment}' not found in state")
            return False
        
        # Validate file is tracked
        if file_path_str not in self._state.environments[environment]:
            self.logger.debug(f"File not tracked in state: {file_path_str}")
            return False
        
        # Remove from state with logging
        del self._state.environments[environment][file_path_str]
        self.logger.info(f"Removed file from state tracking: {file_path_str} (env: {environment})")
        
        return True

    def get_generated_files(self, environment: str) -> Dict[str, FileState]:
        """
        Get all generated files for an environment.
        
        Args:
            environment: Environment name
            
        Returns:
            Dictionary mapping file paths to FileState objects
        """
        return self.tracker.get_generated_files(self._state, environment)
    
    def get_file_state(self, environment: str, file_path: str) -> Optional[FileState]:
        """
        Get file state for a specific generated file.
        
        Args:
            environment: Environment name
            file_path: Path to the generated file (relative to project root)
            
        Returns:
            FileState object if file is tracked, None otherwise
        """
        if environment not in self._state.environments:
            return None
        return self._state.environments[environment].get(file_path)

    def get_files_by_source(self, source_yaml: Path, environment: str) -> List[FileState]:
        """
        Get all files generated from a specific source YAML.
        
        Args:
            source_yaml: Path to the source YAML file
            environment: Environment name
            
        Returns:
            List of FileState objects for files generated from this source
        """
        return self.tracker.get_files_by_source(self._state, source_yaml, environment)

    def find_orphaned_files(self, environment: str) -> List[FileState]:
        """
        Find generated files whose source YAML files no longer exist or don't match include patterns.
        
        Args:
            environment: Environment name
            
        Returns:
            List of orphaned FileState objects
        """
        include_patterns = self.get_include_patterns()
        return self.cleaner.find_orphaned_files(self._state, environment, include_patterns)

    def find_stale_files(self, environment: str) -> List[FileState]:
        """
        Find generated files that need regeneration due to dependency changes.
        
        Args:
            environment: Environment name
            
        Returns:
            List of FileState objects for stale files
        """
        return self.analyzer.find_stale_files(self._state, environment, self.calculate_checksum)

    def get_files_needing_generation(self, environment: str, pipeline: str = None, 
                                   generation_context: Optional[Dict] = None) -> Dict[str, List]:
        """
        Get all files that need generation (new, stale, or untracked).
        
        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by
            generation_context: Optional generation context for parameter-sensitive staleness
            
        Returns:
            Dictionary with 'new', 'stale', and 'up_to_date' lists
        """
        include_patterns = self.get_include_patterns()
        return self.analyzer.get_files_needing_generation(
            self._state, environment, include_patterns, pipeline, generation_context
        )

    def cleanup_orphaned_files(self, environment: str, dry_run: bool = False) -> List[str]:
        """
        Remove generated files whose source YAML files no longer exist.
        
        Args:
            environment: Environment name
            dry_run: If True, only return what would be deleted without actually deleting
            
        Returns:
            List of file paths that were (or would be) deleted
        """
        include_patterns = self.get_include_patterns()
        deleted_files = self.cleaner.cleanup_orphaned_files(
            self._state, environment, include_patterns, dry_run
        )
        
        # Save state if files were actually deleted
        if not dry_run and deleted_files:
            self.save()
        
        return deleted_files

    def cleanup_untracked_files(self, output_dir: Path, env: str) -> List[str]:
        """
        Clean up Python files in output directory that are not tracked in state.
        
        Args:
            output_dir: Output directory to clean
            env: Environment name
            
        Returns:
            List of paths of files that were removed
        """
        return self.cleaner.cleanup_untracked_files(self._state, output_dir, env)

    def save(self) -> None:
        """Save the current state to file."""
        self.persistence.save_state(self._state)
        
    def save_state(self) -> None:
        """Save the current state to file. Alias for save() for backward compatibility."""
        self.save()

    def load_state(self) -> None:
        """Reload state from file."""
        self._state = self.persistence.load_state()

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the current state.
        
        Returns:
            Dictionary with statistics about tracked files
        """
        return self.analyzer.get_statistics(self._state)

    def calculate_checksum(self, file_path: Path) -> str:
        """
        Calculate SHA256 checksum of a file.
        
        Args:
            file_path: Path to file for checksum calculation
            
        Returns:
            SHA256 hexdigest string
        """
        return self.tracker.calculate_checksum(file_path)

    def cleanup_empty_directories(self, environment: str, deleted_files: Optional[List[str]] = None) -> None:
        """
        Remove empty directories in the generated output path.
        
        Args:
            environment: Environment name  
            deleted_files: Optional list of recently deleted files
        """
        self.cleaner.cleanup_empty_directories(self._state, environment, deleted_files)

    def is_lhp_generated_file(self, file_path: Path) -> bool:
        """
        Check if a Python file was generated by LakehousePlumber.
        
        Args:
            file_path: Path to the Python file to check
            
        Returns:
            True if file has LHP generation header, False otherwise
        """
        return self.cleaner.is_lhp_generated_file(file_path)

    def scan_generated_directory(self, output_dir: Path) -> Set[Path]:
        """
        Scan the generated directory for all Python files.
        
        Args:
            output_dir: Output directory to scan
            
        Returns:
            Set of Path objects for all Python files in output directory
        """
        return self.cleaner.scan_generated_directory(output_dir)

    def get_detailed_staleness_info(self, environment: str) -> Dict[str, Any]:
        """
        Get detailed information about which dependencies changed for each file.
        
        Args:
            environment: Environment name
            
        Returns:
            Dictionary with detailed staleness information
        """
        return self.analyzer.get_detailed_staleness_info(self._state, environment)

    def compare_with_current_state(self, environment: str, pipeline: str = None) -> Dict[str, Any]:
        """
        Compare current YAML files with tracked state to find changes.
        
        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by
            
        Returns:
            Dictionary with 'added', 'removed', and 'existing' file lists
        """
        include_patterns = self.get_include_patterns()
        return self.analyzer.compare_with_current_state(
            self._state, environment, include_patterns, pipeline
        )

    def calculate_expected_files(self, output_dir: Path, env: str = None) -> Set[Path]:
        """
        Calculate what Python files should exist based on current YAML configuration.
        
        Args:
            output_dir: Output directory where files should be generated
            env: Optional environment filter
            
        Returns:
            Set of absolute paths to files that should exist based on current config
        """
        return self.analyzer.calculate_expected_files(output_dir, env, self.discoverer)

    def find_new_yaml_files(self, environment: str, pipeline: str = None) -> List[Path]:
        """
        Find YAML files that exist but are not tracked in state.
        
        Args:
            environment: Environment name
            pipeline: Optional pipeline name to filter by
            
        Returns:
            List of Path objects for new YAML files
        """
        include_patterns = self.get_include_patterns()
        return self.analyzer.find_new_yaml_files(self._state, environment, include_patterns, pipeline)

    def get_current_yaml_files(self, pipeline: str = None) -> Set[Path]:
        """
        Get all current YAML files in the pipelines directory.
        
        Args:
            pipeline: Optional pipeline name to filter by (content-based filtering)
            
        Returns:
            Set of Path objects for current YAML files
        """
        include_patterns = self.get_include_patterns()
        current_files = self.analyzer.get_current_yaml_files(include_patterns)
        
        # Apply pipeline content filtering if specified
        if pipeline:
            pipeline_filtered = set()
            
            # Parse each YAML file to check its pipeline field (supports multi-flowgroup files)
            for yaml_file in current_files:
                try:
                    from ..parsers.yaml_parser import YAMLParser
                    yaml_parser = YAMLParser()
                    # Parse all flowgroups from file (supports multi-document and array syntax)
                    flowgroups = yaml_parser.parse_flowgroups_from_file(yaml_file)
                    
                    # Check if ANY flowgroup in this file matches the requested pipeline
                    for fg in flowgroups:
                        if fg.pipeline == pipeline:
                            pipeline_filtered.add(yaml_file)
                            break  # File matches, no need to check other flowgroups
                
                except Exception:
                    # Skip files that can't be parsed
                    continue
            
            return pipeline_filtered
        
        return current_files

    # ============================================================================
    # UTILITY METHODS (Helper Functions)
    # ============================================================================

    def get_include_patterns(self) -> List[str]:
        """
        Get include patterns from project configuration.
        
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

    # ============================================================================
    # DATA LAYER INTERFACE IMPLEMENTATION  
    # ============================================================================
    
    def get_generation_state(self, env: str, pipeline: str = None) -> Dict[str, List]:
        """Get current generation state from persistence."""
        return self.get_files_needing_generation(env, pipeline)
    
    def track_generated_file_metadata(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """Track generated file in persistent state using metadata dict."""
        # Extract metadata for tracking
        source_yaml = metadata.get('source_yaml')
        environment = metadata.get('environment')
        pipeline = metadata.get('pipeline')
        flowgroup = metadata.get('flowgroup')
        generation_context = metadata.get('generation_context', '')
        
        if all([source_yaml, environment, pipeline, flowgroup]):
            self.track_generated_file(
                generated_path=file_path,
                source_yaml=source_yaml,
                environment=environment,
                pipeline=pipeline,
                flowgroup=flowgroup,
                generation_context=generation_context
            )

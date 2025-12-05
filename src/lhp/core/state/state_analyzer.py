"""State analysis service for LakehousePlumber."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Set, Any, Optional, TYPE_CHECKING
from collections import defaultdict

# Import state models from separate module
from ..state_models import FileState, ProjectState, DependencyInfo

if TYPE_CHECKING:
    from ...parsers.yaml_parser import YAMLParser


class StateAnalyzer:
    """
    Service for state analysis, staleness detection, and statistics.
    
    Handles complex analysis operations including dependency change detection,
    file staleness analysis, statistics generation, and smart generation planning.
    """
    
    def __init__(self, project_root: Path, yaml_parser: Optional[YAMLParser] = None):
        """
        Initialize state analyzer.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
            yaml_parser: Optional YAML parser for shared caching
        """
        from ...parsers.yaml_parser import YAMLParser as YAMLParserClass
        
        self.project_root = project_root
        self.yaml_parser = yaml_parser or YAMLParserClass()
        self.logger = logging.getLogger(__name__)
        
        # Initialize dependency resolver (reused across all operations)
        from ..state_dependency_resolver import StateDependencyResolver
        self.dependency_resolver = StateDependencyResolver(project_root, self.yaml_parser)
        
        # Initialize dependency tracker (reused across all operations)
        from .dependency_tracker import DependencyTracker
        self.tracker = DependencyTracker(project_root)
    
    def find_stale_files(self, state: ProjectState, environment: str,
                        checksum_calculator) -> List[FileState]:
        """
        Find generated files that need regeneration due to dependency changes.
        
        This enhanced method checks for staleness due to:
        1. Source YAML file changes
        2. Global dependency changes (substitution files, project config)
        3. File-specific dependency changes (presets, templates)
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            checksum_calculator: Function to calculate file checksums
            
        Returns:
            List of FileState objects for stale files
        """
        stale = []
        env_files = state.environments.get(environment, {})

        if not env_files:
            return stale

        # Check for global dependency changes
        global_deps_changed = self.check_global_dependencies_changed(state, environment)
        
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
            current_source_checksum = checksum_calculator(source_path)
            source_changed = (
                not file_state.source_yaml_checksum
                or current_source_checksum != file_state.source_yaml_checksum
            )
            
            # Check if file-specific dependencies have changed
            file_deps_changed = self.check_file_dependencies_changed(file_state, environment)
            
            if source_changed or file_deps_changed:
                stale.append(file_state)
                reason = []
                if source_changed:
                    reason.append("source YAML changed")
                if file_deps_changed:
                    reason.append("file dependencies changed")
                self.logger.debug(f"File {file_state.generated_path} is stale: {', '.join(reason)}")

        return stale

    def check_global_dependencies_changed(self, state: ProjectState, environment: str) -> bool:
        """
        Check if global dependencies have changed for an environment.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            
        Returns:
            True if global dependencies changed, False otherwise
        """
        try:
            # Get current global dependencies
            current_global_deps = self.dependency_resolver.resolve_global_dependencies(environment)
            
            # Get stored global dependencies
            stored_global_deps = None
            if state.global_dependencies and environment in state.global_dependencies:
                stored_global_deps = state.global_dependencies[environment]
            
            if not stored_global_deps:
                # No stored global dependencies - consider changed if any exist now
                has_current_deps = bool(current_global_deps)
                if has_current_deps:
                    self.logger.debug(f"Global dependencies added for {environment}")
                return has_current_deps
            
            # Compare substitution file
            sub_changed = self.dependency_changed(
                stored_global_deps.substitution_file,
                current_global_deps.get(f"substitutions/{environment}.yaml")
            )
            
            # Compare project config
            config_changed = self.dependency_changed(
                stored_global_deps.project_config,
                current_global_deps.get("lhp.yaml")
            )
            
            return sub_changed or config_changed
            
        except Exception as e:
            self.logger.warning(f"Failed to check global dependencies for {environment}: {e}")
            # Assume changed to be safe
            return True

    def check_file_dependencies_changed(self, file_state: FileState, environment: str) -> bool:
        """
        Check if file-specific dependencies have changed.
        
        Args:
            file_state: FileState to check
            environment: Environment name
            
        Returns:
            True if file dependencies changed, False otherwise
        """
        try:
            # Get current file dependencies with mtime optimization
            source_path = Path(file_state.source_yaml)
            stored_deps = file_state.file_dependencies or {}
            current_deps = self.dependency_resolver.resolve_file_dependencies(
                source_path, environment, file_state.pipeline, file_state.flowgroup,
                stored_deps=stored_deps
            )
            
            # Compare with stored dependencies
            
            # Check if dependency sets are different
            if set(current_deps.keys()) != set(stored_deps.keys()):
                self.logger.debug(f"File dependency set changed for {file_state.generated_path}")
                return True
            
            # Check if any dependency content changed
            for dep_path, current_dep in current_deps.items():
                stored_dep = stored_deps.get(dep_path)
                if self.dependency_changed(stored_dep, current_dep):
                    self.logger.debug(f"File dependency {dep_path} changed for {file_state.generated_path}")
                    return True
            
            return False
            
        except Exception as e:
            self.logger.warning(f"Failed to check file dependencies for {file_state.generated_path}: {e}")
            # Assume changed to be safe
            return True

    def dependency_changed(self, stored_dep: Optional[DependencyInfo], 
                          current_dep: Optional[DependencyInfo]) -> bool:
        """
        Check if a dependency has changed.
        
        Args:
            stored_dep: Previously stored dependency info
            current_dep: Current dependency info
            
        Returns:
            True if dependency changed, False otherwise
        """
        # Handle cases where one is None and other is not
        if stored_dep is None and current_dep is None:
            return False
        if stored_dep is None or current_dep is None:
            return True
        
        # Compare checksums (most reliable indicator)
        return stored_dep.checksum != current_dep.checksum

    def get_files_needing_generation(self, state: ProjectState, environment: str,
                                   include_patterns: Optional[List[str]] = None,
                                   pipeline: Optional[str] = None,
                                   generation_context: Optional[Dict] = None) -> Dict[str, List]:
        """
        Get all files that need generation (new, stale, or untracked).
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            include_patterns: Optional include patterns for filtering
            pipeline: Optional pipeline name to filter by
            generation_context: Optional generation context for parameter-sensitive staleness
            
        Returns:
            Dictionary with 'new', 'stale', and 'up_to_date' lists
        """
        # Find stale files (YAML changed)
        stale_files = self.find_stale_files(state, environment, 
                                           lambda p: self._calculate_checksum_via_tracker(p))
        if pipeline:
            stale_files = [f for f in stale_files if f.pipeline == pipeline]

        # Find new YAML files (not tracked)
        new_files = self.find_new_yaml_files(state, environment, include_patterns, pipeline)

        # Find up-to-date files (reuse shared tracker instance)
        all_tracked = self.tracker.get_generated_files(state, environment)
        if pipeline:
            all_tracked = {
                path: file_state
                for path, file_state in all_tracked.items()
                if file_state.pipeline == pipeline
            }

        up_to_date = []
        for file_state in all_tracked.values():
            source_path = self.project_root / file_state.source_yaml
            if (
                source_path.exists()
                and file_state.source_yaml_checksum
                and self.tracker.calculate_checksum(source_path) == file_state.source_yaml_checksum
            ):
                up_to_date.append(file_state)

        return {"new": new_files, "stale": stale_files, "up_to_date": up_to_date}

    def get_statistics(self, state: ProjectState) -> Dict[str, Any]:
        """
        Get statistics about the current state.
        
        Args:
            state: ProjectState to analyze
            
        Returns:
            Dictionary with statistics about tracked files
        """
        stats = {
            "total_environments": len(state.environments),
            "environments": {},
        }

        for env_name, env_files in state.environments.items():
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

    def compare_with_current_state(self, state: ProjectState, environment: str,
                                  include_patterns: Optional[List[str]] = None,
                                  pipeline: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare current YAML files with tracked state to find changes.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            include_patterns: Optional include patterns for filtering
            pipeline: Optional pipeline name to filter by
            
        Returns:
            Dictionary with 'added', 'removed', and 'existing' file lists
        """
        # Get files with filename/directory filtering only
        current_yamls = self.get_current_yaml_files(include_patterns)
        
        # Apply pipeline content filtering if specified  
        if pipeline:
            pipeline_filtered_files = set()
            
            # Parse each YAML file to check its pipeline field (supports multi-flowgroup files)
            for yaml_file in current_yamls:
                try:
                    # Parse all flowgroups from file (supports multi-document and array syntax)
                    flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                    
                    # Check if ANY flowgroup in this file matches the requested pipeline
                    for fg in flowgroups:
                        if fg.pipeline == pipeline:
                            pipeline_filtered_files.add(yaml_file)
                            self.logger.debug(f"Found YAML file for pipeline '{pipeline}': {yaml_file}")
                            break  # File matches, no need to check other flowgroups
                
                except Exception as e:
                    self.logger.warning(f"Could not parse YAML file {yaml_file}: {e}")
                    continue
            
            current_yamls = pipeline_filtered_files
            self.logger.debug(f"Found {len(current_yamls)} YAML file(s) for pipeline '{pipeline}'")
            
        current_yaml_paths = {
            str(f.relative_to(self.project_root)) for f in current_yamls
        }

        # Get tracked source files for this environment
        tracked_sources = set()
        for file_state in state.environments.get(environment, {}).values():
            tracked_sources.add(file_state.source_yaml)

        # Filter by pipeline if specified
        if pipeline:
            tracked_sources = {
                file_state.source_yaml
                for file_state in state.environments.get(environment, {}).values()
                if file_state.pipeline == pipeline
            }

        return {
            "added": list(current_yaml_paths - tracked_sources),
            "removed": list(tracked_sources - current_yaml_paths),
            "existing": list(current_yaml_paths & tracked_sources),
        }

    def find_new_yaml_files(self, state: ProjectState, environment: str,
                           include_patterns: Optional[List[str]] = None,
                           pipeline: Optional[str] = None) -> List[Path]:
        """
        Find YAML files that exist but are not tracked in state.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            include_patterns: Optional include patterns for filtering
            pipeline: Optional pipeline name to filter by (YAML content field)
            
        Returns:
            List of Path objects for new YAML files
        """
        # Step 1: Get all files matching include patterns (filename/directory only)
        current_yamls = self.get_current_yaml_files(include_patterns)
        
        # Step 2: Apply pipeline content filtering if specified
        if pipeline:
            pipeline_filtered_files = set()
            
            # Parse each YAML file to check its pipeline field (supports multi-flowgroup files)
            for yaml_file in current_yamls:
                try:
                    # Parse all flowgroups from file (supports multi-document and array syntax)
                    flowgroups = self.yaml_parser.parse_flowgroups_from_file(yaml_file)
                    
                    # Check if ANY flowgroup in this file matches the requested pipeline
                    for fg in flowgroups:
                        if fg.pipeline == pipeline:
                            pipeline_filtered_files.add(yaml_file)
                            self.logger.debug(f"Found YAML file for pipeline '{pipeline}': {yaml_file}")
                            break  # File matches, no need to check other flowgroups
                
                except Exception as e:
                    self.logger.warning(f"Could not parse YAML file {yaml_file}: {e}")
                    # Skip files that can't be parsed
                    continue
            
            current_yamls = pipeline_filtered_files
            self.logger.debug(f"Found {len(current_yamls)} YAML file(s) for pipeline '{pipeline}'")
        
        # Get tracked source files
        tracked_sources = set()
        for file_state in state.environments.get(environment, {}).values():
            if pipeline is None or file_state.pipeline == pipeline:
                tracked_path = self.project_root / file_state.source_yaml
                tracked_sources.add(tracked_path)
        
        # Find new files
        new_files = [f for f in current_yamls if f not in tracked_sources]
        
        if new_files:
            self.logger.info(f"Found {len(new_files)} new YAML file(s) for {environment}")
            for f in new_files:
                self.logger.debug(f"  New file: {f.relative_to(self.project_root)}")
        
        return new_files

    def get_current_yaml_files(self, include_patterns: Optional[List[str]] = None) -> Set[Path]:
        """
        Get all current YAML files in the pipelines directory.
        
        Applies ONLY filename/directory-based filtering via include patterns.
        Content filtering (by pipeline field) happens at higher levels.
        
        Args:
            include_patterns: Optional include patterns for filtering (filename/directory only)
            
        Returns:
            Set of Path objects for current YAML files
        """
        yaml_files = set()
        pipelines_dir = self.project_root / "pipelines"
        
        if not pipelines_dir.exists():
            return yaml_files
        
        # Step 1: Find ALL YAML files in pipelines directory
        all_yaml_files = set()
        all_yaml_files.update(pipelines_dir.rglob("*.yaml"))
        all_yaml_files.update(pipelines_dir.rglob("*.yml"))

        # Step 2: Apply include pattern filtering (filename/directory only)
        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns
            filtered_by_patterns = discover_files_with_patterns(pipelines_dir, include_patterns)
            all_yaml_files = set(filtered_by_patterns)
        
        # Return files after filename/directory filtering only
        # Content filtering (by pipeline field) happens in calling methods
        return all_yaml_files

    def calculate_expected_files(self, output_dir: Path, env: Optional[str] = None,
                               discoverer=None) -> Set[Path]:
        """
        Calculate what Python files should exist based on current YAML configuration.
        
        Uses current flowgroup discovery to determine expected generated files.
        
        Args:
            output_dir: Output directory where files should be generated
            env: Optional environment filter
            discoverer: FlowgroupDiscoverer service for finding current flowgroups
            
        Returns:
            Set of absolute paths to files that should exist based on current config
        """
        expected_files = set()
        
        try:
            if discoverer:
                all_flowgroups = discoverer.discover_all_flowgroups()
            else:
                # Fallback: import and use discoverer directly
                from ..services.flowgroup_discoverer import FlowgroupDiscoverer
                from ..project_config_loader import ProjectConfigLoader
                config_loader = ProjectConfigLoader(self.project_root)
                temp_discoverer = FlowgroupDiscoverer(self.project_root, config_loader)
                all_flowgroups = temp_discoverer.discover_all_flowgroups()
            
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

    def get_detailed_staleness_info(self, state: ProjectState, environment: str) -> Dict[str, Any]:
        """
        Get detailed information about which dependencies changed for each file.
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            
        Returns:
            Dictionary with detailed staleness information
        """
        result = {
            "global_changes": [],
            "files": {}
        }
        
        env_files = state.environments.get(environment, {})
        if not env_files:
            return result
        
        # Check for global dependency changes
        global_deps_changed = self.check_global_dependencies_changed(state, environment)
        
        if global_deps_changed:
            # Determine which global dependencies changed
            try:
                current_global_deps = self.dependency_resolver.resolve_global_dependencies(environment)
                stored_global_deps = state.global_dependencies.get(environment) if state.global_dependencies else None
                
                if not stored_global_deps:
                    if f"substitutions/{environment}.yaml" in current_global_deps:
                        result["global_changes"].append(f"Substitution file (substitutions/{environment}.yaml) added")
                    if "lhp.yaml" in current_global_deps:
                        result["global_changes"].append("Project config (lhp.yaml) added")
                else:
                    # Compare specific global dependencies
                    if self.dependency_changed(
                        stored_global_deps.substitution_file,
                        current_global_deps.get(f"substitutions/{environment}.yaml")
                    ):
                        result["global_changes"].append(f"Substitution file (substitutions/{environment}.yaml) changed")
                    
                    if self.dependency_changed(
                        stored_global_deps.project_config,
                        current_global_deps.get("lhp.yaml")
                    ):
                        result["global_changes"].append("Project config (lhp.yaml) changed")
                        
            except Exception as e:
                self.logger.warning(f"Failed to analyze global dependency changes: {e}")
                result["global_changes"].append("Global dependencies changed (details unavailable)")
        
        # Check individual file changes (if global changes exist, all files are stale)
        if global_deps_changed:
            for file_path, file_state in env_files.items():
                result["files"][file_path] = {
                    "stale": True,
                    "reason": "global_dependency_change",
                    "details": result["global_changes"]
                }
        else:
            # Check each file individually
            for file_path, file_state in env_files.items():
                file_changes = self.get_file_dependency_changes(file_state, environment)
                
                if file_changes:
                    result["files"][file_path] = {
                        "stale": True,
                        "reason": "file_dependency_change", 
                        "details": file_changes
                    }
                else:
                    result["files"][file_path] = {
                        "stale": False,
                        "reason": "up_to_date",
                        "details": []
                    }
        
        return result

    def get_file_dependency_changes(self, file_state: FileState, environment: str) -> List[str]:
        """
        Get detailed information about file dependency changes.
        
        Args:
            file_state: FileState to analyze
            environment: Environment name
            
        Returns:
            List of change descriptions
        """
        changes = []
        
        try:
            # Check source YAML file change
            source_path = self.project_root / file_state.source_yaml
            if source_path.exists():
                # Reuse shared tracker instance
                current_checksum = self.tracker.calculate_checksum(source_path)
                
                if not file_state.source_yaml_checksum:
                    changes.append(f"Source YAML checksum missing: {file_state.source_yaml}")
                elif current_checksum != file_state.source_yaml_checksum:
                    changes.append(f"Source YAML changed: {file_state.source_yaml}")
            else:
                changes.append(f"Source YAML missing: {file_state.source_yaml}")
            
            # Check file-specific dependencies with mtime optimization
            source_yaml_path = Path(file_state.source_yaml)
            stored_deps = file_state.file_dependencies or {}
            current_deps = self.dependency_resolver.resolve_file_dependencies(
                source_yaml_path, environment, file_state.pipeline, file_state.flowgroup,
                stored_deps=stored_deps
            )
            
            # Check for added dependencies
            for dep_path in current_deps.keys() - stored_deps.keys():
                changes.append(f"New dependency added: {dep_path}")
            
            # Check for removed dependencies  
            for dep_path in stored_deps.keys() - current_deps.keys():
                changes.append(f"Dependency removed: {dep_path}")
            
            # Check for changed dependencies
            for dep_path in current_deps.keys() & stored_deps.keys():
                current_dep = current_deps[dep_path]
                stored_dep = stored_deps[dep_path]
                
                if self.dependency_changed(stored_dep, current_dep):
                    changes.append(f"Dependency changed: {dep_path}")
                    
        except Exception as e:
            self.logger.warning(f"Failed to analyze file dependency changes for {file_state.generated_path}: {e}")
            changes.append(f"Failed to analyze dependencies: {e}")
        
        return changes

    def _calculate_checksum_via_tracker(self, file_path: Path) -> str:
        """Helper to calculate checksum via DependencyTracker (reuses shared instance)."""
        return self.tracker.calculate_checksum(file_path)

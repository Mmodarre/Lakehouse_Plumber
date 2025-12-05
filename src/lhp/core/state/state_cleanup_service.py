"""State cleanup service for LakehousePlumber."""

import logging
from pathlib import Path
from typing import List, Set, Optional

# Import state models from separate module
from ..state_models import FileState, ProjectState


class StateCleanupService:
    """
    Service for safe file deletion and directory cleanup.
    
    Provides orphaned file detection, safe deletion with validation,
    and empty directory cleanup for state management operations.
    """
    
    def __init__(self, project_root: Path):
        """
        Initialize state cleanup service.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
    
    def find_orphaned_files(self, state: ProjectState, environment: str, 
                           include_patterns: Optional[List[str]] = None) -> List[FileState]:
        """
        Find generated files whose source YAML files no longer exist or don't match include patterns.
        
        A file is considered orphaned if:
        1. The source YAML file doesn't exist anymore, OR
        2. The source YAML file doesn't match include patterns (if patterns are specified)
        
        Args:
            state: ProjectState to analyze
            environment: Environment name
            include_patterns: Optional include patterns for filtering
            
        Returns:
            List of orphaned FileState objects
        """
        # Pattern matching utility function
        def check_pattern_match(file_path, patterns):
            """Check if file matches include patterns."""
            if not patterns:
                return True  # No patterns = all files match (backward compatibility)
            
            try:
                from ...utils.file_pattern_matcher import discover_files_with_patterns
                # Use the same logic as get_current_yaml_files() for consistency
                pipelines_dir = self.project_root / "pipelines"
                if pipelines_dir.exists():
                    # Get all matching files using proper base directory context
                    matched_files = discover_files_with_patterns(pipelines_dir, patterns)
                    # Check if our specific file is in the matched set
                    return file_path in matched_files
                return False
            except ImportError:
                self.logger.warning("file_pattern_matcher not available, assuming files match patterns")
                return True  
            except Exception as e:
                self.logger.warning(f"Error checking pattern match for {file_path}: {e}")
                return True  
        orphaned_files = []
        env_files = state.environments.get(environment, {})
        
        for file_state in env_files.values():
            source_path = self.project_root / file_state.source_yaml
            
            # Check if source YAML file exists
            if not source_path.exists():
                orphaned_files.append(file_state)
                self.logger.info(
                    f"Found orphaned file (source missing): {file_state.generated_path}"
                )
                continue
                
            # Check if source file matches include patterns (if patterns specified)
            if include_patterns:
                if not check_pattern_match(source_path, include_patterns):
                    orphaned_files.append(file_state)
                    self.logger.info(
                        f"Found orphaned file (include pattern mismatch): {file_state.generated_path}"
                    )
                    continue
                    
            # Check if pipeline or flowgroup field in source YAML has changed
            # Support multi-flowgroup files
            try:
                from ...parsers.yaml_parser import YAMLParser
                yaml_parser = YAMLParser()
                
                # Parse all flowgroups from file (supports multi-document and array syntax)
                flowgroups_in_file = yaml_parser.parse_flowgroups_from_file(source_path)
                
                # Check if the flowgroup still exists in the file
                flowgroup_found = False
                for fg in flowgroups_in_file:
                    if fg.pipeline == file_state.pipeline and fg.flowgroup == file_state.flowgroup:
                        flowgroup_found = True
                        break
                
                # If flowgroup not found in file, consider it orphaned
                if not flowgroup_found:
                    orphaned_files.append(file_state)
                    self.logger.info(
                        f"Found orphaned file (flowgroup '{file_state.flowgroup}' no longer in source): {file_state.generated_path}"
                    )
                    continue
                    
            except Exception as e:
                # If we can't parse the YAML, log warning but don't consider it orphaned
                # This prevents false positives from temporarily invalid YAML files
                self.logger.warning(
                    f"Could not parse YAML file {source_path} for orphaned check: {e}"
                )
        
        if orphaned_files:
            self.logger.info(f"Found {len(orphaned_files)} orphaned file(s) for environment: {environment}")
        else:
            self.logger.debug(f"No orphaned files found for environment: {environment}")
        
        return orphaned_files
    
    def cleanup_orphaned_files(self, state: ProjectState, environment: str, 
                              include_patterns: Optional[List[str]] = None,
                              dry_run: bool = False) -> List[str]:
        """
        Remove generated files whose source YAML files no longer exist.
        
        Args:
            state: ProjectState to update
            environment: Environment name
            include_patterns: Optional include patterns for filtering
            dry_run: If True, only return what would be deleted without actually deleting
            
        Returns:
            List of file paths that were (or would be) deleted
        """
        orphaned_files = self.find_orphaned_files(state, environment, include_patterns)
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

                    # Remove from state (normalize lookup key to match stored keys)
                    del state.environments[environment][Path(file_state.generated_path).as_posix()]

                except Exception as e:
                    self.logger.error(
                        f"Failed to delete {file_state.generated_path}: {e}"
                    )

        # Clean up empty directories
        if not dry_run and deleted_files:
            self.cleanup_empty_directories(state, environment, deleted_files)

        return deleted_files
    
    def cleanup_empty_directories(self, state: ProjectState, environment: str, 
                                 deleted_files: Optional[List[str]] = None) -> None:
        """
        Remove empty directories in the generated output path.
        
        Args:
            state: ProjectState for directory analysis
            environment: Environment name
            deleted_files: Optional list of recently deleted files
        """
        output_dirs = set()

        # Collect all output directories for this environment (remaining files)
        for file_state in state.environments.get(environment, {}).values():
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
    
    def scan_generated_directory(self, output_dir: Path) -> Set[Path]:
        """
        Scan the generated directory for all Python files.
        
        Args:
            output_dir: Output directory to scan
            
        Returns:
            Set of Path objects for all Python files in output directory
        """
        python_files = set()
        
        if not output_dir.exists():
            return python_files
        
        try:
            for py_file in output_dir.rglob("*.py"):
                # Convert to relative path from project root for consistent comparison
                try:
                    rel_path = py_file.relative_to(self.project_root)
                    python_files.add(rel_path)
                except ValueError:
                    # File outside project root, use absolute path
                    python_files.add(py_file)
                    
        except Exception as e:
            self.logger.warning(f"Error scanning directory {output_dir}: {e}")
        
        return python_files
    
    def cleanup_untracked_files(self, state: ProjectState, output_dir: Path, env: str) -> List[str]:
        """
        Clean up Python files in output directory that are not tracked in state.
        
        Safety check: Only removes files that have LHP generation headers.
        
        Args:
            state: ProjectState for tracking lookup
            output_dir: Output directory to clean
            env: Environment name
            
        Returns:
            List of paths of files that were removed
        """
        removed_files = []
        
        # Get all Python files in output directory
        found_files = self.scan_generated_directory(output_dir)
        
        # Get tracked files for this environment
        tracked_files = set()
        for file_state in state.environments.get(env, {}).values():
            tracked_files.add(Path(file_state.generated_path))
        
        # Find untracked files
        untracked_files = found_files - tracked_files
        
        for untracked_file in untracked_files:
            file_path = self.project_root / untracked_file
            
            # Safety check: Only remove files we generated
            if self.is_lhp_generated_file(file_path):
                try:
                    file_path.unlink()
                    removed_files.append(str(untracked_file))
                    self.logger.info(f"Removed untracked LHP file: {untracked_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to remove untracked file {untracked_file}: {e}")
            else:
                self.logger.debug(f"Skipping non-LHP file: {untracked_file}")
        
        # Clean up any empty directories
        if removed_files:
            self.cleanup_empty_directories(state, env, removed_files)
        
        return removed_files

"""State Display Service - Business logic for state command operations.

This service separates the business logic of state management from CLI presentation,
providing a clean, testable interface for state operations.
"""

import click
from typing import Dict, List, Optional, Any, Tuple
from pathlib import Path
from collections import defaultdict

from ..core.state_manager import StateManager
from ..utils.error_handler import ErrorHandler


class StateDisplayService:
    """Service for handling state display operations and business logic."""
    
    def __init__(self, state_manager: StateManager, project_root: Path, verbose: bool = False, log_file: Optional[str] = None):
        """Initialize the state display service.
        
        Args:
            state_manager: StateManager instance for state operations
            project_root: Root path of the project
            verbose: Whether to enable verbose logging
            log_file: Path to log file for detailed logging
        """
        self.state_manager = state_manager
        self.project_root = project_root
        self.verbose = verbose
        self.log_file = log_file
        self.error_handler = ErrorHandler(verbose)
    
    def get_overall_stats(self) -> Dict[str, Any]:
        """Get overall state statistics across all environments.
        
        Returns:
            Dictionary containing statistics data or None if no tracked files
        """
        stats = self.state_manager.get_statistics()
        
        if stats["total_environments"] == 0:
            return None  # Indicates no tracked files found
            
        return stats
    
    def get_tracked_files(self, env: str, pipeline: Optional[str] = None) -> Dict[str, Any]:
        """Get tracked files for environment, optionally filtered by pipeline.
        
        Args:
            env: Environment name
            pipeline: Optional pipeline filter
            
        Returns:
            Dictionary of tracked files or None if none found
        """
        tracked_files = self.state_manager.get_generated_files(env)
        
        if not tracked_files:
            return None
            
        # Filter by pipeline if specified
        if pipeline:
            tracked_files = {
                path: file_state
                for path, file_state in tracked_files.items()
                if file_state.pipeline == pipeline
            }
            
            if not tracked_files:
                return None
                
        return tracked_files
    
    def get_orphaned_files(self, env: str, pipeline: Optional[str] = None) -> List[Any]:
        """Get orphaned files for environment, optionally filtered by pipeline.
        
        Args:
            env: Environment name
            pipeline: Optional pipeline filter
            
        Returns:
            List of orphaned file states
        """
        orphaned_files = self.state_manager.find_orphaned_files(env)
        
        if pipeline:
            orphaned_files = [
                file_state
                for file_state in orphaned_files
                if file_state.pipeline == pipeline
            ]
            
        return orphaned_files
    
    def get_stale_files(self, env: str, pipeline: Optional[str] = None) -> Tuple[List[Any], Dict[str, Any]]:
        """Get stale files and staleness information.
        
        Args:
            env: Environment name
            pipeline: Optional pipeline filter
            
        Returns:
            Tuple of (stale_files_list, staleness_info_dict)
        """
        stale_files = self.state_manager.find_stale_files(env)
        
        if pipeline:
            stale_files = [
                file_state
                for file_state in stale_files
                if file_state.pipeline == pipeline
            ]
            
        staleness_info = self.state_manager.get_detailed_staleness_info(env)
        
        return stale_files, staleness_info
    
    def get_new_files(self, env: str, pipeline: Optional[str] = None) -> List[Path]:
        """Get new YAML files for environment, optionally filtered by pipeline.
        
        Args:
            env: Environment name
            pipeline: Optional pipeline filter
            
        Returns:
            List of new YAML file paths
        """
        new_files = self.state_manager.find_new_yaml_files(env)
        
        if pipeline:
            # Filter new files by pipeline
            filtered_new_files = []
            for yaml_file in new_files:
                try:
                    relative_path = yaml_file.relative_to(self.project_root)
                    if (
                        len(relative_path.parts) > 1
                        and relative_path.parts[1] == pipeline
                    ):
                        filtered_new_files.append(yaml_file)
                except ValueError:
                    continue
            new_files = filtered_new_files
            
        return new_files
    
    def cleanup_orphaned_files(self, env: str, dry_run: bool = False) -> List[str]:
        """Clean up orphaned files.
        
        Args:
            env: Environment name
            dry_run: Whether to perform dry run only
            
        Returns:
            List of deleted file paths
        """
        try:
            return self.state_manager.cleanup_orphaned_files(env, dry_run=dry_run)
        except Exception as e:
            # Use existing error handling framework
            raise self.error_handler.handle_file_error(
                e, "orphaned files", "cleanup"
            )
    
    def regenerate_stale_files(self, env: str, stale_files: List[Any], dry_run: bool = False) -> int:
        """Regenerate stale files.
        
        Args:
            env: Environment name
            stale_files: List of stale file states
            dry_run: Whether to perform dry run only
            
        Returns:
            Number of files regenerated
        """
        if dry_run:
            return 0  # No actual regeneration in dry run
            
        # Import here to avoid circular imports
        from ..core.orchestrator import ActionOrchestrator
        
        orchestrator = ActionOrchestrator(self.project_root)
        regenerated_count = 0
        
        # Group by pipeline
        by_pipeline = defaultdict(list)
        for file_state in stale_files:
            by_pipeline[file_state.pipeline].append(file_state)
        
        for pipeline_name, files in by_pipeline.items():
            try:
                output_dir = self.project_root / "generated" / pipeline_name
                generated_files = orchestrator.generate_pipeline(
                    pipeline_name, env, output_dir, state_manager=self.state_manager
                )
                regenerated_count += len(generated_files)
                
                # Log success
                if self.verbose:
                    self.error_handler.logger.info(
                        f"Regenerated {len(generated_files)} file(s) for {pipeline_name}"
                    )
                    
            except Exception as e:
                # Use existing error handling framework
                error_handler = ErrorHandler(self.verbose)
                error_handler.with_pipeline_context(
                    pipeline_name, env
                ).handle_cli_error(
                    e, f"Regeneration for pipeline '{pipeline_name}'"
                )
                # Re-raise to let CLI handle the display
                raise
        
        return regenerated_count
    
    def group_files_by_pipeline(self, files: List[Path]) -> Dict[str, List[Path]]:
        """Group files by pipeline based on their path structure.
        
        Args:
            files: List of file paths
            
        Returns:
            Dictionary mapping pipeline names to file lists
        """
        by_pipeline = defaultdict(list)
        
        for file_path in files:
            try:
                relative_path = file_path.relative_to(self.project_root)
                pipeline_name = relative_path.parts[1]  # pipelines/pipeline_name/...
                by_pipeline[pipeline_name].append(file_path)
            except (ValueError, IndexError):
                by_pipeline["unknown"].append(file_path)
                
        return dict(by_pipeline)
    
    def calculate_file_status(self, file_state: Any) -> Tuple[bool, bool, str]:
        """Calculate the status of a tracked file.
        
        Args:
            file_state: File state object
            
        Returns:
            Tuple of (source_exists, generated_exists, change_status)
        """
        # Check if source still exists
        source_path = self.project_root / file_state.source_yaml
        source_exists = source_path.exists()
        
        # Check if generated file still exists
        generated_path = self.project_root / file_state.generated_path
        generated_exists = generated_path.exists()
        
        # Check if source has changed (stale)
        change_status = ""
        if source_exists and file_state.source_yaml_checksum:
            current_checksum = self.state_manager._calculate_checksum(source_path)
            if current_checksum != file_state.source_yaml_checksum:
                change_status = " 🟡 (stale)"
            else:
                change_status = " 🟢 (up-to-date)"
        elif source_exists and not file_state.source_yaml_checksum:
            change_status = " 🟡 (unknown)"
            
        return source_exists, generated_exists, change_status
    
    def calculate_summary_counts(self, env: str, pipeline: Optional[str] = None) -> Dict[str, int]:
        """Calculate summary counts for different file types.
        
        Args:
            env: Environment name
            pipeline: Optional pipeline filter
            
        Returns:
            Dictionary with counts for different file types
        """
        # Get all file counts
        tracked_files = self.get_tracked_files(env, pipeline) or {}
        orphaned_files = self.get_orphaned_files(env, pipeline)
        stale_files, _ = self.get_stale_files(env, pipeline)
        new_files = self.get_new_files(env, pipeline)
        
        # Calculate counts
        total_tracked = len(tracked_files)
        orphaned_count = len(orphaned_files)
        stale_count = len(stale_files)
        new_count = len(new_files)
        up_to_date_count = total_tracked - stale_count
        
        return {
            "total_tracked": total_tracked,
            "orphaned_count": orphaned_count,
            "stale_count": stale_count,
            "new_count": new_count,
            "up_to_date_count": up_to_date_count
        } 
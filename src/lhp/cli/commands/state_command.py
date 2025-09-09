"""State command implementation for LakehousePlumber CLI."""

from typing import Optional
import click

from .base_command import BaseCommand
from ...core.state_manager import StateManager
from ...services.state_display_service import StateDisplayService
from ...services.state_display_utils import StateDisplayUtils


class StateCommand(BaseCommand):
    """
    Handles state management and display command.
    
    Shows and manages the current state of generated files including
    tracking, staleness detection, cleanup operations, and regeneration.
    """
    
    def execute(self, env: Optional[str] = None, pipeline: Optional[str] = None,
                orphaned: bool = False, stale: bool = False, new: bool = False,
                dry_run: bool = False, cleanup: bool = False, regen: bool = False) -> None:
        """
        Execute the state command.
        
        Args:
            env: Environment to show state for
            pipeline: Specific pipeline to show state for
            orphaned: Show only orphaned files
            stale: Show only stale files (YAML changed)
            new: Show only new/untracked YAML files
            dry_run: Preview cleanup without actually deleting files
            cleanup: Clean up orphaned files
            regen: Regenerate stale files
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")
        
        # Initialize state management
        state_manager = StateManager(project_root)
        service = StateDisplayService(state_manager, project_root, self.verbose, self.log_file)
        
        # Handle no environment specified - show overall stats
        if not env:
            self._display_overall_stats(service)
            return
        
        # Handle environment-specific operations
        StateDisplayUtils.display_environment_header(env)
        
        # Check if any tracked files exist for this environment/pipeline
        tracked_files = service.get_tracked_files(env, pipeline)
        
        if not tracked_files and not new:
            StateDisplayUtils.display_missing_tracked_files(env, pipeline)
            return
        
        # Handle specific flag-based operations
        if orphaned:
            self._handle_orphaned_files_operation(service, env, pipeline, cleanup, dry_run)
            return
        
        if stale:
            self._handle_stale_files_operation(service, env, pipeline, regen, dry_run)
            return
        
        if new:
            self._handle_new_files_operation(service, env, pipeline, project_root)
            return
        
        # Default comprehensive view
        self._display_comprehensive_view(service, tracked_files, env, pipeline, project_root)
    
    def _display_overall_stats(self, service: StateDisplayService) -> None:
        """Display overall statistics across all environments."""
        stats = service.get_overall_stats()
        if stats is None:
            StateDisplayUtils.display_no_tracked_files_message()
        else:
            StateDisplayUtils.display_overall_stats(stats)
    
    def _handle_orphaned_files_operation(self, service: StateDisplayService, env: str,
                                        pipeline: Optional[str], cleanup: bool, 
                                        dry_run: bool) -> None:
        """Handle orphaned files display and cleanup."""
        orphaned_files = service.get_orphaned_files(env, pipeline)
        StateDisplayUtils.display_orphaned_files(orphaned_files, cleanup, dry_run)
        
        if cleanup and orphaned_files and not dry_run:
            deleted_files = service.cleanup_orphaned_files(env, dry_run)
            StateDisplayUtils.display_cleanup_results(deleted_files)
    
    def _handle_stale_files_operation(self, service: StateDisplayService, env: str,
                                     pipeline: Optional[str], regen: bool, 
                                     dry_run: bool) -> None:
        """Handle stale files display and regeneration."""
        stale_files, staleness_info = service.get_stale_files(env, pipeline)
        StateDisplayUtils.display_stale_files(stale_files, staleness_info, regen, dry_run)
        
        if regen and stale_files and not dry_run:
            try:
                regenerated_count = service.regenerate_stale_files(env, stale_files, dry_run)
                StateDisplayUtils.display_regeneration_results(regenerated_count)
            except Exception:
                # Error already handled by service layer
                pass
    
    def _handle_new_files_operation(self, service: StateDisplayService, env: str,
                                   pipeline: Optional[str], project_root) -> None:
        """Handle new files display."""
        new_files = service.get_new_files(env, pipeline)
        by_pipeline = service.group_files_by_pipeline(new_files)
        StateDisplayUtils.display_new_files(new_files, project_root, env, by_pipeline)
    
    def _display_comprehensive_view(self, service: StateDisplayService, tracked_files,
                                   env: str, pipeline: Optional[str], project_root) -> None:
        """Display comprehensive state view with all file types."""
        # Display tracked files
        StateDisplayUtils.display_tracked_files(
            tracked_files, project_root, service.calculate_file_status
        )
        
        # Get new files for summary
        new_files = service.get_new_files(env, pipeline)
        
        # Display comprehensive summary
        counts = service.calculate_summary_counts(env, pipeline)
        by_pipeline = service.group_files_by_pipeline(new_files)
        
        StateDisplayUtils.display_new_files_in_summary(new_files, project_root, env, by_pipeline)
        StateDisplayUtils.display_comprehensive_summary(counts, env)

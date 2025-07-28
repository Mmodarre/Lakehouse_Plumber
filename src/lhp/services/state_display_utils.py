"""State Display Utilities - Presentation logic for state command.

This module contains all the CLI presentation logic separated from business logic,
making it easier to test and maintain.
"""

import click
from typing import Dict, List, Optional, Any
from pathlib import Path
from collections import defaultdict


class StateDisplayUtils:
    """Utilities for displaying state information in the CLI."""
    
    @staticmethod
    def display_overall_stats(stats: Dict[str, Any]) -> None:
        """Display overall state statistics."""
        click.echo("📊 LakehousePlumber State Information")
        click.echo("=" * 60)

        click.echo(f"Total environments: {stats['total_environments']}")

        for env_name, env_stats in stats["environments"].items():
            click.echo(f"\n🌍 Environment: {env_name}")
            click.echo(f"   Total files: {env_stats['total_files']}")
            click.echo(f"   Pipelines: {len(env_stats['pipelines'])}")
            click.echo(f"   FlowGroups: {len(env_stats['flowgroups'])}")

            if env_stats["pipelines"]:
                click.echo("   Pipeline breakdown:")
                for pipeline_name, file_count in env_stats["pipelines"].items():
                    click.echo(f"     • {pipeline_name}: {file_count} files")

        click.echo("\n💡 Use --env <environment> to see detailed file information")
    
    @staticmethod
    def display_no_tracked_files_message() -> None:
        """Display message when no tracked files are found."""
        click.echo("📭 No tracked files found")
        click.echo("\n💡 Generate code to start tracking files")
    
    @staticmethod
    def display_environment_header(env: str) -> None:
        """Display environment-specific header."""
        click.echo(f"📊 State for Environment: {env}")
        click.echo("=" * 60)
    
    @staticmethod
    def display_missing_tracked_files(env: str, pipeline: Optional[str] = None) -> None:
        """Display message when no tracked files are found for environment/pipeline."""
        if pipeline:
            click.echo(
                f"📭 No tracked files found for pipeline '{pipeline}' in environment '{env}'"
            )
        else:
            click.echo("📭 No tracked files found for this environment")
    
    @staticmethod
    def display_orphaned_files(orphaned_files: List[Any], cleanup: bool, dry_run: bool) -> None:
        """Display orphaned files information."""
        if not orphaned_files:
            click.echo("✅ No orphaned files found")
            return

        click.echo(f"🗑️  Orphaned Files ({len(orphaned_files)} found)")
        click.echo("─" * 60)

        for file_state in orphaned_files:
            click.echo(f"• {file_state.generated_path}")
            click.echo(f"  Source: {file_state.source_yaml} (missing)")
            click.echo(f"  Pipeline: {file_state.pipeline}")
            click.echo(f"  FlowGroup: {file_state.flowgroup}")
            click.echo(f"  Generated: {file_state.timestamp}")
            click.echo()

        if cleanup:
            if dry_run:
                click.echo(
                    "📋 Would delete these orphaned files (use without --dry-run to actually delete)"
                )
            else:
                click.echo("🗑️  Cleaning up orphaned files...")
        else:
            click.echo("💡 Use --cleanup flag to remove these orphaned files")
    
    @staticmethod
    def display_cleanup_results(deleted_files: List[str]) -> None:
        """Display cleanup operation results."""
        click.echo(f"✅ Deleted {len(deleted_files)} orphaned files")
    
    @staticmethod
    def display_stale_files(stale_files: List[Any], staleness_info: Dict[str, Any], 
                           regen: bool, dry_run: bool) -> None:
        """Display stale files information."""
        if not stale_files:
            click.echo("✅ No stale files found")
            return

        click.echo(f"📝 Stale Files ({len(stale_files)} found)")
        click.echo("─" * 60)

        # Show global changes if any
        if staleness_info["global_changes"]:
            click.echo("🌍 Global dependency changes:")
            for change in staleness_info["global_changes"]:
                click.echo(f"   • {change}")
            click.echo()

        for file_state in stale_files:
            click.echo(f"• {file_state.generated_path}")
            click.echo(f"  Source: {file_state.source_yaml}")
            click.echo(f"  Pipeline: {file_state.pipeline}")
            click.echo(f"  FlowGroup: {file_state.flowgroup}")
            click.echo(f"  Last generated: {file_state.timestamp}")
            
            # Show detailed dependency changes
            if file_state.generated_path in staleness_info["files"]:
                file_info = staleness_info["files"][file_state.generated_path]
                click.echo(f"  Changes detected:")
                for detail in file_info["details"]:
                    click.echo(f"    - {detail}")
            
            click.echo()

        if regen:
            if dry_run:
                click.echo(
                    "📋 Would regenerate these stale files (use without --dry-run to actually regenerate)"
                )
            else:
                click.echo("🔄 Regenerating stale files...")
        else:
            click.echo("💡 Use --regen flag to regenerate these stale files")
    
    @staticmethod
    def display_regeneration_progress(pipeline_name: str, file_count: int) -> None:
        """Display regeneration progress for a pipeline."""
        click.echo(f"   ✅ Regenerated {file_count} file(s) for {pipeline_name}")
    
    @staticmethod
    def display_regeneration_error(pipeline_name: str, env: str, log_file: Optional[str] = None) -> None:
        """Display regeneration error for a pipeline."""
        if log_file:
            click.echo(f"   📝 Check detailed logs: {log_file}")
        
        click.echo(f"   ❌ Failed to regenerate {pipeline_name}")
        click.echo(f"   💡 Try running 'lhp validate --env {env} --pipeline {pipeline_name}' first")
    
    @staticmethod
    def display_regeneration_results(regenerated_count: int) -> None:
        """Display final regeneration results."""
        click.echo(f"✅ Regenerated {regenerated_count} stale files")
    
    @staticmethod
    def display_new_files(new_files: List[Path], project_root: Path, env: str, 
                         by_pipeline: Dict[str, List[Path]]) -> None:
        """Display new YAML files information."""
        if not new_files:
            click.echo("✅ No new YAML files found")
            return

        click.echo(f"🆕 New YAML Files ({len(new_files)} found)")
        click.echo("─" * 60)

        for pipeline_name, files in sorted(by_pipeline.items()):
            click.echo(f"\n🔧 Pipeline: {pipeline_name} ({len(files)} new files)")
            for yaml_file in sorted(files):
                try:
                    relative_path = yaml_file.relative_to(project_root)
                    click.echo(f"  • {relative_path}")
                except ValueError:
                    click.echo(f"  • {yaml_file}")

        click.echo(
            f"\n💡 Use 'lhp generate --env {env}' to generate code for these files"
        )
    
    @staticmethod
    def display_tracked_files(tracked_files: Dict[str, Any], project_root: Path, 
                             file_status_calculator) -> None:
        """Display all tracked files with their status."""
        click.echo(f"📁 Tracked Files ({len(tracked_files)} total)")
        click.echo("─" * 60)

        # Group by pipeline
        by_pipeline = defaultdict(list)
        for file_state in tracked_files.values():
            by_pipeline[file_state.pipeline].append(file_state)

        for pipeline_name, files in sorted(by_pipeline.items()):
            click.echo(f"\n🔧 Pipeline: {pipeline_name} ({len(files)} files)")

            for file_state in sorted(files, key=lambda f: f.flowgroup):
                source_exists, generated_exists, change_status = file_status_calculator(file_state)

                source_status = "✅" if source_exists else "❌"
                generated_status = "✅" if generated_exists else "❌"

                click.echo(f"  • {file_state.generated_path} {generated_status}")
                click.echo(
                    f"    Source: {file_state.source_yaml} {source_status}{change_status}"
                )
                click.echo(f"    FlowGroup: {file_state.flowgroup}")
                click.echo(f"    Generated: {file_state.timestamp}")
    
    @staticmethod
    def display_new_files_in_summary(new_files: List[Path], project_root: Path, env: str,
                                   by_pipeline: Dict[str, List[Path]]) -> None:
        """Display new files section in comprehensive summary."""
        new_count = len(new_files)
        
        if new_count > 0:
            click.echo(f"\n📄 New YAML Files ({new_count} found)")
            click.echo("─" * 60)

            for pipeline_name, files in sorted(by_pipeline.items()):
                click.echo(f"\n🔧 Pipeline: {pipeline_name} ({len(files)} new files)")
                for yaml_file in sorted(files):
                    try:
                        relative_path = yaml_file.relative_to(project_root)
                        click.echo(f"  • {relative_path} 🆕")
                    except ValueError:
                        click.echo(f"  • {yaml_file} 🆕")

            click.echo(
                f"\n💡 Use 'lhp generate --env {env}' to generate code for these files"
            )
    
    @staticmethod
    def display_comprehensive_summary(counts: Dict[str, int], env: str) -> None:
        """Display comprehensive summary with counts and tips."""
        click.echo("\n📊 Summary:")
        click.echo(f"   🟢 {counts['up_to_date_count']} files up-to-date")

        if counts['new_count'] > 0:
            click.echo(f"   🆕 {counts['new_count']} new YAML files (not generated yet)")
            click.echo(f"      Use 'lhp generate --env {env}' to generate them")

        if counts['stale_count'] > 0:
            click.echo(f"   🟡 {counts['stale_count']} files stale (YAML changed)")
            click.echo("      Use --stale flag to see details")
            click.echo("      Use --stale --regen to regenerate them")

        if counts['orphaned_count'] > 0:
            click.echo(f"   🔴 {counts['orphaned_count']} files orphaned (YAML deleted)")
            click.echo("      Use --orphaned flag to see details")
            click.echo("      Use --orphaned --cleanup to remove them")

        if (counts['orphaned_count'] == 0 and counts['stale_count'] == 0 and 
            counts['new_count'] == 0):
            click.echo("   ✨ Everything is in perfect sync!")

        StateDisplayUtils.display_smart_tips(env)
    
    @staticmethod
    def display_smart_tips(env: str) -> None:
        """Display smart generation tips."""
        click.echo("\n💡 Smart generation tips:")
        click.echo(
            f"   • lhp generate --env {env}    # Only process changed files (default)"
        )
        click.echo(
            f"   • lhp generate --env {env} --force  # Force regenerate all"
        ) 
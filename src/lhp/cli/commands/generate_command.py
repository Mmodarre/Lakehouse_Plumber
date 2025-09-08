"""Generate command implementation for LakehousePlumber CLI."""

import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional
import click
import yaml

from .base_command import BaseCommand
from ...core.orchestrator import ActionOrchestrator
from ...core.state_manager import StateManager
from ...utils.bundle_detection import should_enable_bundle_support
from ...bundle.manager import BundleManager
from ...bundle.exceptions import BundleResourceError

logger = logging.getLogger(__name__)


class GenerateCommand(BaseCommand):
    """
    Handles pipeline code generation command.
    
    Manages the complete workflow of generating DLT pipeline code from YAML
    configurations, including state management, cleanup, and bundle integration.
    """
    
    def execute(self, env: str, pipeline: Optional[str] = None, output: Optional[str] = None,
                dry_run: bool = False, no_cleanup: bool = False, force: bool = False, 
                no_bundle: bool = False, include_tests: bool = False) -> None:
        """
        Execute the generate command.
        
        Args:
            env: Environment to generate for
            pipeline: Specific pipeline to generate (optional)
            output: Output directory (defaults to generated/{env})
            dry_run: Preview without generating files
            no_cleanup: Disable cleanup of generated files
            force: Force regeneration of all files
            no_bundle: Disable bundle support
            include_tests: Include test actions in generation
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        # Set default output based on environment if not provided
        if output is None:
            output = f"generated/{env}"
        
        click.echo(f"🚀 Generating pipeline code for environment: {env}")
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")
        
        # Validate environment setup
        self._validate_environment_setup(env, project_root)
        
        # Initialize orchestrator and state manager
        if self.verbose:
            click.echo("🔧 Initializing orchestrator and state manager...")
        
        orchestrator = ActionOrchestrator(project_root)
        state_manager = StateManager(project_root) if not no_cleanup else None
        
        # Determine pipelines to generate
        pipelines_to_generate = self._determine_pipelines_to_generate(
            pipeline, orchestrator
        )
        
        # Set output directory
        output_dir = project_root / output
        
        # Handle cleanup operations
        if not no_cleanup and state_manager:
            self._handle_cleanup_operations(env, state_manager, output_dir, dry_run)
        
        # Smart generation analysis
        pipelines_needing_generation = self._analyze_generation_needs(
            pipelines_to_generate, env, state_manager, no_cleanup, force
        )
        
        # Generate pipelines
        total_files, all_generated_files = self._generate_pipelines(
            pipelines_needing_generation, env, output_dir, dry_run, 
            orchestrator, state_manager, force, no_cleanup, include_tests
        )
        
        # Save state before bundle operations (as in original)
        if not no_cleanup and state_manager:
            state_manager.save()
        
        # Handle bundle operations (as in original - check support regardless of dry-run)
        self._handle_bundle_operations(
            project_root, output_dir, env, no_bundle, all_generated_files, dry_run
        )
        
        # Display completion message
        self._display_completion_message(
            total_files, output_dir, project_root, dry_run, no_cleanup, state_manager
        )
    
    def _validate_environment_setup(self, env: str, project_root: Path) -> None:
        """Validate environment configuration and dependencies."""
        # Check substitution file exists
        self.check_substitution_file(env)
        
        # Validate environment consistency with databricks.yml
        databricks_yml = project_root / "databricks.yml"
        if databricks_yml.exists():
            try:
                with open(databricks_yml, 'r') as f:
                    bundle_config = yaml.safe_load(f)
                
                if bundle_config and "targets" in bundle_config:
                    targets = bundle_config.get("targets", {})
                    if env not in targets:
                        click.echo(
                            f"⚠️  Warning: Environment '{env}' not found in databricks.yml targets.\n"
                            f"   Add a target named '{env}' to databricks.yml for bundle deployment.\n"
                            f"   LHP will generate resources to: resources/lhp/{env}/"
                        )
            except Exception as e:
                if self.verbose:
                    click.echo(f"⚠️  Could not validate databricks.yml: {e}")
    
    def _determine_pipelines_to_generate(self, pipeline: Optional[str], 
                                        orchestrator: ActionOrchestrator) -> List[str]:
        """Determine which pipelines to generate based on user input and discovery."""
        if pipeline:
            # Check if specific pipeline exists
            all_flowgroups = orchestrator.discover_all_flowgroups()
            pipeline_fields = {fg.pipeline for fg in all_flowgroups}
            
            if pipeline not in pipeline_fields:
                click.echo(f"❌ Pipeline field '{pipeline}' not found in any flowgroup")
                if pipeline_fields:
                    click.echo(f"💡 Available pipeline fields: {sorted(pipeline_fields)}")
                sys.exit(1)
            return [pipeline]
        else:
            # Discover all pipeline fields from flowgroups
            all_flowgroups = orchestrator.discover_all_flowgroups()
            if not all_flowgroups:
                click.echo("❌ No flowgroups found in project")
                sys.exit(1)
            
            pipeline_fields = {fg.pipeline for fg in all_flowgroups}
            return sorted(pipeline_fields)
    
    def _handle_cleanup_operations(self, env: str, state_manager: StateManager,
                                  output_dir: Path, dry_run: bool) -> None:
        """Handle cleanup of orphaned files."""
        click.echo(f"🧹 Checking for orphaned files in environment: {env}")
        
        # Handle fresh start scenario (no state file exists)
        if not state_manager.state_file_exists():
            click.echo("🆕 Fresh start detected: no state file exists")
            if not dry_run:
                deleted_files = state_manager.cleanup_untracked_files(output_dir, env)
                if deleted_files:
                    click.echo(f"🧹 Fresh start cleanup: removed {len(deleted_files)} orphaned file(s):")
                    for deleted_file in deleted_files:
                        click.echo(f"   • Deleted: {deleted_file}")
                else:
                    click.echo("✨ Fresh start cleanup: no orphaned files found")
            else:
                # Dry-run: show what would be cleaned up
                self._show_fresh_start_preview(state_manager, output_dir, env)
        
        # Find orphaned files (tracked files)
        orphaned_files = state_manager.find_orphaned_files(env)
        
        if orphaned_files:
            if dry_run:
                click.echo(f"📋 Would clean up {len(orphaned_files)} orphaned file(s):")
                for file_state in orphaned_files:
                    click.echo(f"   • {file_state.generated_path} (from {file_state.source_yaml})")
            else:
                click.echo(f"🗑️  Cleaning up {len(orphaned_files)} orphaned file(s):")
                deleted_files = state_manager.cleanup_orphaned_files(env, dry_run=False)
                for deleted_file in deleted_files:
                    click.echo(f"   • Deleted: {deleted_file}")
        else:
            click.echo("✅ No orphaned files found")
    
    def _show_fresh_start_preview(self, state_manager: StateManager,
                                 output_dir: Path, env: str) -> None:
        """Show fresh start cleanup preview for dry-run mode."""
        existing_files = state_manager.scan_generated_directory(output_dir)
        expected_files = state_manager.calculate_expected_files(output_dir, env)
        orphaned_files_fs = existing_files - expected_files
        lhp_orphaned = [f for f in orphaned_files_fs if state_manager.is_lhp_generated_file(f)]
        
        if lhp_orphaned:
            click.echo(f"📋 Fresh start cleanup would remove {len(lhp_orphaned)} orphaned file(s):")
            for file_path in sorted(lhp_orphaned):
                try:
                    rel_path = file_path.relative_to(output_dir.parent.parent)  # project_root equivalent
                    click.echo(f"   • {rel_path}")
                except ValueError:
                    click.echo(f"   • {file_path}")
        else:
            click.echo("📋 Fresh start cleanup: no orphaned files would be removed")
    
    def _analyze_generation_needs(self, pipelines_to_generate: List[str], env: str,
                                 state_manager: Optional[StateManager], no_cleanup: bool,
                                 force: bool) -> Dict[str, Dict]:
        """Analyze which pipelines need generation based on staleness detection."""
        pipelines_needing_generation = {}
        
        if not no_cleanup and state_manager and not force:
            click.echo(f"🔍 Analyzing changes in environment: {env}")
            
            # Get detailed staleness information
            staleness_info = state_manager.get_detailed_staleness_info(env)
            
            # Show global dependency changes if any
            if staleness_info["global_changes"]:
                click.echo("🌍 Global dependency changes detected:")
                for change in staleness_info["global_changes"]:
                    click.echo(f"   • {change}")
                click.echo("   → All files will be regenerated")
            
            for pipeline_name in pipelines_to_generate:
                generation_info = state_manager.get_files_needing_generation(env, pipeline_name)
                
                new_count = len(generation_info["new"])
                stale_count = len(generation_info["stale"])
                up_to_date_count = len(generation_info["up_to_date"])
                
                if new_count > 0 or stale_count > 0:
                    pipelines_needing_generation[pipeline_name] = generation_info
                    status_parts = []
                    if new_count > 0:
                        status_parts.append(f"{new_count} new")
                    if stale_count > 0:
                        status_parts.append(f"{stale_count} stale")
                    click.echo(f"   📁 {pipeline_name}: {', '.join(status_parts)} file(s)")
                    
                    # Show detailed dependency changes for verbose mode
                    if self.verbose and stale_count > 0:
                        for file_state in generation_info["stale"]:
                            file_path = file_state.generated_path
                            if file_path in staleness_info["files"]:
                                file_info = staleness_info["files"][file_path]
                                click.echo(f"      • {file_path}:")
                                for detail in file_info["details"]:
                                    click.echo(f"        - {detail}")
                else:
                    click.echo(f"   ✅ {pipeline_name}: {up_to_date_count} file(s) up-to-date")
            
            if not pipelines_needing_generation:
                click.echo("✨ All files are up-to-date! Nothing to generate.")
                click.echo("💡 Use --force flag to regenerate all files anyway.")
                return {}
            else:
                original_count = len(pipelines_to_generate)
                skipped_count = original_count - len(pipelines_needing_generation)
                
                if skipped_count > 0:
                    click.echo(
                        f"⚡ Smart generation: processing {len(pipelines_needing_generation)}"
                        f"/{original_count} pipelines"
                    )
        elif force:
            click.echo("🔄 Force mode: regenerating all files regardless of changes")
            # Include all pipelines for force mode
            for pipeline_name in pipelines_to_generate:
                pipelines_needing_generation[pipeline_name] = {"reason": "force"}
        else:
            click.echo("📝 State tracking disabled: generating all files")
            # Include all pipelines when state tracking disabled
            for pipeline_name in pipelines_to_generate:
                pipelines_needing_generation[pipeline_name] = {"reason": "no_state_tracking"}
        
        return pipelines_needing_generation
    
    def _generate_pipelines(self, pipelines_needing_generation: Dict[str, Dict], env: str,
                           output_dir: Path, dry_run: bool, orchestrator: ActionOrchestrator,
                           state_manager: Optional[StateManager], force: bool, 
                           no_cleanup: bool, include_tests: bool) -> tuple[int, Dict]:
        """Generate code for all pipelines that need generation."""
        total_files = 0
        all_generated_files = {}
        
        # Use pipeline names from the dictionary (handles both smart generation and force/no_cleanup modes)
        pipelines_to_process = list(pipelines_needing_generation.keys())
        
        for pipeline_name in pipelines_to_process:
            click.echo(f"\n🔧 Processing pipeline: {pipeline_name}")
            click.echo("   FlowGroups:")
            
            try:
                # Generate pipeline by field
                pipeline_output_dir = output_dir if not dry_run else None
                generated_files = orchestrator.generate_pipeline_by_field(
                    pipeline_name,
                    env,
                    pipeline_output_dir,
                    state_manager=state_manager,
                    force_all=force or no_cleanup,
                    include_tests=include_tests,
                )
                
                # Track files
                all_generated_files[pipeline_name] = generated_files
                total_files += len(generated_files)
                
                if dry_run:
                    self._show_dry_run_preview(generated_files)
                else:
                    self._show_generation_results(generated_files, output_dir, pipeline_name, 
                                                output_dir.parent)  # project_root
                
            except ValueError as e:
                if "No flowgroups found in pipeline" in str(e):
                    self._handle_missing_pipeline(pipeline_name, env, state_manager, dry_run,
                                                 output_dir.parent)  # project_root
                else:
                    self.handle_error(e, f"Error generating pipeline {pipeline_name}")
            except Exception as e:
                self.handle_error(e, f"Unexpected error generating pipeline {pipeline_name}")
        
        return total_files, all_generated_files
    
    def _show_dry_run_preview(self, generated_files: Dict[str, str]) -> None:
        """Show preview of what would be generated in dry-run mode."""
        click.echo(f"📄 Would generate {len(generated_files)} file(s):")
        for filename in sorted(generated_files.keys()):
            click.echo(f"   • {filename}")
        
        # Show preview of first file if verbose
        if generated_files and logger.isEnabledFor(logging.DEBUG):
            first_file = next(iter(generated_files.values()))
            click.echo("\n📄 Preview of generated code:")
            click.echo("─" * 60)
            # Show first 50 lines
            lines = first_file.split("\n")[:50]
            for line in lines:
                click.echo(line)
            if len(first_file.split("\n")) > 50:
                click.echo("... (truncated)")
            click.echo("─" * 60)
    
    def _show_generation_results(self, generated_files: Dict[str, str], output_dir: Path,
                                pipeline_name: str, project_root: Path) -> None:
        """Show results of successful generation."""
        click.echo(f"✅ Generated {len(generated_files)} file(s) in {output_dir / pipeline_name}")
        for filename in sorted(generated_files.keys()):
            file_path = output_dir / pipeline_name / filename
            click.echo(f"   • {file_path.relative_to(project_root)}")
    
    def _handle_missing_pipeline(self, pipeline_name: str, env: str,
                                state_manager: Optional[StateManager], dry_run: bool,
                                project_root: Path) -> None:
        """Handle case where no flowgroups found in pipeline."""
        click.echo(f"📭 No flowgroups found in pipeline: {pipeline_name}")
        
        # Still run cleanup if enabled
        if state_manager:
            click.echo(f"🧹 Checking for orphaned files from pipeline: {pipeline_name}")
            
            # Find orphaned files for this specific pipeline
            all_orphaned = state_manager.find_orphaned_files(env)
            pipeline_orphaned = [f for f in all_orphaned if f.pipeline == pipeline_name]
            
            if pipeline_orphaned:
                click.echo(f"🗑️  Found {len(pipeline_orphaned)} orphaned file(s) from {pipeline_name}")
                if not dry_run:
                    # Clean up orphaned files for this pipeline
                    for file_state in pipeline_orphaned:
                        generated_path = project_root / file_state.generated_path
                        if generated_path.exists():
                            generated_path.unlink()
                            click.echo(f"   • Deleted: {file_state.generated_path}")
                        
                        # Remove from state
                        if env in state_manager.state.environments:
                            if file_state.generated_path in state_manager.state.environments[env]:
                                del state_manager.state.environments[env][file_state.generated_path]
                    
                    # Clean up empty directories
                    state_manager.cleanup_empty_directories(env)
            else:
                click.echo(f"✅ No orphaned files found from pipeline: {pipeline_name}")
    
    def _handle_bundle_operations(self, project_root: Path, output_dir: Path, env: str,
                                 no_bundle: bool, all_generated_files: Dict, dry_run: bool = False) -> None:
        """Handle Databricks Asset Bundle operations after generation."""
        try:
            # Check if bundle support should be enabled (match original - regardless of dry_run)
            bundle_enabled = should_enable_bundle_support(project_root, no_bundle)
            if bundle_enabled:
                if self.verbose:
                    click.echo("🔗 Bundle support detected - syncing resource files...")
                
                # Only actually sync if not dry-run
                if not dry_run:
                    bundle_manager = BundleManager(project_root)
                    bundle_manager.sync_resources_with_generated_files(output_dir, env)
                    
                    if self.verbose:
                        click.echo("✅ Bundle resource files synchronized")
                else:
                    # In dry-run mode, just show what would happen
                    if self.verbose:
                        click.echo("📋 Dry run: Bundle sync would be performed")
            
        except BundleResourceError as e:
            click.echo(f"⚠️  Bundle sync warning: {e}")
            if self.verbose and self.log_file:
                click.echo(f"📝 Bundle details in logs: {self.log_file}")
        except Exception as e:
            click.echo(f"⚠️  Bundle sync failed: {e}")
            if self.verbose and self.log_file:
                click.echo(f"📝 Bundle error details in logs: {self.log_file}")
    
    def _display_completion_message(self, total_files: int, output_dir: Path,
                                   project_root: Path, dry_run: bool, no_cleanup: bool,
                                   state_manager: Optional[StateManager]) -> None:
        """Display completion message with summary and next steps."""
        click.echo(f"   Total files generated: {total_files}")
        
        if not dry_run:
            if total_files > 0:
                click.echo(f"   Output location: {output_dir.relative_to(project_root)}")
            
            # Show cleanup information if enabled
            if not no_cleanup and state_manager:
                click.echo("   State tracking: Enabled (.lhp_state.json)")
            
            if total_files > 0:
                click.echo("\n✅ Code generation completed successfully")
                click.echo("\n🚀 Next steps:")
                click.echo("   1. Review the generated code")
                click.echo("   2. Copy to your Databricks workspace")  
                click.echo("   3. Create a DLT pipeline with the generated notebooks")
            else:
                click.echo("\n✅ Pipeline processing completed")
                if not no_cleanup:
                    click.echo("   • Cleanup operations were performed")
                click.echo("   • No files were generated (no flowgroups found)")
        else:
            click.echo("\n✨ Dry run completed - no files were written")
            click.echo("   Remove --dry-run flag to generate files")

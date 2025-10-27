"""Clean Architecture generate command implementation."""

import sys
import logging
import time
import hashlib
import platform
import uuid
import subprocess
from pathlib import Path
from typing import Dict, List, Optional, Set
import click

from .base_command import BaseCommand
from ...core.orchestrator import ActionOrchestrator
from ...core.state_manager import StateManager
from ...core.layers import (
    PipelineGenerationRequest, LakehousePlumberApplicationFacade,
    GenerationResponse, ValidationResponse, AnalysisResponse, PresentationLayer
)
from ...utils.bundle_detection import should_enable_bundle_support
from ...bundle.manager import BundleManager
from ...bundle.exceptions import BundleResourceError

logger = logging.getLogger(__name__)


class GenerateCommand(BaseCommand):
    """
    Pipeline code generation command.
    
    This command follows Clean Architecture principles:
    - Pure presentation layer (no business logic)
    - Uses DTOs for layer communication  
    - Delegates all business logic to application facade
    - Focused only on user interaction and display
    """
    
    def execute(self, env: str, pipeline: Optional[str] = None, output: Optional[str] = None,
                dry_run: bool = False, no_cleanup: bool = False, force: bool = False, 
                no_bundle: bool = False, include_tests: bool = False, pipeline_config: Optional[str] = None) -> None:
        """
        Execute the generate command using clean architecture.
        
        This method is purely coordinative - it creates request DTOs,
        delegates to application layer, and displays results.
        
        Args:
            env: Environment to generate for
            pipeline: Specific pipeline to generate (optional)
            output: Output directory (defaults to generated/{env})
            dry_run: Preview without generating files
            no_cleanup: Disable cleanup of generated files
            force: Force regeneration of all files
            no_bundle: Disable bundle support
            include_tests: Include test actions in generation
            pipeline_config: Custom pipeline config file path (relative to project root)
        """
        # ========================================================================
        # PRESENTATION LAYER RESPONSIBILITIES ONLY
        # ========================================================================
        
        # Start timing for analytics
        generation_start_time = time.time()
        
        # 1. Setup and validation (presentation concerns)
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        if output is None:
            output = f"generated/{env}"
        output_dir = project_root / output
        
        # 2. User feedback (presentation)
        self._display_startup_message(env)
        
        # 3. Validate environment setup (critical validation)
        self.check_substitution_file(env)
        
        # 4. Initialize application layer facade
        application_facade = self._create_application_facade(project_root, no_cleanup, pipeline_config)
        
        # 5. Discover pipelines to generate (coordinate with application layer)
        pipelines_to_generate = self._discover_pipelines_for_generation(
            pipeline, application_facade
        )
        
        if not pipelines_to_generate:
            click.echo("❌ No flowgroups found in project")
            sys.exit(1)
        
        # 6. Handle cleanup operations (coordinate)
        if not no_cleanup:
            self._handle_cleanup_operations(application_facade, env, output_dir, dry_run)
        
        # 7. Analyze generation requirements (show context changes)
        self._display_generation_analysis(application_facade, pipelines_to_generate, env, include_tests, force, no_cleanup)
        
        # 8. Execute generation for each pipeline (pure coordination)
        total_files = 0
        all_generated_files = {}
        
        # Initialize metrics collection for analytics
        total_flowgroups = 0
        unique_templates: Set[str] = set()
        flowgroups_with_templates = 0
        
        for pipeline_identifier in pipelines_to_generate:
            # Collect metrics for this pipeline
            pipeline_flowgroups = application_facade.orchestrator.discover_flowgroups_by_pipeline_field(pipeline_identifier)
            total_flowgroups += len(pipeline_flowgroups)
            
            # Track template usage
            for fg in pipeline_flowgroups:
                if hasattr(fg, 'use_template') and fg.use_template:
                    unique_templates.add(fg.use_template)
                    flowgroups_with_templates += 1
            
            response = self._execute_pipeline_generation(
                application_facade, pipeline_identifier, env, output_dir,
                dry_run, force, include_tests, no_cleanup, pipeline_config
            )
            
            # Display results (presentation)
            self._display_generation_response(response, pipeline_identifier)
            
            if response.is_successful():
                total_files += response.files_written
                all_generated_files.update(response.generated_files)
        
        # 9. Handle bundle operations (coordinate)
        if not no_bundle:
            self._handle_bundle_operations(
                project_root, output_dir, env, no_bundle, dry_run, pipeline_config
            )
        
        # 10. Display completion message (presentation)
        self._display_completion_message(total_files, output_dir, dry_run)
        
        # 11. Track generation metrics (only on successful completion)
        if self._should_track_analytics(project_root):
            try:
                import klyne
                from ...utils.version import get_version
                
                # Collect identifiers and metrics
                project_id = self._get_project_identifier(application_facade)
                machine_id = self._get_machine_identifier()
                is_ci = self._is_ci_environment()
                generation_end_time = time.time()
                lhp_version = get_version()
                
                # Track metrics
                klyne.track('lhp_generate', {
                    'project_id': project_id,
                    'machine_id': machine_id,
                    'is_ci': is_ci,
                    'flowgroups_count': total_flowgroups,
                    'templates_count': len(unique_templates),
                    'flowgroups_using_templates': flowgroups_with_templates,
                    'pipelines_count': len(pipelines_to_generate),
                    'files_generated': total_files,
                    'environment': env,
                    'dry_run': dry_run,
                    'bundle_enabled': not no_bundle,
                    'lhp_version': lhp_version,
                    'python_version': f"{sys.version_info.major}.{sys.version_info.minor}",
                    'generation_time_seconds': round(generation_end_time - generation_start_time, 2)
                })
            except Exception as e:
                self.logger.debug(f"Analytics tracking failed: {e}")
    
    def _create_application_facade(self, project_root: Path, 
                                 no_cleanup: bool, pipeline_config_path: Optional[str] = None) -> LakehousePlumberApplicationFacade:
        """Create application facade for business layer access."""
        orchestrator = ActionOrchestrator(project_root, pipeline_config_path=pipeline_config_path)
        state_manager = StateManager(project_root) if not no_cleanup else None
        return LakehousePlumberApplicationFacade(orchestrator, state_manager)
    
    def _execute_pipeline_generation(self, application_facade: LakehousePlumberApplicationFacade,
                                   pipeline_identifier: str, env: str, output_dir: Path,
                                   dry_run: bool, force: bool, include_tests: bool, 
                                   no_cleanup: bool, pipeline_config_path: Optional[str] = None) -> GenerationResponse:
        """Execute generation for single pipeline using application facade."""
        # Create request DTO
        request = PipelineGenerationRequest(
            pipeline_identifier=pipeline_identifier,
            environment=env,
            include_tests=include_tests,
            force_all=force,
            specific_flowgroups=None,
            output_directory=output_dir,
            dry_run=dry_run,
            no_cleanup=no_cleanup,
            pipeline_config_path=pipeline_config_path
        )
        
        # Delegate to application layer
        return application_facade.generate_pipeline(request)
    
    # ========================================================================
    # PURE PRESENTATION METHODS
    # ========================================================================
    
    def display_generation_results(self, response: GenerationResponse) -> None:
        """Display generation results - pure presentation logic."""
        if response.is_successful():
            if response.files_written > 0:
                click.echo(f"✅ Generated {response.files_written} file(s)")
                click.echo(f"📂 Output location: {response.output_location}")
            else:
                if response.performance_info.get("dry_run"):
                    click.echo("✨ Dry run completed - no files were written")
                else:
                    click.echo("✨ All files are up-to-date! Nothing to generate.")
        else:
            click.echo(f"❌ Generation failed: {response.error_message}")
    
    def display_validation_results(self, response: ValidationResponse) -> None:
        """Display validation results - pure presentation logic."""
        if response.success:
            click.echo("✅ Validation successful")
            if response.has_warnings():
                click.echo("⚠️ Warnings found:")
                for warning in response.warnings:
                    click.echo(f"   • {warning}")
        else:
            click.echo("❌ Validation failed")
            for error in response.errors:
                click.echo(f"   • {error}")
    
    def display_analysis_results(self, response: AnalysisResponse) -> None:
        """Display analysis results - pure presentation logic."""
        if response.success:
            if response.has_work_to_do():
                click.echo(f"🔧 Analysis: {len(response.pipelines_needing_generation)} pipeline(s) need generation")
                for pipeline, info in response.pipelines_needing_generation.items():
                    click.echo(f"   • {pipeline}: needs generation")
            else:
                click.echo("✅ Analysis: All pipelines are up-to-date")
            
            if response.include_tests_context_applied:
                click.echo("🧪 Generation context changes detected (include_tests parameter)")
        else:
            click.echo(f"❌ Analysis failed: {response.error_message}")
    
    def get_user_input(self, prompt: str) -> str:
        """Get input from user - pure presentation."""
        return input(prompt)
    
    def _display_startup_message(self, env: str) -> None:
        """Display startup message."""
        click.echo(f"🚀 Generating pipeline code for environment: {env}")
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")
    
    def _display_generation_response(self, response: GenerationResponse, pipeline_id: str) -> None:
        """Display single pipeline generation response."""
        if response.is_successful():
            if response.files_written > 0:
                click.echo(f"✅ {pipeline_id}: Generated {response.files_written} file(s)")
            else:
                if response.performance_info.get("dry_run"):
                    click.echo(f"📝 {pipeline_id}: Would generate {response.total_flowgroups} file(s)")
                    # Show specific filenames in dry-run mode
                    for filename in response.generated_files.keys():
                        click.echo(f"   • {filename}")
                else:
                    click.echo(f"✅ {pipeline_id}: Up-to-date")
        else:
            click.echo(f"❌ {pipeline_id}: Generation failed - {response.error_message}")
    
    def _display_completion_message(self, total_files: int, output_dir: Path, dry_run: bool) -> None:
        """Display completion message."""
        if dry_run:
            click.echo("✨ Dry run completed - no files were written")
            click.echo("   Remove --dry-run flag to generate files")
        elif total_files > 0:
            click.echo("✅ Code generation completed successfully")
            click.echo(f"📂 Total files generated: {total_files}")
            click.echo(f"📂 Output location: {output_dir}")
        else:
            click.echo("✨ All files are up-to-date! Nothing to generate.")
    
    def _discover_pipelines_for_generation(self, pipeline: Optional[str], 
                                         application_facade: LakehousePlumberApplicationFacade) -> List[str]:
        """Discover which pipelines to generate - coordinate with application layer."""
        if pipeline:
            # Specific pipeline requested
            return [pipeline]
        else:
            # Discover all available pipelines
            try:
                # Use orchestrator through facade for discovery
                all_flowgroups = application_facade.orchestrator.discover_all_flowgroups()
                pipeline_fields = {fg.pipeline for fg in all_flowgroups}
                
                if not pipeline_fields:
                    click.echo("❌ No flowgroups found in project")
                    return []
                
                return list(pipeline_fields)
                
            except Exception as e:
                click.echo(f"❌ Error discovering pipelines: {e}")
                return []
    
    def _display_generation_analysis(self, application_facade: LakehousePlumberApplicationFacade,
                                   pipelines_to_generate: List[str], env: str, include_tests: bool,
                                   force: bool, no_cleanup: bool) -> None:
        """Display generation analysis including context change detection."""
        # Display force mode message if force flag is used
        if force:
            click.echo("🔄 Force mode: regenerating all files regardless of changes")
        
        # Create analysis request
        from ...core.layers import StalenessAnalysisRequest
        
        analysis_request = StalenessAnalysisRequest(
            environment=env,
            pipeline_names=pipelines_to_generate,
            include_tests=include_tests,
            force=force
        )
        
        # Get analysis from application facade
        analysis_response = application_facade.analyze_staleness(analysis_request)
        
        # Display analysis results (includes context change detection)
        self.display_analysis_results(analysis_response)

    def _handle_cleanup_operations(self, application_facade: LakehousePlumberApplicationFacade,
                                 env: str, output_dir: Path, dry_run: bool) -> None:
        """Handle cleanup operations - coordinate with application layer."""
        # This could be enhanced to use application facade for cleanup coordination
        # For now, direct state manager access
        if application_facade.state_manager:
            click.echo("🧹 Checking for orphaned files in environment: " + env)
            orphaned_files = application_facade.state_manager.find_orphaned_files(env)
            
            if orphaned_files:
                if dry_run:
                    click.echo(f"Would clean up {len(orphaned_files)} orphaned file(s)")
                else:
                    click.echo(f"Cleaning up {len(orphaned_files)} orphaned file(s)")
                    deleted_files = application_facade.state_manager.cleanup_orphaned_files(env, dry_run=False)
                    for deleted_file in deleted_files:
                        click.echo(f"   • Deleted: {deleted_file}")
            else:
                click.echo("✅ No orphaned files found")
    
    def _handle_bundle_operations(self, project_root: Path, output_dir: Path, env: str,
                                no_bundle: bool, dry_run: bool, pipeline_config_path: Optional[str] = None) -> None:
        """Handle bundle operations - coordinate with bundle management."""
        try:
            # Check if bundle support should be enabled
            bundle_enabled = should_enable_bundle_support(project_root, no_bundle)
            if bundle_enabled:
                click.echo("Bundle support detected")
                if self.verbose:
                    click.echo("🔗 Bundle support detected - syncing resource files...")
                
                # Only actually sync if not dry-run
                if not dry_run:
                    bundle_manager = BundleManager(project_root, pipeline_config_path)
                    bundle_manager.sync_resources_with_generated_files(output_dir, env)
                    click.echo("📦 Bundle resource files synchronized")
                    
                    if self.verbose:
                        click.echo("✅ Bundle resource files synchronized")
                else:
                    # In dry-run mode, just show what would happen
                    click.echo("📦 Bundle sync would be performed")
                    if self.verbose:
                        click.echo("Dry run: Bundle sync would be performed")
            
        except BundleResourceError as e:
            click.echo(f"⚠️ Bundle sync warning: {e}")
        except Exception as e:
            click.echo(f"⚠️ Unexpected bundle error: {e}")
    
    # ========================================================================
    # ANALYTICS HELPER METHODS
    # ========================================================================
    
    def _should_track_analytics(self, project_root: Path) -> bool:
        """Check if user has opted out of analytics tracking."""
        import os
        
        # Check for test environment variable (disable in tests)
        if os.environ.get('LHP_DISABLE_ANALYTICS') or os.environ.get('PYTEST_CURRENT_TEST'):
            return False
        
        try:
            opt_out_file = project_root / ".lhp_do_not_track"
            return not opt_out_file.exists()
        except Exception:
            return True
    
    def _get_project_identifier(self, application_facade: LakehousePlumberApplicationFacade) -> str:
        """Get hashed project identifier for analytics."""
        project_name = "unknown_project"
        if application_facade.orchestrator.project_config:
            project_name = getattr(application_facade.orchestrator.project_config, 'name', 'unknown_project')
        return hashlib.sha256(project_name.encode()).hexdigest()
    
    def _get_machine_identifier(self) -> str:
        """Get hashed machine identifier (silently fails if unavailable)."""
        try:
            system = platform.system()
            
            if system == 'Linux':
                for path in ['/etc/machine-id', '/var/lib/dbus/machine-id']:
                    try:
                        with open(path, 'r') as f:
                            machine_id = f.read().strip()
                            if machine_id:
                                return hashlib.sha256(machine_id.encode()).hexdigest()
                    except Exception:
                        continue
            
            elif system == 'Darwin':  # macOS
                try:
                    result = subprocess.run(
                        ['ioreg', '-rd1', '-c', 'IOPlatformExpertDevice'],
                        capture_output=True, text=True, timeout=2
                    )
                    if result.returncode == 0:
                        for line in result.stdout.split('\n'):
                            if 'IOPlatformUUID' in line:
                                parts = line.split('"')
                                if len(parts) > 3:
                                    machine_id = parts[3]
                                    return hashlib.sha256(machine_id.encode()).hexdigest()
                except Exception:
                    pass
            
            elif system == 'Windows':
                try:
                    result = subprocess.run(
                        ['wmic', 'csproduct', 'get', 'UUID'],
                        capture_output=True, text=True, timeout=2
                    )
                    if result.returncode == 0:
                        lines = result.stdout.strip().split('\n')
                        if len(lines) > 1:
                            machine_id = lines[1].strip()
                            return hashlib.sha256(machine_id.encode()).hexdigest()
                except Exception:
                    pass
            
            # Fallback: Use MAC address-based UUID
            machine_id = str(uuid.getnode())
            return hashlib.sha256(machine_id.encode()).hexdigest()
        except Exception:
            return hashlib.sha256(b"unknown_machine").hexdigest()
    
    def _is_ci_environment(self) -> bool:
        """Detect if running in a CI/CD environment."""
        import os
        ci_indicators = [
            'CI', 'GITHUB_ACTIONS', 'GITLAB_CI', 'JENKINS_HOME',
            'TRAVIS', 'CIRCLECI', 'BITBUCKET_BUILD_NUMBER', 'AZURE_PIPELINES'
        ]
        return any(os.environ.get(var) for var in ci_indicators)

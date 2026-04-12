"""Clean Architecture generate command implementation."""

import logging
from pathlib import Path
from typing import Dict, List, Optional

import click

from ...bundle.exceptions import BundleResourceError
from ...bundle.manager import BundleManager
from ...utils.performance_timer import log_perf_summary, perf_timer
from ...core.layers import (
    AnalysisResponse,
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    PipelineGenerationRequest,
    PresentationLayer,
    StalenessAnalysisRequest,
    ValidationResponse,
)
from ...core.orchestrator import ActionOrchestrator
from ...core.state.checksum_cache import ChecksumCache
from ...core.state_manager import StateManager
from ...models.config import FlowGroup
from ...utils.bundle_detection import should_enable_bundle_support
from .base_command import BaseCommand

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

    def execute(
        self,
        env: str,
        pipeline: Optional[str] = None,
        output: Optional[str] = None,
        dry_run: bool = False,
        no_cleanup: bool = False,
        force: bool = False,
        no_bundle: bool = False,
        include_tests: bool = False,
        pipeline_config: Optional[str] = None,
    ) -> None:
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

        # 1. Setup and validation (presentation concerns)
        self.setup_from_context()
        project_root = self.ensure_project_root()

        logger.debug(
            f"Generate request: env={env}, pipeline={pipeline}, force={force}, "
            f"dry_run={dry_run}, include_tests={include_tests}, no_bundle={no_bundle}"
        )

        if output is None:
            output = f"generated/{env}"
        output_dir = project_root / output

        # 2. User feedback (presentation)
        self._display_startup_message(env)

        # 3. Validate environment setup (critical validation)
        self.check_substitution_file(env)

        # 4. Initialize application layer facade
        with perf_timer("Orchestrator init", phase=True):
            application_facade = self._create_application_facade(
                project_root, no_cleanup, pipeline_config
            )

        # 5. Discover all flowgroups ONCE (used by cleanup, analysis, and generation)
        with perf_timer("Pipeline discovery", phase=True):
            all_flowgroups = application_facade.orchestrator.discover_all_flowgroups()
            application_facade.orchestrator.validate_duplicate_pipeline_flowgroup_combinations(
                all_flowgroups
            )
            pipelines_to_generate = self._get_pipeline_names(
                pipeline, all_flowgroups
            )

        if not pipelines_to_generate:
            from ...utils.error_formatter import ErrorCategory, LHPConfigError

            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number="014",
                title="No flowgroups found in project",
                details="No flowgroup YAML files were found in the pipelines/ directory.",
                suggestions=[
                    "Create flowgroup YAML files in pipelines/<pipeline_name>/",
                    "Check that pipeline YAML files have the correct extension (.yaml or .yml)",
                    "Run 'lhp init <name>' to create a new project with example files",
                ],
            )

        logger.debug(f"Pipelines discovered for generation: {pipelines_to_generate}")

        # 6. Create per-run checksum cache (shared across analysis + generation)
        checksum_cache = ChecksumCache()
        if application_facade.state_manager:
            application_facade.state_manager.set_checksum_cache(checksum_cache)

        # 7. Handle cleanup operations (coordinate)
        if force and not no_cleanup:
            with perf_timer("Cleanup operations", phase=True):
                self._wipe_generated_directory(
                    application_facade, output_dir, dry_run
                )
        elif not no_cleanup:
            with perf_timer("Cleanup operations", phase=True):
                active_flowgroups = {
                    (fg.pipeline, fg.flowgroup) for fg in all_flowgroups
                }
                self._handle_cleanup_operations(
                    application_facade, env, output_dir, dry_run,
                    active_flowgroups=active_flowgroups,
                )

        # 8. Analyze generation requirements (show context changes)
        with perf_timer("Staleness analysis", phase=True):
            self._display_generation_analysis(
                application_facade,
                pipelines_to_generate,
                env,
                include_tests,
                force,
                no_cleanup,
                all_flowgroups=all_flowgroups,
            )

        # 9. Execute generation for each pipeline (pure coordination)
        total_files = 0
        all_generated_files = {}

        for pipeline_identifier in pipelines_to_generate:
            logger.debug(f"Starting generation for pipeline: {pipeline_identifier}")
            with perf_timer(
                f"Pipeline generation [{pipeline_identifier}]", phase=True
            ):
                response = self._execute_pipeline_generation(
                    application_facade,
                    pipeline_identifier,
                    env,
                    output_dir,
                    dry_run,
                    force,
                    include_tests,
                    no_cleanup,
                    pipeline_config,
                    all_flowgroups=all_flowgroups,
                )

            # Display results (presentation)
            self._display_generation_response(response, pipeline_identifier)

            if response.is_successful():
                total_files += response.files_written
                all_generated_files.update(response.generated_files)

        # 10. Handle bundle operations (coordinate)
        if not no_bundle:
            with perf_timer("Bundle sync", phase=True):
                self._handle_bundle_operations(
                    project_root,
                    output_dir,
                    env,
                    no_bundle,
                    dry_run,
                    force,
                    pipeline_config,
                    project_config=application_facade.orchestrator.project_config,
                )

        # 11. Log performance summary (before completion message)
        log_perf_summary()

        # 12. Display completion message (presentation)
        logger.info(
            f"Generation complete: {total_files} file(s) generated, "
            f"{len(all_generated_files)} total output file(s)"
        )
        self._display_completion_message(total_files, output_dir, dry_run)

    def _create_application_facade(
        self,
        project_root: Path,
        no_cleanup: bool,
        pipeline_config_path: Optional[str] = None,
    ) -> LakehousePlumberApplicationFacade:
        """Create application facade for business layer access."""
        orchestrator = ActionOrchestrator(
            project_root, pipeline_config_path=pipeline_config_path
        )
        state_manager = (
            StateManager(
                project_root, yaml_parser=orchestrator.cached_yaml_parser
            )
            if not no_cleanup
            else None
        )
        return LakehousePlumberApplicationFacade(orchestrator, state_manager)

    def _execute_pipeline_generation(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        pipeline_identifier: str,
        env: str,
        output_dir: Path,
        dry_run: bool,
        force: bool,
        include_tests: bool,
        no_cleanup: bool,
        pipeline_config_path: Optional[str] = None,
        all_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> GenerationResponse:
        """Execute generation for single pipeline using application facade."""
        logger.debug(
            f"Building generation request for pipeline={pipeline_identifier}, "
            f"output_dir={output_dir}"
        )
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
            pipeline_config_path=pipeline_config_path,
        )

        # Delegate to application layer
        return application_facade.generate_pipeline(
            request, pre_discovered_all_flowgroups=all_flowgroups
        )

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
                click.echo(
                    f"🔧 Analysis: {len(response.pipelines_needing_generation)} pipeline(s) need generation"
                )
                for pipeline, info in response.pipelines_needing_generation.items():
                    click.echo(f"   • {pipeline}: needs generation")
            else:
                click.echo("✅ Analysis: All pipelines are up-to-date")

            if response.include_tests_context_applied:
                click.echo(
                    "🧪 Generation context changes detected (include_tests parameter)"
                )
        else:
            click.echo(f"❌ Analysis failed: {response.error_message}")

    def get_user_input(self, prompt: str) -> str:
        """Get input from user - pure presentation."""
        return input(prompt)

    @staticmethod
    def _get_pipeline_names(
        pipeline: Optional[str], all_flowgroups: List[FlowGroup]
    ) -> List[str]:
        """Extract pipeline names from discovered flowgroups, or return specific pipeline."""
        if pipeline:
            return [pipeline]
        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        return list(pipeline_fields) if pipeline_fields else []

    def _display_startup_message(self, env: str) -> None:
        """Display startup message."""
        click.echo(f"🚀 Generating pipeline code for environment: {env}")
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")

    def _display_generation_response(
        self, response: GenerationResponse, pipeline_id: str
    ) -> None:
        """Display single pipeline generation response."""
        if response.is_successful():
            if response.files_written > 0:
                click.echo(
                    f"✅ {pipeline_id}: Generated {response.files_written} file(s)"
                )
            else:
                if response.performance_info.get("dry_run"):
                    click.echo(
                        f"📝 {pipeline_id}: Would generate {response.total_flowgroups} file(s)"
                    )
                    # Show specific filenames in dry-run mode
                    for filename in response.generated_files.keys():
                        click.echo(f"   • {filename}")
                else:
                    click.echo(f"✅ {pipeline_id}: Up-to-date")
        else:
            click.echo(
                f"❌ {pipeline_id}: Generation failed - {response.error_message}"
            )

    def _display_completion_message(
        self, total_files: int, output_dir: Path, dry_run: bool
    ) -> None:
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

    def _display_generation_analysis(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        pipelines_to_generate: List[str],
        env: str,
        include_tests: bool,
        force: bool,
        no_cleanup: bool,
        all_flowgroups: Optional[List[FlowGroup]] = None,
    ) -> None:
        """Display generation analysis including context change detection."""
        # Display force mode message if force flag is used
        if force:
            click.echo("🔄 Force mode: regenerating all files regardless of changes")

        analysis_request = StalenessAnalysisRequest(
            environment=env,
            pipeline_names=pipelines_to_generate,
            include_tests=include_tests,
            force=force,
        )

        # Get analysis from application facade
        analysis_response = application_facade.analyze_staleness(
            analysis_request,
            pre_discovered_all_flowgroups=all_flowgroups,
        )

        # Display analysis results (includes context change detection)
        self.display_analysis_results(analysis_response)

    def _handle_cleanup_operations(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        env: str,
        output_dir: Path,
        dry_run: bool,
        active_flowgroups: Optional[set] = None,
    ) -> None:
        """Handle cleanup operations - coordinate with application layer."""
        if application_facade.state_manager:
            click.echo("🧹 Checking for orphaned files in environment: " + env)
            orphaned_files = application_facade.state_manager.find_orphaned_files(
                env, active_flowgroups=active_flowgroups
            )

            if orphaned_files:
                if dry_run:
                    click.echo(f"Would clean up {len(orphaned_files)} orphaned file(s)")
                else:
                    click.echo(f"Cleaning up {len(orphaned_files)} orphaned file(s)")
                    deleted_files = (
                        application_facade.state_manager.cleanup_orphaned_files(
                            env, dry_run=False
                        )
                    )
                    for deleted_file in deleted_files:
                        click.echo(f"   • Deleted: {deleted_file}")
            else:
                click.echo("✅ No orphaned files found")

    def _wipe_generated_directory(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        output_dir: Path,
        dry_run: bool,
    ) -> None:
        """Wipe LHP-generated files in output dir (force mode replacement for orphan detection)."""
        if not output_dir.exists():
            click.echo("🧹 No generated directory to clean")
            return

        deleted_count = 0
        cleaner = (
            application_facade.state_manager.cleaner
            if application_facade.state_manager
            else None
        )

        for py_file in output_dir.rglob("*.py"):
            # Safety: only delete files with the LHP header
            is_lhp = False
            if cleaner:
                is_lhp = cleaner.is_lhp_generated_file(py_file)
            else:
                # Fallback inline check
                try:
                    with open(py_file, "r", encoding="utf-8") as f:
                        for i, line in enumerate(f):
                            if i >= 5:
                                break
                            if "Generated by LakehousePlumber" in line:
                                is_lhp = True
                                break
                except (OSError, UnicodeDecodeError):
                    pass

            if is_lhp:
                if dry_run:
                    deleted_count += 1
                else:
                    py_file.unlink()
                    deleted_count += 1
                    logger.debug(f"Wiped generated file: {py_file}")

        # Clean up empty directories (deepest first)
        if not dry_run and deleted_count > 0:
            for dirpath in sorted(
                (d for d in output_dir.rglob("*") if d.is_dir()),
                key=lambda x: len(x.parts),
                reverse=True,
            ):
                try:
                    if not any(dirpath.iterdir()):
                        dirpath.rmdir()
                        logger.debug(f"Removed empty directory: {dirpath}")
                except OSError:
                    pass

        if dry_run:
            click.echo(f"🧹 Force mode: would wipe {deleted_count} generated file(s)")
        elif deleted_count > 0:
            click.echo(f"🧹 Force mode: wiped {deleted_count} generated file(s)")
        else:
            click.echo("🧹 No generated files to clean")

    def _handle_bundle_operations(
        self,
        project_root: Path,
        output_dir: Path,
        env: str,
        no_bundle: bool,
        dry_run: bool,
        force: bool = False,
        pipeline_config_path: Optional[str] = None,
        project_config=None,
    ) -> None:
        """Handle bundle operations - coordinate with bundle management."""
        try:
            # Check if bundle support should be enabled
            bundle_enabled = should_enable_bundle_support(project_root, no_bundle)
            logger.debug(f"Bundle support enabled: {bundle_enabled}")
            if bundle_enabled:
                click.echo("Bundle support detected")

                # Display force regeneration message when both flags are present
                if force and pipeline_config_path is not None:
                    click.echo(
                        "🔄 Force regenerating pipeline YAML files with pipeline config changes"
                    )

                if self.verbose:
                    click.echo("🔗 Bundle support detected - syncing resource files...")

                # Only actually sync if not dry-run
                if not dry_run:
                    bundle_manager = BundleManager(
                        project_root,
                        pipeline_config_path,
                        project_config=project_config,
                    )
                    bundle_manager.sync_resources_with_generated_files(
                        output_dir,
                        env,
                        force=force,
                        has_pipeline_config=(pipeline_config_path is not None),
                    )
                    click.echo("📦 Bundle resource files synchronized")

                    if self.verbose:
                        click.echo("✅ Bundle resource files synchronized")
                else:
                    # In dry-run mode, just show what would happen
                    click.echo("📦 Bundle sync would be performed")
                    if self.verbose:
                        click.echo("Dry run: Bundle sync would be performed")

        except BundleResourceError:
            raise  # Let cli_error_boundary handle BundleResourceError
        except Exception:
            raise  # Let cli_error_boundary handle unexpected errors

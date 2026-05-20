"""Clean Architecture generate command implementation."""

import logging
import shutil
from pathlib import Path
from typing import List, Optional

import click

from ...bundle.exceptions import BundleResourceError
from ...bundle.manager import BundleManager
from ...core.layers import (
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    ValidationResponse,
)
from ...core.orchestrator import ActionOrchestrator
from ...models.config import FlowGroup
from ...utils.bundle_detection import should_enable_bundle_support
from ...utils.performance_timer import log_perf_summary, perf_timer
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
        no_bundle: bool = False,
        include_tests: bool = False,
        pipeline_config: Optional[str] = None,
        *,
        max_workers: Optional[int] = None,
    ) -> None:
        """Execute the generate command using clean architecture.

        Args:
            env: Environment to generate for
            pipeline: Specific pipeline to generate (optional)
            output: Output directory (defaults to generated/{env})
            dry_run: Preview without generating files
            no_bundle: Disable bundle support
            include_tests: Include test actions in generation
            pipeline_config: Custom pipeline config file path (relative to project root)
            max_workers: Override the worker pool size; ``None`` defers to auto-detection.
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        logger.debug(
            f"Generate request: env={env}, pipeline={pipeline}, "
            f"dry_run={dry_run}, include_tests={include_tests}, no_bundle={no_bundle}"
        )

        if output is None:
            output = f"generated/{env}"
        output_dir = project_root / output

        _remove_legacy_state_artifacts(project_root)

        self._display_startup_message(env)
        self.check_substitution_file(env)

        with perf_timer("Orchestrator init", phase=True):
            application_facade = self._create_application_facade(
                project_root,
                pipeline_config,
                max_workers=max_workers,
            )

        # Discover all flowgroups once — the same set is reused for cleanup,
        # analysis, and generation below.
        with perf_timer("Pipeline discovery", phase=True):
            all_flowgroups = application_facade.orchestrator.discover_all_flowgroups()
            application_facade.orchestrator.validate_duplicate_pipeline_flowgroup_combinations(
                all_flowgroups
            )
            pipelines_to_generate = self._get_pipeline_names(pipeline, all_flowgroups)

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

        bundle_enabled = should_enable_bundle_support(project_root, no_bundle)

        # Always wipe the env-specific generated directory: every run is a
        # full regenerate after the V0.8.7 state-tracking removal. The
        # resources/lhp/ wipe is gated on bundle support so non-bundle projects
        # don't materialize the directory just to delete it.
        if not dry_run:
            with perf_timer("Cleanup operations", phase=True):
                self._wipe_generated_directory(output_dir.parent, env)
                if bundle_enabled:
                    _wipe_resources_lhp_directory(project_root)

        # Per-pipeline ✅ / ❌ display happens in real time via the
        # on_pipeline_complete callback (fired on the main thread).
        # Aggregate exit-code-mapping happens at the fail-fast boundary
        # below by re-raising original_error to cli_error_boundary.
        def _display_per_pipeline(
            pipeline_name: str, response: GenerationResponse
        ) -> None:
            self._display_generation_response(response, pipeline_name)

        with perf_timer(
            f"Batch pipeline generation [{len(pipelines_to_generate)}]",
            phase=True,
        ):
            batch_response = application_facade.generate_pipelines(
                pipeline_fields=pipelines_to_generate,
                env=env,
                output_dir=output_dir if not dry_run else None,
                specific_flowgroups=None,
                include_tests=include_tests,
                pre_discovered_all_flowgroups=all_flowgroups,
                max_workers=max_workers,
                on_pipeline_complete=_display_per_pipeline,
            )

        if not batch_response.is_successful():
            # Fail-fast at the boundary: re-raise the underlying aggregate
            # exception so cli_error_boundary formats it and maps the LHP
            # code to a POSIX exit code.
            if batch_response.original_error is not None:
                raise batch_response.original_error
            raise RuntimeError(
                batch_response.error_message or "Batch pipeline generation failed"
            )

        total_files = batch_response.total_files_written
        all_generated_filenames = batch_response.aggregate_generated_filenames

        # Finalize monitoring artifacts after all pipelines have generated.
        if not dry_run:
            with perf_timer("Monitoring artifacts", phase=True):
                application_facade.orchestrator.finalize_monitoring_artifacts(
                    env, output_dir
                )

        if not no_bundle:
            with perf_timer("Bundle sync", phase=True):
                self._handle_bundle_operations(
                    project_root,
                    output_dir,
                    env,
                    no_bundle,
                    dry_run,
                    pipeline_config,
                    project_config=application_facade.orchestrator.project_config,
                )

        log_perf_summary()

        logger.info(
            f"Generation complete: {total_files} file(s) generated, "
            f"{len(all_generated_filenames)} total output file(s)"
        )
        self._display_completion_message(total_files, output_dir, dry_run)

    def _create_application_facade(
        self,
        project_root: Path,
        pipeline_config_path: Optional[str] = None,
        *,
        max_workers: Optional[int] = None,
    ) -> LakehousePlumberApplicationFacade:
        """Create application facade for business layer access."""
        orchestrator = ActionOrchestrator(
            project_root,
            pipeline_config_path=pipeline_config_path,
            max_workers=max_workers,
        )
        return LakehousePlumberApplicationFacade(orchestrator)

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
                    for filename in response.generated_filenames:
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

    def _wipe_generated_directory(self, generated_dir: Path, env: str) -> None:
        env_dir = generated_dir / env
        if env_dir.exists():
            shutil.rmtree(env_dir)
        env_dir.mkdir(parents=True, exist_ok=True)

    def _handle_bundle_operations(
        self,
        project_root: Path,
        output_dir: Path,
        env: str,
        no_bundle: bool,
        dry_run: bool,
        pipeline_config_path: Optional[str] = None,
        project_config=None,
    ) -> None:
        """Handle bundle operations - coordinate with bundle management."""
        bundle_enabled = should_enable_bundle_support(project_root, no_bundle)
        logger.debug(f"Bundle support enabled: {bundle_enabled}")
        if not bundle_enabled:
            return

        click.echo("Bundle support detected")

        if pipeline_config_path is not None:
            click.echo(
                "🔄 Regenerating pipeline YAML files because a pipeline-config override was supplied"
            )

        if self.verbose:
            click.echo("🔗 Bundle support detected - syncing resource files...")

        if dry_run:
            click.echo("📦 Bundle sync would be performed")
            if self.verbose:
                click.echo("Dry run: Bundle sync would be performed")
            return

        bundle_manager = BundleManager(
            project_root,
            pipeline_config_path,
            project_config=project_config,
        )
        bundle_manager.sync_resources_with_generated_files(
            output_dir,
            env,
        )
        click.echo("📦 Bundle resource files synchronized")
        if self.verbose:
            click.echo("✅ Bundle resource files synchronized")


def _remove_legacy_state_artifacts(project_root: Path) -> None:
    legacy_file = project_root / ".lhp_state.json"
    legacy_dir = project_root / ".lhp_state"
    if legacy_file.exists():
        legacy_file.unlink()
    if legacy_dir.is_dir():
        shutil.rmtree(legacy_dir)


def _wipe_resources_lhp_directory(project_root: Path) -> None:
    resources_lhp = project_root / "resources" / "lhp"
    if resources_lhp.is_symlink():
        raise BundleResourceError(
            f"resources/lhp is a symlink; refusing to delete: {resources_lhp}. "
            f"Remove the symlink and let LHP manage this directory directly."
        )
    if resources_lhp.exists():
        shutil.rmtree(resources_lhp)
    resources_lhp.mkdir(parents=True, exist_ok=True)

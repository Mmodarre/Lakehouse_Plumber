"""Validate command implementation for LakehousePlumber CLI."""

import logging
from typing import List, Optional, Tuple

import click

from ...core.orchestrator import ActionOrchestrator
from ...utils.exit_codes import ExitCode
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class ValidateCommand(BaseCommand):
    """
    Handles pipeline configuration validation command.

    Validates YAML pipeline configurations for syntax, structure,
    and business logic rules across specified environments.
    """

    def execute(
        self, env: str = "dev", pipeline: Optional[str] = None, verbose: bool = False
    ) -> None:
        """
        Execute the validate command.

        Args:
            env: Environment to validate against
            pipeline: Specific pipeline to validate (optional)
            verbose: Enable verbose output
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Override verbose setting if provided directly
        if verbose:
            self.verbose = verbose

        logger.debug(
            f"Validation request: env={env}, pipeline={pipeline}, verbose={verbose}"
        )

        click.echo(f"🔍 Validating pipeline configurations for environment: {env}")
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")

        # Check if substitution file exists
        self.check_substitution_file(env)

        # Initialize orchestrator
        orchestrator = ActionOrchestrator(project_root)

        # Determine which pipelines to validate
        pipelines_to_validate = self._determine_pipelines_to_validate(
            pipeline, orchestrator
        )

        # Validate all pipelines
        total_errors, total_warnings = self._validate_all_pipelines(
            pipelines_to_validate, env, orchestrator
        )

        # Display summary
        self._display_validation_summary(
            env, len(pipelines_to_validate), total_errors, total_warnings
        )

        # Exit with appropriate code (let error boundary handle it)
        if total_errors > 0:
            raise SystemExit(ExitCode.DATA_ERROR)

    def _determine_pipelines_to_validate(
        self, pipeline: Optional[str], orchestrator: ActionOrchestrator
    ) -> List[str]:
        """Determine which pipelines to validate based on user input."""
        from ...utils.error_formatter import ErrorCategory, LHPConfigError

        if pipeline:
            # Check if specific pipeline exists
            logger.debug(f"Validating specific pipeline: {pipeline}")
            all_flowgroups = orchestrator.discover_all_flowgroups()
            pipeline_fields = {fg.pipeline for fg in all_flowgroups}

            if pipeline not in pipeline_fields:
                suggestions = [
                    "Check the pipeline name for typos",
                ]
                if pipeline_fields:
                    suggestions.insert(
                        0,
                        f"Available pipelines: {', '.join(sorted(pipeline_fields))}",
                    )
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="015",
                    title=f"Pipeline '{pipeline}' not found",
                    details=f"No flowgroup with pipeline field '{pipeline}' was found.",
                    suggestions=suggestions,
                    context={"Pipeline": pipeline},
                )
            return [pipeline]
        else:
            # Discover all pipeline fields from flowgroups
            all_flowgroups = orchestrator.discover_all_flowgroups()
            if not all_flowgroups:
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

            pipeline_fields = {fg.pipeline for fg in all_flowgroups}
            return sorted(pipeline_fields)

    def _validate_all_pipelines(
        self,
        pipelines_to_validate: List[str],
        env: str,
        orchestrator: ActionOrchestrator,
    ) -> Tuple[int, int]:
        """
        Validate all specified pipelines.

        Args:
            pipelines_to_validate: List of pipeline names to validate
            env: Environment name
            orchestrator: Action orchestrator instance

        Returns:
            Tuple of (total_errors, total_warnings)
        """
        total_errors = 0
        total_warnings = 0

        for pipeline_name in pipelines_to_validate:
            logger.debug(f"Starting validation for pipeline: {pipeline_name}")
            click.echo(f"\n🔧 Validating pipeline: {pipeline_name}")

            try:
                # Validate pipeline using orchestrator by field
                errors, warnings = orchestrator.validate_pipeline_by_field(
                    pipeline_name, env
                )

                pipeline_errors = len(errors)
                pipeline_warnings = len(warnings)
                total_errors += pipeline_errors
                total_warnings += pipeline_warnings

                # Show results for this pipeline
                self._display_pipeline_validation_results(
                    pipeline_name, pipeline_errors, pipeline_warnings, errors, warnings
                )

            except Exception as e:
                logger.warning(f"Validation error for pipeline '{pipeline_name}': {e}")
                click.echo(f"❌ Validation for pipeline '{pipeline_name}' failed: {e}")
                if self.log_file:
                    click.echo(f"📝 Check detailed logs: {self.log_file}")
                total_errors += 1

        return total_errors, total_warnings

    def _display_pipeline_validation_results(
        self,
        pipeline_name: str,
        pipeline_errors: int,
        pipeline_warnings: int,
        errors: List[str],
        warnings: List[str],
    ) -> None:
        """Display validation results for a single pipeline."""
        if pipeline_errors == 0 and pipeline_warnings == 0:
            click.echo(f"✅ Pipeline '{pipeline_name}' is valid")
        else:
            if pipeline_errors > 0:
                click.echo(
                    f"❌ Pipeline '{pipeline_name}' has {pipeline_errors} error(s)"
                )
                if self.verbose:
                    for error in errors:
                        click.echo(f"   Error: {error}")

            if pipeline_warnings > 0:
                click.echo(
                    f"⚠️  Pipeline '{pipeline_name}' has {pipeline_warnings} warning(s)"
                )
                if self.verbose:
                    for warning in warnings:
                        click.echo(f"   Warning: {warning}")

            if not self.verbose:
                click.echo("   Use --verbose flag to see detailed messages")

    def _display_validation_summary(
        self, env: str, pipelines_validated: int, total_errors: int, total_warnings: int
    ) -> None:
        """Display validation summary and exit with appropriate code."""
        logger.info(
            f"Validation summary: env={env}, pipelines={pipelines_validated}, "
            f"errors={total_errors}, warnings={total_warnings}"
        )

        click.echo("\n📊 Validation Summary:")
        click.echo(f"   Environment: {env}")
        click.echo(f"   Pipelines validated: {pipelines_validated}")
        click.echo(f"   Total errors: {total_errors}")
        click.echo(f"   Total warnings: {total_warnings}")

        if total_errors == 0:
            click.echo("\n✅ All configurations are valid")
        else:
            click.echo(f"\n❌ Validation failed with {total_errors} error(s)")

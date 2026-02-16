"""Dependencies command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List, Optional

import click

from ...core.project_config_loader import ProjectConfigLoader
from ...core.services.dependency_analyzer import DependencyAnalyzer
from ...core.services.dependency_output_manager import DependencyOutputManager
from ...utils.error_formatter import ErrorCategory, LHPError
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class DependenciesCommand(BaseCommand):
    """
    Handles pipeline dependency analysis and visualization command.

    Analyzes flowgroup and pipeline dependencies to enable orchestration
    planning and execution order determination using NetworkX graphs.
    """

    def execute(
        self,
        output_format: str = "all",
        output_dir: Optional[str] = None,
        pipeline: Optional[str] = None,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
        verbose: bool = False,
    ) -> None:
        """
        Execute the dependencies command.

        Args:
            output_format: Output format(s) to generate ("dot", "json", "text", "job", "all")
            output_dir: Output directory path (optional)
            pipeline: Specific pipeline to analyze (optional)
            job_name: Custom name for orchestration job (optional, only used with job format)
            job_config_path: Custom job config file path (relative to project root)
            bundle_output: If True, save job file to resources/ directory
            verbose: Enable verbose output
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        if verbose:
            self._setup_verbose_logging()

        logger.debug(
            f"Dependencies request: format={output_format}, pipeline={pipeline}, "
            f"job_name={job_name}, bundle_output={bundle_output}"
        )

        click.echo("🔍 Analyzing Pipeline Dependencies")
        click.echo("=" * 60)

        # Initialize services
        config_loader = ProjectConfigLoader(project_root)
        analyzer = DependencyAnalyzer(project_root, config_loader)
        output_manager = DependencyOutputManager()

        # Validate pipeline filter with job_name usage
        if pipeline:
            flowgroups = self._validate_pipeline_exists(analyzer, pipeline)
            # Check if job_name is used - error out if so
            if any(fg.job_name for fg in flowgroups):
                raise LHPError(
                    category=ErrorCategory.VALIDATION,
                    code_number="003",
                    title="Pipeline filter not supported with job_name",
                    details=(
                        "Cannot use --pipeline filter when job_name is defined in flowgroups.\n\n"
                        f"You specified: --pipeline {pipeline}\n"
                        "However, your flowgroups use job_name property which enables multi-job mode.\n\n"
                        "A single pipeline may span multiple jobs, making filtering ambiguous."
                    ),
                    suggestions=[
                        "Remove the --pipeline filter to analyze all jobs",
                        "Or remove job_name from flowgroups to use single-job mode",
                        "Use separate lhp deps runs for different projects if needed",
                    ],
                    context={
                        "Pipeline Filter": pipeline,
                        "Flowgroups with job_name": len(
                            [fg for fg in flowgroups if fg.job_name]
                        ),
                    },
                )

        # Perform dependency analysis
        click.echo("📊 Building dependency graphs...")
        result = analyzer.analyze_dependencies(pipeline_filter=pipeline)

        # Display analysis summary
        self._display_analysis_summary(result, pipeline)

        # Handle output generation
        output_formats = self._parse_output_formats(output_format)
        output_path = self._resolve_output_path(output_dir, project_root)

        # Adjust message if using bundle output
        if bundle_output:
            click.echo(f"\n💾 Generating output files...")
            click.echo(
                f"   Job file will be saved to resources/ directory for bundle integration"
            )
        else:
            click.echo(f"\n💾 Generating output files in {output_path}...")

        generated_files = output_manager.save_outputs(
            analyzer,
            result,
            output_formats,
            output_path,
            job_name,
            job_config_path,
            bundle_output,
        )

        # Display generated files
        self._display_generated_files(generated_files)

        # Show execution order if pipelines found
        if result.execution_stages:
            self._display_execution_order(result)

        # Show warnings if any issues detected
        self._display_warnings(result)

        logger.info(
            f"Dependency analysis complete: {len(result.execution_stages)} stages"
        )
        click.echo("\n✅ Dependency analysis complete!")

    def _setup_verbose_logging(self) -> None:
        """Enable verbose logging for detailed analysis output."""
        # Get the dependency analyzer logger
        dep_logger = logging.getLogger("lhp.core.services.dependency_analyzer")
        dep_logger.setLevel(logging.DEBUG)

        # Get the output manager logger
        out_logger = logging.getLogger("lhp.core.services.dependency_output_manager")
        out_logger.setLevel(logging.DEBUG)

    def _validate_pipeline_exists(self, analyzer: DependencyAnalyzer, pipeline: str):
        """Validate that the specified pipeline exists.

        Returns:
            List of flowgroups (reusable by caller to avoid redundant calls)
        """
        flowgroups = analyzer.get_flowgroups()
        available_pipelines = set(fg.pipeline for fg in flowgroups)

        if available_pipelines and pipeline not in available_pipelines:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="002",
                title=f"Pipeline '{pipeline}' not found",
                details=f"The specified pipeline '{pipeline}' does not exist in the project.",
                suggestions=[
                    f"Use one of the available pipelines: {', '.join(sorted(available_pipelines))}",
                    "Check the 'pipeline' field in your flowgroup YAML files",
                    "Verify that flowgroup YAML files are in the correct location",
                    "Run 'lhp stats' to see all available pipelines",
                ],
                context={
                    "Requested Pipeline": pipeline,
                    "Available Pipelines": sorted(available_pipelines),
                    "Total Available": len(available_pipelines),
                },
            )

        return flowgroups

    def _parse_output_formats(self, output_format: str) -> List[str]:
        """Parse and validate output format specification."""
        valid_formats = {"dot", "json", "text", "job", "all"}
        formats = [fmt.strip().lower() for fmt in output_format.split(",")]

        # Validate formats
        invalid_formats = set(formats) - valid_formats
        if invalid_formats:
            raise click.BadParameter(
                f"Invalid output format(s): {', '.join(invalid_formats)}. "
                f"Valid formats: {', '.join(valid_formats)}"
            )

        return formats

    def _resolve_output_path(
        self, output_dir: Optional[str], project_root: Path
    ) -> Path:
        """Resolve the output directory path."""
        if output_dir:
            return Path(output_dir).resolve()
        else:
            return project_root / ".lhp" / "dependencies"

    def _display_analysis_summary(self, result, pipeline_filter: Optional[str]) -> None:
        """Display summary of dependency analysis results."""
        click.echo(f"\n📈 Analysis Summary:")

        if pipeline_filter:
            click.echo(f"   Pipeline: {pipeline_filter}")
        else:
            click.echo(f"   Total pipelines analyzed: {result.total_pipelines}")

        click.echo(f"   Execution stages: {len(result.execution_stages)}")
        click.echo(f"   External sources: {result.total_external_sources}")

        if result.circular_dependencies:
            click.echo(
                f"   ⚠️  Circular dependencies: {len(result.circular_dependencies)}"
            )

    def _display_generated_files(self, generated_files: dict) -> None:
        """Display information about generated output files."""
        for format_name, file_path_or_dict in generated_files.items():
            # Handle both single file path and dict of paths (for multiple jobs)
            if isinstance(file_path_or_dict, dict):
                # Multiple job files (dict of {job_name: path})
                click.echo(f"   {format_name.upper()} (multiple jobs):")
                for job_name, job_path in file_path_or_dict.items():
                    file_size = job_path.stat().st_size if job_path.exists() else 0
                    if job_name == "_master":
                        click.echo(
                            f"      Master Job: {job_path} ({file_size:,} bytes)"
                        )
                    else:
                        click.echo(
                            f"      {job_name}: {job_path} ({file_size:,} bytes)"
                        )
            else:
                # Single file path (backward compatible)
                file_size = (
                    file_path_or_dict.stat().st_size
                    if file_path_or_dict.exists()
                    else 0
                )
                click.echo(
                    f"   {format_name.upper()}: {file_path_or_dict} ({file_size:,} bytes)"
                )

    def _display_execution_order(self, result) -> None:
        """Display pipeline execution order."""
        click.echo(f"\n🔄 Execution Order:")

        if not result.execution_stages:
            click.echo(
                "   No pipelines found or circular dependencies prevent execution order."
            )
            return

        for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
            if len(stage_pipelines) == 1:
                click.echo(f"   Stage {stage_idx}: {stage_pipelines[0]}")
            else:
                click.echo(
                    f"   Stage {stage_idx}: {', '.join(stage_pipelines)} (can run in parallel)"
                )

    def _display_warnings(self, result) -> None:
        """Display warnings about dependency analysis results."""
        if result.circular_dependencies:
            click.echo(f"\n⚠️  Warnings:")
            click.echo("   Circular dependencies detected! These must be resolved:")
            for cycle in result.circular_dependencies:
                for cycle_description in cycle:
                    click.echo(f"     {cycle_description}")
            click.echo("   Pipeline execution order may be affected.")

        if not result.execution_stages:
            click.echo(f"\n⚠️  Warning:")
            click.echo("   No execution order could be determined.")
            click.echo(
                "   This may indicate circular dependencies or missing pipelines."
            )

        self._display_info(result)

    def _display_info(self, result) -> None:
        """Display informational messages about dependency analysis results."""
        if result.total_external_sources > 0:
            click.echo(f"\n💡 Info:")
            click.echo(f"   {result.total_external_sources} external sources detected.")
            click.echo("   These are dependencies outside of LHP-managed pipelines.")
            if result.total_external_sources <= 5:
                click.echo("   External sources:")
                for source in result.external_sources:
                    click.echo(f"     {source}")
            else:
                click.echo(
                    "   Use generated files to see complete list of external sources."
                )

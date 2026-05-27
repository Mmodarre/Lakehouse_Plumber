"""Dependencies command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

import click
from rich.table import Table
from rich.text import Text

from ...api import (
    DependencyAnalysisResult,
    DependencyOutputEntry,
    DependencyOutputsResult,
    FlowgroupView,
    LakehousePlumberApplicationFacade,
)
from ...errors import ErrorCategory, LHPError
from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class DependenciesCommand(BaseCommand):
    """Pipeline dependency analysis and visualization."""

    def execute(
        self,
        output_format: str = "all",
        output_dir: Optional[str] = None,
        pipeline: Optional[str] = None,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
        verbose: bool = False,
        expand_blueprints: bool = False,
        blueprint_filter: Optional[str] = None,
    ) -> None:
        """Run dependency analysis.

        ``expand_blueprints=False`` dedupes synthetic flowgroups by
        ``(blueprint_name, spec_index)`` so the graph stays readable at scale;
        ``True`` renders the literal expansion (one node per
        blueprint × instance × spec). ``blueprint_filter`` restricts the
        graph to synthetic flowgroups expanded from the named blueprint.
        """
        render_command_header("lhp deps")
        self.setup_from_context()
        project_root = self.ensure_project_root()

        if verbose:
            self._setup_verbose_logging()

        logger.debug(
            f"Dependencies request: format={output_format}, pipeline={pipeline}, "
            f"job_name={job_name}, bundle_output={bundle_output}, "
            f"expand_blueprints={expand_blueprints}, "
            f"blueprint_filter={blueprint_filter}"
        )

        # Route through the §9.24-clean facade bootstrap so every domain
        # call goes via a sub-facade (no orchestrator reach-through).
        application_facade = LakehousePlumberApplicationFacade.for_project(
            project_root
        )

        output_formats = self._parse_output_formats(output_format)
        output_path = self._resolve_output_path(output_dir, project_root)

        if pipeline:
            self._validate_pipeline_exists(application_facade, pipeline)

        # Perform dependency analysis -- the graph build is the only
        # potentially-slow phase; wrap it in a stderr spinner so stdout
        # stays clean for piping.
        with _console_module.err_console.status("Building dependency graphs…"):
            result = application_facade.inspection.analyze_dependencies(
                pipeline_filter=pipeline,
                expand_blueprints=expand_blueprints,
                blueprint_filter=blueprint_filter,
            )

        self._display_analysis_summary(result, pipeline)

        # Announce where files will be written (stderr so stdout stays clean).
        if bundle_output:
            _console_module.err_console.print(
                "Writing job file to resources/ directory for bundle integration.",
                style="dim",
            )
        else:
            _console_module.err_console.print(
                f"Writing output files to {output_path}.",
                style="dim",
            )

        with _console_module.err_console.status("Generating output files…"):
            outputs = application_facade.inspection.save_dependency_outputs(
                formats=output_formats,
                output_dir=output_path,
                pipeline_filter=pipeline,
                expand_blueprints=expand_blueprints,
                blueprint_filter=blueprint_filter,
                job_name=job_name,
                job_config_path=job_config_path,
                bundle_output=bundle_output,
            )

        # File paths + sizes go to stdout so callers can pipe/grep them.
        self._display_generated_files(outputs)

        if result.execution_stages:
            self._display_execution_order(result)

        self._display_warnings(result)

        logger.info(
            f"Dependency analysis complete: {len(result.execution_stages)} stages"
        )
        _console_module.console.print(
            Text.assemble(("✓ ", "bold green"), "Dependency analysis complete.")
        )

    def _setup_verbose_logging(self) -> None:
        dep_logger = logging.getLogger("lhp.core.services.dependency_analyzer")
        dep_logger.setLevel(logging.DEBUG)

        out_logger = logging.getLogger("lhp.core.dependencies.output")
        out_logger.setLevel(logging.DEBUG)

    def _validate_pipeline_exists(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        pipeline: str,
    ) -> Tuple[FlowgroupView, ...]:
        """Validate that ``pipeline`` exists and is not in multi-job mode.

        Returns the discovered flowgroup views so the caller may reuse
        them. Raises :class:`LHPError` with a CLI-friendly message when
        the pipeline is unknown or when ``job_name`` is in use anywhere
        in the project (which makes pipeline filtering ambiguous).
        """
        flowgroups = application_facade.inspection.list_flowgroups()
        available_pipelines = {fg.pipeline for fg in flowgroups}

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

        pipeline_flowgroups = tuple(
            fg for fg in flowgroups if fg.pipeline == pipeline
        )
        if any(fg.job_name for fg in pipeline_flowgroups):
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
                        [fg for fg in pipeline_flowgroups if fg.job_name]
                    ),
                },
            )

        return pipeline_flowgroups

    def _parse_output_formats(self, output_format: str) -> List[str]:
        valid_formats = {"dot", "json", "text", "job", "all"}
        formats = [fmt.strip().lower() for fmt in output_format.split(",")]

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
        if output_dir:
            return Path(output_dir).resolve()
        else:
            return project_root / ".lhp" / "dependencies"

    def _display_analysis_summary(
        self,
        result: DependencyAnalysisResult,
        pipeline_filter: Optional[str],
    ) -> None:
        """Inline header per STYLE.md §8 — not promoted to a shared helper."""
        _console_module.console.print()
        _console_module.console.print(Text("Analysis Summary", style="bold dim"))

        if pipeline_filter:
            _console_module.console.print(f"  Pipeline: {pipeline_filter}")
        else:
            _console_module.console.print(
                f"  Total pipelines analyzed: {result.total_pipelines}"
            )

        _console_module.console.print(
            f"  Execution stages: {len(result.execution_stages)}"
        )
        _console_module.console.print(
            f"  External sources: {result.total_external_sources}"
        )

        if result.circular_dependencies:
            _console_module.console.print(
                Text.assemble(
                    "  ",
                    ("⚠ Circular dependencies: ", "bold yellow"),
                    str(len(result.circular_dependencies)),
                )
            )

    def _display_generated_files(self, outputs: DependencyOutputsResult) -> None:
        """Render generated-file paths grouped by format.

        Multi-job runs of the ``job`` format produce one entry per job
        plus one ``"_master"`` entry; those share a format name and are
        rendered as a nested group.
        """
        by_format: dict[str, List[DependencyOutputEntry]] = {}
        for entry in outputs.entries:
            by_format.setdefault(entry.format_name, []).append(entry)

        for format_name, entries in by_format.items():
            # Single-file outputs collapse to a one-line render; the
            # ``job`` format may be either single or multi-job (label
            # populated for multi-job entries).
            multi = len(entries) > 1 or (
                len(entries) == 1 and entries[0].label
            )
            if multi:
                _console_module.console.print(
                    f"  {format_name.upper()} (multiple jobs):"
                )
                for entry in entries:
                    file_size = (
                        entry.path.stat().st_size if entry.path.exists() else 0
                    )
                    if entry.label == "_master":
                        _console_module.console.print(
                            f"    Master Job: {entry.path} ({file_size:,} bytes)"
                        )
                    else:
                        _console_module.console.print(
                            f"    {entry.label}: {entry.path} ({file_size:,} bytes)"
                        )
            else:
                entry = entries[0]
                file_size = entry.path.stat().st_size if entry.path.exists() else 0
                _console_module.console.print(
                    f"  {format_name.upper()}: {entry.path} ({file_size:,} bytes)"
                )

    def _display_execution_order(self, result: DependencyAnalysisResult) -> None:
        """Inline execution-order table per STYLE.md §8.

        Non-TTY branch writes tab-separated rows directly to
        ``_console_module.console.file`` so pipe consumers (e.g.
        ``lhp deps | grep <pipeline>``) see literal tabs intact.
        """
        _console_module.console.print()
        _console_module.console.print(Text("Execution Order", style="bold dim"))

        if not result.execution_stages:
            _console_module.console.print(
                "  No pipelines found or circular dependencies prevent execution order."
            )
            return

        rows: List[List[str]] = []
        for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
            if len(stage_pipelines) == 1:
                rows.append([str(stage_idx), stage_pipelines[0], ""])
            else:
                # One row per parallel pipeline so the tab-separated view
                # stays grep-friendly at one-pipeline-per-line.
                for name in stage_pipelines:
                    rows.append([str(stage_idx), name, "parallel"])

        headers = ("Stage", "Pipeline", "Notes")

        if _console_module.console.is_terminal:
            table = Table(
                border_style="dim",
                header_style="bold dim",
            )
            table.add_column("Stage", justify="right")
            table.add_column("Pipeline")
            table.add_column("Notes", style="dim")
            for row in rows:
                table.add_row(*row)
            _console_module.console.print(table)
            return

        # Non-TTY: tab-separated rows so ``| grep <pipeline>`` works.
        lines = ["\t".join(headers)]
        for row in rows:
            lines.append("\t".join(row))
        _console_module.console.file.write("\n".join(lines) + "\n")

    def _display_warnings(self, result: DependencyAnalysisResult) -> None:
        """Display warnings about dependency analysis results.

        Warnings go to stderr so they do not contaminate the stdout data
        stream that callers may pipe or redirect.
        """
        if result.circular_dependencies:
            _console_module.err_console.print()
            _console_module.err_console.print(Text("⚠ Warnings", style="bold yellow"))
            _console_module.err_console.print(
                "  Circular dependencies detected! These must be resolved:"
            )
            for cycle in result.circular_dependencies:
                for cycle_description in cycle:
                    _console_module.err_console.print(f"    {cycle_description}")
            _console_module.err_console.print(
                "  Pipeline execution order may be affected."
            )

        if not result.execution_stages:
            _console_module.err_console.print()
            _console_module.err_console.print(Text("⚠ Warning", style="bold yellow"))
            _console_module.err_console.print(
                "  No execution order could be determined."
            )
            _console_module.err_console.print(
                "  This may indicate circular dependencies or missing pipelines."
            )

        self._display_info(result)

    def _display_info(self, result: DependencyAnalysisResult) -> None:
        """Display informational messages about dependency analysis results.

        External-source notices are advisory (not part of the primary data
        stream) and route to stderr per the sink discipline in STYLE.md §2.
        """
        if result.total_external_sources > 0:
            _console_module.err_console.print()
            _console_module.err_console.print(Text("Info", style="dim"))
            _console_module.err_console.print(
                f"  {result.total_external_sources} external sources detected."
            )
            _console_module.err_console.print(
                "  These are dependencies outside of LHP-managed pipelines."
            )
            if result.total_external_sources <= 5:
                _console_module.err_console.print("  External sources:")
                for source in result.external_sources:
                    _console_module.err_console.print(f"    {source}")
            else:
                _console_module.err_console.print(
                    "  Use generated files to see complete list of external sources."
                )

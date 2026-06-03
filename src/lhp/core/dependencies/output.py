"""Output management service for dependency analysis results.

In addition to the file-I/O facade (``DependencyOutputManager``), this
module exposes three pure module-level serializers — ``export_to_dot``,
``export_to_json``, ``export_to_text`` — used by both the manager (for
file output) and the service-level ``export`` ABC method (returns a
string). The shared text-rendering implementation lives in the private
module-level ``_generate_text_representation`` function so the manager's
file-save path and ``export_to_text`` produce identical output.
"""

# JUSTIFIED: This module is ~687 lines because it co-locates three concerns
# that all need the same `DependencyAnalysisResult` / `DependencyGraphs`
# shape: (1) the pure serializers (`export_to_dot`, `export_to_json`,
# `export_to_text`) consumed by both the file-save facade and the future
# service-level `export()` ABC method, (2) the shared private renderers
# (`_generate_text_representation`, `_generate_dependency_tree_text`)
# that emit the multi-section human-readable report (header, summary,
# execution order, per-pipeline details, external sources, circular
# dependencies, ASCII tree), and (3) the file-I/O facade
# `DependencyOutputManager` with its dot/json/text/job save paths plus
# multi-job partitioning logic. Splitting the renderers into a
# `_text_renderer.py` shadow module would force two `_generate_*` import
# hops for one logical operation (the renderer also recursively calls
# `_generate_dependency_tree_text`, so the helpers travel as a pair),
# while splitting the file-save facade into a separate `manager.py`
# would force the manager to import the serializers it co-owns — net no
# clarity gain. The 80-line text renderer carries the bulk of the
# size; once the placeholder text format stabilizes it can be folded
# into a Jinja template, which is the real long-term path to shrinking
# this module below 500 lines.
# TODO(Phase 9.5): fold the text/tree renderer into a Jinja template and split DependencyOutputManager into per-format exporters once the format stabilises; see LOCAL/REMAINING_WORK.md §9.5.

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ...errors import ErrorFactory, codes
from ...models.dependencies import DependencyAnalysisResult, DependencyGraphs
from ..jobs.job_generator import JobGenerator

if TYPE_CHECKING:
    from .service import DependencyAnalysisService


def export_to_dot(graphs: DependencyGraphs, level: str = "pipeline") -> str:
    """Export a dependency graph to DOT format.

    Args:
        graphs: Dependency graphs (action / flowgroup / pipeline triple)
        level: Graph level to export (``"action"``, ``"flowgroup"``, or ``"pipeline"``)

    Returns:
        DOT format string representation of the requested graph level.
    """
    graph = graphs.get_graph_by_level(level)

    # Use networkx built-in DOT export with custom attributes
    dot_lines = [f"digraph {level}_dependencies {{"]
    dot_lines.append("  rankdir=LR;")
    dot_lines.append("  node [shape=box];")

    for node in graph.nodes():
        node_attrs = graph.nodes[node]
        label = node

        if level == "pipeline" and "flowgroup_count" in node_attrs:
            label = f"{node}\\n({node_attrs['flowgroup_count']} flowgroups)"
        elif level == "flowgroup" and "action_count" in node_attrs:
            label = f"{node}\\n({node_attrs['action_count']} actions)"

        dot_lines.append(f'  "{node}" [label="{label}"];')

    for source, target in graph.edges():
        dot_lines.append(f'  "{source}" -> "{target}";')

    dot_lines.append("}")

    return "\n".join(dot_lines)


def export_to_json(result: DependencyAnalysisResult) -> Dict[str, Any]:
    """Export a dependency analysis result to a structured JSON-ready dict.

    Args:
        result: Complete dependency analysis result

    Returns:
        Structured dictionary suitable for ``json.dump``.
    """
    return {
        "metadata": {
            "total_pipelines": result.total_pipelines,
            "total_external_sources": result.total_external_sources,
            "total_stages": len(result.execution_stages),
            "has_circular_dependencies": len(result.circular_dependencies) > 0,
        },
        "pipelines": {
            name: {
                "depends_on": dep.depends_on,
                "flowgroup_count": dep.flowgroup_count,
                "action_count": dep.action_count,
                "external_sources": dep.external_sources,
                "can_run_parallel": dep.can_run_parallel,
                "stage": dep.stage,
            }
            for name, dep in result.pipeline_dependencies.items()
        },
        "execution_stages": result.execution_stages,
        "external_sources": result.external_sources,
        "circular_dependencies": result.circular_dependencies,
    }


def export_to_text(result: DependencyAnalysisResult) -> str:
    """Render a dependency analysis result as a human-readable text report.

    Thin wrapper around the shared :func:`_generate_text_representation`
    used internally by :class:`DependencyOutputManager`'s file-save path.
    """
    return _generate_text_representation(result)


def _generate_text_representation(result: DependencyAnalysisResult) -> str:
    """Generate human-readable text representation of dependency analysis.

    Shared by :func:`export_to_text` and
    :meth:`DependencyOutputManager._generate_text_representation`.
    """
    lines = []

    lines.append("=" * 80)
    lines.append("LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS")
    lines.append("=" * 80)
    lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    lines.append("SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Total Pipelines: {result.total_pipelines}")
    lines.append(f"Total Execution Stages: {len(result.execution_stages)}")
    lines.append(f"External Sources: {result.total_external_sources}")
    lines.append(f"Circular Dependencies: {len(result.circular_dependencies)}")
    lines.append("")

    lines.append("EXECUTION ORDER")
    lines.append("-" * 40)
    if result.execution_stages:
        for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
            if len(stage_pipelines) == 1:
                lines.append(f"Stage {stage_idx}: {stage_pipelines[0]}")
            else:
                lines.append(
                    f"Stage {stage_idx}: {', '.join(stage_pipelines)} (parallel)"
                )
    else:
        lines.append(
            "No pipelines found or circular dependencies prevent execution order."
        )
    lines.append("")

    lines.append("PIPELINE DETAILS")
    lines.append("-" * 40)
    for pipeline_name in sorted(result.pipeline_dependencies.keys()):
        dep = result.pipeline_dependencies[pipeline_name]
        lines.append(f"Pipeline: {pipeline_name}")
        lines.append(f"  Flowgroups: {dep.flowgroup_count}")
        lines.append(f"  Actions: {dep.action_count}")
        lines.append(
            f"  Depends on: {', '.join(dep.depends_on) if dep.depends_on else 'None'}"
        )
        lines.append(f"  Stage: {dep.stage if dep.stage is not None else 'N/A'}")
        lines.append(f"  Can run parallel: {dep.can_run_parallel}")
        if dep.external_sources:
            lines.append(f"  External sources: {', '.join(dep.external_sources[:5])}")
            if len(dep.external_sources) > 5:
                lines.append(f"    ... and {len(dep.external_sources) - 5} more")
        lines.append("")

    if result.external_sources:
        lines.append("EXTERNAL SOURCES")
        lines.append("-" * 40)
        for ext_source in sorted(result.external_sources):
            lines.append(f"  {ext_source}")
        lines.append("")

    if result.circular_dependencies:
        lines.append("CIRCULAR DEPENDENCIES")
        lines.append("-" * 40)
        lines.append("⚠️  WARNING: Circular dependencies detected!")
        lines.append("These must be resolved before pipeline execution:")
        for cycle in result.circular_dependencies:
            lines.append(f"  {cycle[0]}")
        lines.append("")

    lines.append("DEPENDENCY TREE")
    lines.append("-" * 40)
    lines.extend(_generate_dependency_tree_text(result))

    return "\n".join(lines)


def _generate_dependency_tree_text(
    result: DependencyAnalysisResult,
) -> List[str]:
    """Generate ASCII tree representation of pipeline dependencies."""
    lines: List[str] = []

    if not result.pipeline_dependencies:
        lines.append("No pipelines found.")
        return lines

    root_pipelines = [
        name for name, dep in result.pipeline_dependencies.items() if not dep.depends_on
    ]

    if not root_pipelines:
        lines.append("⚠️  No root pipelines found (possible circular dependencies)")
        return lines

    visited: set = set()

    def add_pipeline_tree(pipeline: str, indent: str = "", is_last: bool = True):
        if pipeline in visited:
            lines.append(
                f"{indent}{'└── ' if is_last else '├── '}{pipeline} (already shown)"
            )
            return

        visited.add(pipeline)
        dep = result.pipeline_dependencies.get(pipeline)
        if dep:
            info = f"{pipeline} ({dep.flowgroup_count} flowgroups, {dep.action_count} actions)"
        else:
            info = pipeline

        lines.append(f"{indent}{'└── ' if is_last else '├── '}{info}")

        dependents = [
            name
            for name, dep in result.pipeline_dependencies.items()
            if pipeline in dep.depends_on
        ]

        child_indent = indent + ("    " if is_last else "│   ")
        for i, dependent in enumerate(sorted(dependents)):
            is_last_child = i == len(dependents) - 1
            add_pipeline_tree(dependent, child_indent, is_last_child)

    for i, root_pipeline in enumerate(sorted(root_pipelines)):
        is_last_root = i == len(root_pipelines) - 1
        add_pipeline_tree(root_pipeline, "", is_last_root)

    return lines


class DependencyOutputManager:
    """File-I/O facade for dependency analysis results (DOT/JSON/text/job formats)."""

    def __init__(self, base_output_dir: Optional[Path] = None):
        self.base_output_dir = base_output_dir
        self.logger = logging.getLogger(__name__)

    def save_outputs(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        output_formats: List[str],
        output_dir: Optional[Path] = None,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
    ) -> Dict[str, Path]:
        """
        Save dependency analysis results in specified formats.

        Args:
            analyzer: DependencyAnalysisService instance for format-specific exports
            result: Complete dependency analysis result
            output_formats: List of formats to generate ("dot", "json", "text", "job", "all")
            output_dir: Specific output directory (overrides base_output_dir)
            job_name: Custom name for orchestration job (only used with job format)
            job_config_path: Custom job config file path (relative to project root)
            bundle_output: If True, save job file to resources/ directory for bundle integration

        Returns:
            Dictionary mapping format names to generated file paths

        Raises:
            IOError: If output directory cannot be created or files cannot be written
            ValueError: If invalid output format specified
        """
        target_dir = self._resolve_output_directory(output_dir)
        self._ensure_directory_exists(target_dir)

        if "all" in output_formats:
            output_formats = ["dot", "json", "text", "job"]

        valid_formats = {"dot", "json", "text", "job"}
        invalid_formats = set(output_formats) - valid_formats
        if invalid_formats:
            raise ErrorFactory.config_error(
                codes.CFG_014,
                title="Invalid output format(s)",
                details=f"Invalid output formats: {invalid_formats}. Valid formats: {valid_formats}",
                suggestions=[
                    f"Use one or more of: {', '.join(sorted(valid_formats))}",
                    "Use 'all' to generate all formats",
                ],
                context={
                    "Invalid": list(invalid_formats),
                    "Valid": sorted(valid_formats),
                },
            )

        generated_files = {}

        if "dot" in output_formats:
            dot_file = self._save_dot_format(analyzer, result.graphs, target_dir)
            generated_files["dot"] = dot_file

        if "json" in output_formats:
            json_file = self._save_json_format(analyzer, result, target_dir)
            generated_files["json"] = json_file

        if "text" in output_formats:
            text_file = self._save_text_format(result, target_dir)
            generated_files["text"] = text_file

        if "job" in output_formats:
            if result.circular_dependencies:
                self.logger.warning(
                    "Skipping job format generation due to circular dependencies. "
                    "Circular dependencies must be resolved before orchestration "
                    "job generation is possible."
                )
            else:
                job_file = self._save_job_format(
                    analyzer,
                    result,
                    target_dir,
                    job_name,
                    job_config_path,
                    bundle_output,
                )
                generated_files["job"] = job_file

        self.logger.info(
            f"Generated {len(generated_files)} output files in {target_dir}"
        )
        return generated_files

    def save_dot_format(
        self,
        analyzer: "DependencyAnalysisService",
        graphs: DependencyGraphs,
        output_path: Path,
        level: str = "pipeline",
    ) -> Path:
        """``analyzer`` is kept for backward compatibility; DOT conversion uses
        :func:`export_to_dot` directly."""
        del analyzer  # Unused — DOT conversion is now a pure module-level function.
        dot_content = export_to_dot(graphs, level)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(dot_content)
            self.logger.debug(f"DOT format saved to {output_path}")
            return output_path
        except IOError as e:
            raise IOError(f"Failed to save DOT file to {output_path}: {e}") from e

    def save_json_format(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        output_path: Path,
    ) -> Path:
        """``analyzer`` is kept for backward compatibility; JSON conversion uses
        :func:`export_to_json` directly."""
        del analyzer  # Unused — JSON conversion is now a pure module-level function.
        json_data = export_to_json(result)

        # Add generation metadata
        json_data["generation_info"] = {
            "generated_at": datetime.now().isoformat(),
            "generator": "LakehousePlumber DependencyAnalysisService",
            "version": "1.0",
        }

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"JSON format saved to {output_path}")
            return output_path
        except (IOError, TypeError) as e:
            raise IOError(f"Failed to save JSON file to {output_path}: {e}") from e

    def save_text_format(
        self, result: DependencyAnalysisResult, output_path: Path
    ) -> Path:
        text_content = self._generate_text_representation(result)

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(text_content)
            self.logger.debug(f"Text format saved to {output_path}")
            return output_path
        except IOError as e:
            raise IOError(f"Failed to save text file to {output_path}: {e}") from e

    def _resolve_output_directory(self, output_dir: Optional[Path]) -> Path:
        if output_dir:
            return output_dir
        if self.base_output_dir:
            return self.base_output_dir
        return Path.cwd() / ".lhp" / "dependencies"

    def _ensure_directory_exists(self, directory: Path) -> None:
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise IOError(f"Cannot create output directory {directory}: {e}") from e

    def _save_dot_format(
        self,
        analyzer: "DependencyAnalysisService",
        graphs: DependencyGraphs,
        target_dir: Path,
    ) -> Path:
        """Returns pipeline-level DOT path; also writes flowgroup-level file."""
        pipeline_dot_file = target_dir / "pipeline_dependencies.dot"
        self.save_dot_format(analyzer, graphs, pipeline_dot_file, level="pipeline")

        flowgroup_dot_file = target_dir / "flowgroup_dependencies.dot"
        self.save_dot_format(analyzer, graphs, flowgroup_dot_file, level="flowgroup")

        self.logger.info(
            f"Generated DOT files: {pipeline_dot_file.name} and {flowgroup_dot_file.name}"
        )

        return pipeline_dot_file

    def _save_json_format(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        target_dir: Path,
    ) -> Path:
        json_file = target_dir / "pipeline_dependencies.json"
        return self.save_json_format(analyzer, result, json_file)

    def _save_text_format(
        self, result: DependencyAnalysisResult, target_dir: Path
    ) -> Path:
        text_file = target_dir / "pipeline_dependencies.txt"
        return self.save_text_format(result, text_file)

    def _generate_text_representation(self, result: DependencyAnalysisResult) -> str:
        return _generate_text_representation(result)

    def _generate_dependency_tree_text(
        self, result: DependencyAnalysisResult
    ) -> List[str]:
        return _generate_dependency_tree_text(result)

    def _save_job_format(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        target_dir: Path,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
    ):
        """
        Save job format to standard filename.

        Detects if job_name is used in flowgroups and generates either:
        - Single job file (backward compatible)
        - Multiple job files + master job (when job_name is used)

        Args:
            analyzer: DependencyAnalysisService instance
            result: Dependency analysis result
            target_dir: Target directory for output (used when bundle_output=False)
            job_name: Custom job name (only used when no job_name in flowgroups)
            job_config_path: Custom job config file path
            bundle_output: If True, save to resources/ directory (flat structure)

        Returns:
            Path or Dict[str, Path] - single path for backward compat, dict for multiple jobs
        """

        job_generator = JobGenerator(
            project_root=analyzer.project_root, config_file_path=job_config_path
        )

        project_name = analyzer.get_project_name()

        try:
            flowgroups = analyzer.get_flowgroups()
            has_job_name = any(fg.job_name for fg in flowgroups)
        except (TypeError, AttributeError) as e:
            # If we can't check flowgroups (mocked analyzer), assume single-job mode
            self.logger.debug(f"Could not check flowgroup job_name properties: {e}")
            has_job_name = False

        if not has_job_name:
            self.logger.info(
                "No job_name defined - generating single orchestration job"
            )

            if not job_name:
                job_name = f"{project_name}_orchestration"

            if bundle_output:
                job_file = analyzer.project_root / "resources" / f"{job_name}.job.yml"
            else:
                job_file = target_dir / f"{job_name}.job.yml"

            return job_generator.save_job_to_file(
                result, job_file, job_name, project_name
            )

        # Multi-job mode: job_name is defined in flowgroups
        self.logger.info(
            "job_name detected - generating multiple jobs + master orchestration job"
        )

        job_results = analyzer.partition_result_by_job(result, flowgroups)
        global_result = result

        if bundle_output:
            output_dir = analyzer.project_root / "resources"
        else:
            output_dir = target_dir

        output_dir.mkdir(parents=True, exist_ok=True)

        job_yamls = job_generator.generate_jobs_by_name(job_results, project_name)
        generated_files = {}
        for job_name_key, job_yaml in job_yamls.items():
            job_file = output_dir / f"{job_name_key}.job.yml"

            try:
                with open(job_file, "w", encoding="utf-8") as f:
                    f.write(job_yaml)
                generated_files[job_name_key] = job_file
                self.logger.info(f"Generated job file: {job_file}")
            except IOError:
                self.logger.exception(f"Failed to write job file {job_file}")
                raise

        if job_generator.should_generate_master_job():
            master_job_name = job_generator.get_master_job_name(project_name)

            master_yaml = job_generator.generate_master_job(
                job_results, master_job_name, project_name, global_result=global_result
            )

            master_file = output_dir / f"{master_job_name}.job.yml"
            try:
                with open(master_file, "w", encoding="utf-8") as f:
                    f.write(master_yaml)
                generated_files["_master"] = master_file
                self.logger.info(f"Generated master job file: {master_file}")
            except IOError:
                self.logger.exception(f"Failed to write master job file {master_file}")
                raise
        else:
            self.logger.info(
                "Master job generation disabled via job_config.yaml "
                "(project_defaults.generate_master_job: false)"
            )

        self.logger.info(f"Generated {len(generated_files)} job file(s) total")
        return generated_files

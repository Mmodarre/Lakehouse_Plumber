"""Output management service for dependency analysis results."""

import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime

from ...models.dependencies import DependencyAnalysisResult, DependencyGraphs
from .dependency_analyzer import DependencyAnalyzer
from .job_generator import JobGenerator


class DependencyOutputManager:
    """
    Manages different output formats for dependency analysis results.

    Follows Python best practices for file I/O, error handling, and
    separation of concerns. Provides multiple output formats with
    consistent naming and structure.
    """

    def __init__(self, base_output_dir: Optional[Path] = None):
        """
        Initialize output manager.

        Args:
            base_output_dir: Base directory for outputs. If None, uses .lhp/dependencies/
        """
        self.base_output_dir = base_output_dir
        self.logger = logging.getLogger(__name__)

    def save_outputs(self, analyzer: DependencyAnalyzer, result: DependencyAnalysisResult,
                    output_formats: List[str], output_dir: Optional[Path] = None,
                    job_name: Optional[str] = None) -> Dict[str, Path]:
        """
        Save dependency analysis results in specified formats.

        Args:
            analyzer: DependencyAnalyzer instance for format-specific exports
            result: Complete dependency analysis result
            output_formats: List of formats to generate ("dot", "json", "text", "job", "all")
            output_dir: Specific output directory (overrides base_output_dir)
            job_name: Custom name for orchestration job (only used with job format)

        Returns:
            Dictionary mapping format names to generated file paths

        Raises:
            IOError: If output directory cannot be created or files cannot be written
            ValueError: If invalid output format specified
        """
        # Determine output directory
        target_dir = self._resolve_output_directory(output_dir)
        self._ensure_directory_exists(target_dir)

        # Expand "all" format
        if "all" in output_formats:
            output_formats = ["dot", "json", "text", "job"]
            output_formats = [fmt for fmt in output_formats if fmt != "all"]

        # Validate formats
        valid_formats = {"dot", "json", "text", "job"}
        invalid_formats = set(output_formats) - valid_formats
        if invalid_formats:
            raise ValueError(f"Invalid output formats: {invalid_formats}. Valid formats: {valid_formats}")

        generated_files = {}

        # Generate each requested format
        try:
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
                job_file = self._save_job_format(analyzer, result, target_dir, job_name)
                generated_files["job"] = job_file


            self.logger.info(f"Generated {len(generated_files)} output files in {target_dir}")
            return generated_files

        except Exception as e:
            self.logger.error(f"Error generating output files: {e}")
            raise IOError(f"Failed to save dependency outputs: {e}") from e

    def save_dot_format(self, analyzer: DependencyAnalyzer, graphs: DependencyGraphs,
                       output_path: Path, level: str = "pipeline") -> Path:
        """
        Save dependency graph in DOT format.

        Args:
            analyzer: DependencyAnalyzer instance
            graphs: Dependency graphs
            output_path: Full path for the output file
            level: Graph level to export ("action", "flowgroup", or "pipeline")

        Returns:
            Path to the generated DOT file
        """
        dot_content = analyzer.export_to_dot(graphs, level)

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(dot_content)
            self.logger.debug(f"DOT format saved to {output_path}")
            return output_path
        except IOError as e:
            raise IOError(f"Failed to save DOT file to {output_path}: {e}") from e

    def save_json_format(self, analyzer: DependencyAnalyzer, result: DependencyAnalysisResult,
                        output_path: Path) -> Path:
        """
        Save dependency analysis in structured JSON format.

        Args:
            analyzer: DependencyAnalyzer instance
            result: Complete dependency analysis result
            output_path: Full path for the output file

        Returns:
            Path to the generated JSON file
        """
        json_data = analyzer.export_to_json(result)

        # Add generation metadata
        json_data["generation_info"] = {
            "generated_at": datetime.now().isoformat(),
            "generator": "LakehousePlumber DependencyAnalyzer",
            "version": "1.0"
        }

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False)
            self.logger.debug(f"JSON format saved to {output_path}")
            return output_path
        except (IOError, TypeError) as e:
            raise IOError(f"Failed to save JSON file to {output_path}: {e}") from e

    def save_text_format(self, result: DependencyAnalysisResult, output_path: Path) -> Path:
        """
        Save dependency analysis in human-readable text format.

        Args:
            result: Complete dependency analysis result
            output_path: Full path for the output file

        Returns:
            Path to the generated text file
        """
        text_content = self._generate_text_representation(result)

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(text_content)
            self.logger.debug(f"Text format saved to {output_path}")
            return output_path
        except IOError as e:
            raise IOError(f"Failed to save text file to {output_path}: {e}") from e

    # Private helper methods

    def _resolve_output_directory(self, output_dir: Optional[Path]) -> Path:
        """Resolve the output directory to use."""
        if output_dir:
            return output_dir
        elif self.base_output_dir:
            return self.base_output_dir
        else:
            # Default to .lhp/dependencies/ in current working directory
            return Path.cwd() / ".lhp" / "dependencies"

    def _ensure_directory_exists(self, directory: Path) -> None:
        """Ensure output directory exists, creating it if necessary."""
        try:
            directory.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise IOError(f"Cannot create output directory {directory}: {e}") from e

    def _save_dot_format(self, analyzer: DependencyAnalyzer, graphs: DependencyGraphs,
                        target_dir: Path) -> Path:
        """Save DOT format files for both pipeline and flowgroup levels."""
        # Save pipeline-level dependencies
        pipeline_dot_file = target_dir / "pipeline_dependencies.dot"
        self.save_dot_format(analyzer, graphs, pipeline_dot_file, level="pipeline")

        # Save flowgroup-level dependencies
        flowgroup_dot_file = target_dir / "flowgroup_dependencies.dot"
        self.save_dot_format(analyzer, graphs, flowgroup_dot_file, level="flowgroup")

        self.logger.info(f"Generated DOT files: {pipeline_dot_file.name} and {flowgroup_dot_file.name}")

        # Return the pipeline file for backward compatibility
        return pipeline_dot_file

    def _save_json_format(self, analyzer: DependencyAnalyzer, result: DependencyAnalysisResult,
                         target_dir: Path) -> Path:
        """Save JSON format to standard filename."""
        json_file = target_dir / "pipeline_dependencies.json"
        return self.save_json_format(analyzer, result, json_file)

    def _save_text_format(self, result: DependencyAnalysisResult, target_dir: Path) -> Path:
        """Save text format to standard filename."""
        text_file = target_dir / "pipeline_dependencies.txt"
        return self.save_text_format(result, text_file)

    def _generate_text_representation(self, result: DependencyAnalysisResult) -> str:
        """Generate human-readable text representation of dependency analysis."""
        lines = []

        # Header
        lines.append("=" * 80)
        lines.append("LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS")
        lines.append("=" * 80)
        lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # Summary statistics
        lines.append("SUMMARY")
        lines.append("-" * 40)
        lines.append(f"Total Pipelines: {result.total_pipelines}")
        lines.append(f"Total Execution Stages: {len(result.execution_stages)}")
        lines.append(f"External Sources: {result.total_external_sources}")
        lines.append(f"Circular Dependencies: {len(result.circular_dependencies)}")
        lines.append("")

        # Execution order
        lines.append("EXECUTION ORDER")
        lines.append("-" * 40)
        if result.execution_stages:
            for stage_idx, stage_pipelines in enumerate(result.execution_stages, 1):
                if len(stage_pipelines) == 1:
                    lines.append(f"Stage {stage_idx}: {stage_pipelines[0]}")
                else:
                    lines.append(f"Stage {stage_idx}: {', '.join(stage_pipelines)} (parallel)")
        else:
            lines.append("No pipelines found or circular dependencies prevent execution order.")
        lines.append("")

        # Pipeline details
        lines.append("PIPELINE DETAILS")
        lines.append("-" * 40)
        for pipeline_name in sorted(result.pipeline_dependencies.keys()):
            dep = result.pipeline_dependencies[pipeline_name]
            lines.append(f"Pipeline: {pipeline_name}")
            lines.append(f"  Flowgroups: {dep.flowgroup_count}")
            lines.append(f"  Actions: {dep.action_count}")
            lines.append(f"  Depends on: {', '.join(dep.depends_on) if dep.depends_on else 'None'}")
            lines.append(f"  Stage: {dep.stage if dep.stage is not None else 'N/A'}")
            lines.append(f"  Can run parallel: {dep.can_run_parallel}")
            if dep.external_sources:
                lines.append(f"  External sources: {', '.join(dep.external_sources[:5])}")
                if len(dep.external_sources) > 5:
                    lines.append(f"    ... and {len(dep.external_sources) - 5} more")
            lines.append("")

        # External sources
        if result.external_sources:
            lines.append("EXTERNAL SOURCES")
            lines.append("-" * 40)
            for ext_source in sorted(result.external_sources):
                lines.append(f"  {ext_source}")
            lines.append("")

        # Circular dependencies
        if result.circular_dependencies:
            lines.append("CIRCULAR DEPENDENCIES")
            lines.append("-" * 40)
            lines.append("⚠️  WARNING: Circular dependencies detected!")
            lines.append("These must be resolved before pipeline execution:")
            for cycle in result.circular_dependencies:
                lines.append(f"  {cycle[0]}")
            lines.append("")

        # Dependency tree visualization
        lines.append("DEPENDENCY TREE")
        lines.append("-" * 40)
        lines.extend(self._generate_dependency_tree_text(result))

        return "\n".join(lines)

    def _generate_dependency_tree_text(self, result: DependencyAnalysisResult) -> List[str]:
        """Generate ASCII tree representation of pipeline dependencies."""
        lines = []

        if not result.pipeline_dependencies:
            lines.append("No pipelines found.")
            return lines

        # Find root pipelines (no dependencies)
        root_pipelines = [
            name for name, dep in result.pipeline_dependencies.items()
            if not dep.depends_on
        ]

        if not root_pipelines:
            lines.append("⚠️  No root pipelines found (possible circular dependencies)")
            return lines

        # Build tree representation
        visited = set()

        def add_pipeline_tree(pipeline: str, indent: str = "", is_last: bool = True):
            if pipeline in visited:
                lines.append(f"{indent}{'└── ' if is_last else '├── '}{pipeline} (already shown)")
                return

            visited.add(pipeline)

            # Pipeline info
            dep = result.pipeline_dependencies.get(pipeline)
            if dep:
                info = f"{pipeline} ({dep.flowgroup_count} flowgroups, {dep.action_count} actions)"
            else:
                info = pipeline

            lines.append(f"{indent}{'└── ' if is_last else '├── '}{info}")

            # Find dependents
            dependents = [
                name for name, dep in result.pipeline_dependencies.items()
                if pipeline in dep.depends_on
            ]

            # Add dependent pipelines
            child_indent = indent + ("    " if is_last else "│   ")
            for i, dependent in enumerate(sorted(dependents)):
                is_last_child = i == len(dependents) - 1
                add_pipeline_tree(dependent, child_indent, is_last_child)

        # Add each root pipeline and its dependents
        for i, root_pipeline in enumerate(sorted(root_pipelines)):
            is_last_root = i == len(root_pipelines) - 1
            add_pipeline_tree(root_pipeline, "", is_last_root)

        return lines

    def _save_job_format(self, analyzer: DependencyAnalyzer, result: DependencyAnalysisResult,
                        target_dir: Path, job_name: Optional[str] = None) -> Path:
        """Save job format to standard filename."""
        job_generator = JobGenerator()

        # Extract project name from target directory or use default
        project_name = target_dir.parent.parent.name if target_dir.parent.parent.name != "." else "lhp_project"

        # Use provided job name or generate default
        if not job_name:
            job_name = f"{project_name}_orchestration"

        job_file = target_dir / f"{job_name}.job.yml"
        return job_generator.save_job_to_file(result, job_file, job_name, project_name)


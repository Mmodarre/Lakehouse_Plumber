"""Disk-writing facade for dependency analysis results.

:class:`DependencyOutputWriter` owns the file-I/O paths for the
dot/json/text/job output formats (including multi-job partitioning). It
obtains the rendered strings by composing
:class:`~lhp.core.dependencies.output.DependencyOutputFormatter` — the
pure serializers live in the sibling :mod:`output` module so the save
path and the service-level ``export()`` produce identical output.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from ...errors import ErrorFactory, codes
from ...models.dependencies import DependencyAnalysisResult, DependencyGraphs
from ..jobs.job_generator import JobGenerator
from .output import DependencyOutputFormatter

if TYPE_CHECKING:
    from .service import DependencyAnalysisService


class DependencyOutputWriter:
    """File-I/O facade for dependency analysis results (DOT/JSON/text/job formats)."""

    def __init__(self, base_output_dir: Optional[Path] = None):
        self.base_output_dir = base_output_dir
        self.logger = logging.getLogger(__name__)
        self._formatter = DependencyOutputFormatter()

    def save_outputs(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        output_formats: List[str],
        output_dir: Optional[Path] = None,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
        *,
        trust_depends_on: bool = False,
        filters_active: bool = False,
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
            trust_depends_on: Analysis mode for the "job" format's global pass;
                must match the mode ``result`` was produced with so both share
                one memoized analysis
            filters_active: True when ``result`` was produced under a
                pipeline/blueprint filter. The "job" format is a whole-project
                DEPLOYMENT artifact (its global pass ignores filters), so it is
                skipped with a warning rather than silently written from a
                different flowgroup set than the other outputs

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
            if filters_active:
                self.logger.warning(
                    "Skipping job format generation: a pipeline/blueprint "
                    "filter is active and orchestration job files are "
                    "whole-project artifacts. Re-run without --pipeline/"
                    "--blueprint to generate them."
                )
            elif result.circular_dependencies:
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
                    trust_depends_on=trust_depends_on,
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
        the formatter directly."""
        del analyzer  # Unused — DOT conversion is now a pure formatter method.
        dot_content = self._formatter.export_to_dot(graphs, level)

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
        the formatter directly."""
        del analyzer  # Unused — JSON conversion is now a pure formatter method.
        json_data = self._formatter.export_to_json(result)

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
        text_content = self._formatter.export_to_text(result)

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

    def _save_job_format(
        self,
        analyzer: "DependencyAnalysisService",
        result: DependencyAnalysisResult,
        target_dir: Path,
        job_name: Optional[str] = None,
        job_config_path: Optional[str] = None,
        bundle_output: bool = False,
        *,
        trust_depends_on: bool = False,
    ) -> Union[Path, Dict[str, Path]]:
        """
        Save job format to standard filename.

        Validation, single-vs-multi detection, and per-job partitioning are
        delegated to ``analyzer.analyze_dependencies_by_job()`` — the single
        validated source of truth. This method only dispatches the write:
        either a single orchestration job file (no ``job_name`` in flowgroups)
        or multiple job files + a master job (when ``job_name`` is used).

        Args:
            analyzer: DependencyAnalysisService instance
            result: Dependency analysis result (used for the single-job write)
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

        # The service validates job_name usage (raising LHP-VAL-001/002 on
        # inconsistent configs), runs the single global analyze pass, and
        # partitions the result per job_name. We only choose the write path.
        job_results, global_result = analyzer.analyze_dependencies_by_job(
            trust_depends_on=trust_depends_on
        )

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

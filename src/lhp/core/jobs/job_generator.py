"""
Job generator service for creating orchestration jobs from dependency analysis.

This module provides the JobGenerator class that creates Databricks job YAML
configurations based on pipeline dependency analysis results.
"""

import logging
from pathlib import Path
from typing import Any, Dict, Optional

from ...errors import ErrorFactory, codes
from ...models.dependencies import DependencyAnalysisResult
from ...utils.yaml_filters import dict_to_yaml
from ..loaders import JobConfigLoader
from .job_builder import (
    JobPipeline,
    JobStage,
    analyze_cross_job_dependencies,
    build_job_stages,
    build_pipeline_to_job_mapping,
)
from .job_writer import write_job_yaml

__all__ = [
    "EXPLICITLY_RENDERED_JOB_CONFIG_KEYS",
    "JobGenerator",
    "JobPipeline",
    "JobStage",
]

logger = logging.getLogger(__name__)


# Top-level job_config keys that the Jinja job templates render explicitly.
# Anything NOT in this set is passed through as-is via the `toyaml` filter,
# so users can use new Databricks Jobs API fields (trigger.file_arrival,
# continuous, run_as, git_source, health, etc.) without waiting for LHP to
# explicitly support them.
#
# Kept here (not inside JobGenerator) so templates and tests can reference it
# via a single source of truth.
EXPLICITLY_RENDERED_JOB_CONFIG_KEYS = frozenset(
    {
        # Rendered by job_resource.yml.j2 / monitoring_job_resource.yml.j2
        "max_concurrent_runs",
        "queue",
        "performance_target",
        "timeout_seconds",
        "tags",
        "email_notifications",
        "webhook_notifications",
        "permissions",
        "schedule",
        # Only in monitoring_job_resource.yml.j2
        "notebook_cluster",
        # LHP-internal control knobs — must never be emitted into the rendered YAML
        "generate_master_job",
        "master_job_name",
    }
)


class JobGenerator:
    """
    Generates Databricks orchestration jobs from dependency analysis results.

    This service transforms pipeline dependency information into executable
    job configurations with proper task ordering and dependency management.
    """

    # Default job configuration values
    DEFAULT_JOB_CONFIG = {
        "max_concurrent_runs": 1,
        "queue": {"enabled": True},
        "performance_target": "STANDARD",
        "generate_master_job": True,  # Control master job generation
        "master_job_name": None,  # Custom master job name (None = auto-generate)
    }

    def __init__(
        self,
        template_dir: Optional[Path] = None,
        project_root: Optional[Path] = None,
        config_file_path: Optional[str] = None,
    ):
        """
        Initialize the job generator.

        Args:
            template_dir: Directory containing Jinja2 templates. If None, uses
                the LHP package template loader.
            project_root: Root directory of the project for loading custom config.
            config_file_path: Custom config file path (relative to project_root).
        """
        from jinja2 import Environment

        if template_dir is None:
            # Default: load templates from the installed lhp package.
            from ..codegen.template_renderer import get_lhp_template_loader

            loader = get_lhp_template_loader()
        else:
            # Test/injection path: read templates from the provided directory.
            from jinja2 import FileSystemLoader

            loader = FileSystemLoader(template_dir)

        # Distinct Environment settings: YAML output requires block-preserving
        # behavior and trailing-newline retention, unlike the Python generators.
        self.jinja_env = Environment(  # nosec B701 — generates YAML, not HTML
            loader=loader,
            trim_blocks=False,
            lstrip_blocks=False,
            keep_trailing_newline=True,
        )
        # Pass-through filter: lets job_resource templates render any unknown
        # job_config key as YAML so users can use new Databricks Jobs fields
        # (trigger.file_arrival, continuous, run_as, git_source, …) without
        # waiting for an LHP release to explicitly support them.
        self.jinja_env.filters["toyaml"] = dict_to_yaml
        # Exposed as a Jinja global so every render site sees it without
        # having to thread it through its own context dict.
        self.jinja_env.globals["explicitly_rendered_keys"] = (
            EXPLICITLY_RENDERED_JOB_CONFIG_KEYS
        )
        self.logger = logger

        # Load and merge job configuration (used by the `deps` orchestration-job flow)
        self.project_defaults, self.job_specific_configs = JobConfigLoader().load(
            project_root, config_file_path
        )
        # For backward compatibility, store merged defaults as job_config
        self.job_config = self._deep_merge_dicts(
            self.DEFAULT_JOB_CONFIG.copy(), self.project_defaults
        )

    @staticmethod
    def _deep_merge_dicts(
        base: Dict[str, Any], override: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep merge two dictionaries. Nested dicts recursed; lists replaced."""
        result = base.copy()

        for key, value in override.items():
            if (
                key in result
                and isinstance(result[key], dict)
                and isinstance(value, dict)
            ):
                result[key] = JobGenerator._deep_merge_dicts(result[key], value)
            else:
                # Replace value (including lists)
                result[key] = value

        return result

    def get_job_config_for_job(self, job_name: str) -> Dict[str, Any]:
        """Merge in order: DEFAULT_JOB_CONFIG → project_defaults → job-specific config."""
        config = self.DEFAULT_JOB_CONFIG.copy()
        config = self._deep_merge_dicts(config, self.project_defaults)

        if job_name in self.job_specific_configs:
            config = self._deep_merge_dicts(config, self.job_specific_configs[job_name])
            self.logger.debug(f"Using job-specific config for '{job_name}'")
        else:
            self.logger.debug(
                f"No job-specific config for '{job_name}', using project_defaults"
            )

        return config

    def should_generate_master_job(self) -> bool:
        """Check if master orchestration job should be generated.

        Reads from project_defaults.generate_master_job in job_config.yaml.

        Returns:
            bool: True if master job should be generated (default: True)
        """
        return self.project_defaults.get("generate_master_job", True)

    def get_master_job_name(self, project_name: str) -> str:
        """Get master job name from config or generate default.

        Reads from project_defaults.master_job_name in job_config.yaml.

        Args:
            project_name: Project name for auto-generating default name

        Returns:
            str: Master job name (custom or auto-generated)

        Example:
            Custom: "production_orchestrator"
            Auto: "acme_edw_master"
        """
        custom_name = self.project_defaults.get("master_job_name")
        if custom_name:
            self.logger.info(f"Using custom master job name: {custom_name}")
            return custom_name
        return f"{project_name}_master"

    def generate_job(
        self,
        dependency_result: DependencyAnalysisResult,
        job_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> str:
        """
        Generate job YAML content from dependency analysis results.

        Args:
            dependency_result: Results from dependency analysis
            job_name: Custom name for the job (defaults to project_name_orchestration)
            project_name: Name of the project (used in template)

        Returns:
            YAML content for the orchestration job

        Raises:
            ValueError: If no pipelines found in dependency results
        """
        if not dependency_result.execution_stages:
            raise ErrorFactory.validation_error(
                codes.VAL_009,
                title="No pipeline execution stages found",
                details="No pipeline execution stages found in dependency results.",
                suggestions=[
                    "Ensure the project has flowgroups with pipeline definitions",
                    "Run 'lhp deps' to check pipeline dependencies",
                    "Verify that flowgroup YAML files exist and are valid",
                ],
                context={},
            )

        if not project_name:
            project_name = "lhp_project"

        if not job_name:
            job_name = f"{project_name}_orchestration"

        job_stages = build_job_stages(dependency_result, job_name)

        context = {
            "project_name": project_name,
            "job_name": job_name,
            "execution_stages": job_stages,
            "total_pipelines": len(dependency_result.pipeline_dependencies),
            "total_stages": len(dependency_result.execution_stages),
            "job_config": self.job_config,
        }

        try:
            template = self.jinja_env.get_template("bundle/job_resource.yml.j2")
            return template.render(**context)
        except Exception as e:
            self.logger.exception(f"Failed to render job template: {e}")
            raise

    def save_job_to_file(
        self,
        dependency_result: DependencyAnalysisResult,
        output_path: Path,
        job_name: Optional[str] = None,
        project_name: Optional[str] = None,
    ) -> Path:
        """
        Generate and save job YAML to a file.

        Args:
            dependency_result: Results from dependency analysis
            output_path: Directory or file path to save the job YAML
            job_name: Custom name for the job
            project_name: Name of the project

        Returns:
            Path to the generated job file

        Raises:
            IOError: If file cannot be written
        """
        job_content = self.generate_job(dependency_result, job_name, project_name)

        if output_path.is_dir():
            filename = f"{job_name or 'orchestration_job'}.job.yml"
            file_path = output_path / filename
        else:
            file_path = output_path

        return write_job_yaml(job_content, file_path)

    def generate_jobs_by_name(
        self,
        job_results: Dict[str, DependencyAnalysisResult],
        project_name: Optional[str] = None,
    ) -> Dict[str, str]:
        """
        Generate multiple job YAML files from job-specific dependency results.

        Args:
            job_results: Dictionary mapping job_name to DependencyAnalysisResult
            project_name: Name of the project (used in templates)

        Returns:
            Dictionary mapping job_name to YAML content
        """
        if not project_name:
            project_name = "lhp_project"

        job_yamls = {}

        for job_name, dep_result in job_results.items():
            self.logger.info(f"Generating job YAML for: {job_name}")

            job_config = self.get_job_config_for_job(job_name)
            job_stages = build_job_stages(dep_result, job_name)

            context = {
                "project_name": project_name,
                "job_name": job_name,
                "execution_stages": job_stages,
                "total_pipelines": len(dep_result.pipeline_dependencies),
                "total_stages": len(dep_result.execution_stages),
                "job_config": job_config,
            }

            try:
                template = self.jinja_env.get_template("bundle/job_resource.yml.j2")
                job_yaml = template.render(**context)
                job_yamls[job_name] = job_yaml
                self.logger.debug(
                    f"Generated YAML for job '{job_name}' ({len(job_yaml)} bytes)"
                )

            except Exception as e:
                self.logger.exception(
                    f"Failed to render template for job '{job_name}': {e}"
                )
                raise

        self.logger.info(f"Generated {len(job_yamls)} job YAML file(s)")
        return job_yamls

    def generate_master_job(
        self,
        job_results: Dict[str, DependencyAnalysisResult],
        master_job_name: str,
        project_name: Optional[str] = None,
        global_result: Optional[DependencyAnalysisResult] = None,
    ) -> str:
        """
        Generate master orchestration job with cross-job dependencies.

        Uses global dependency analysis to determine which jobs depend on others
        based on their pipeline relationships.

        Args:
            job_results: Mapping of job_name to individual job's DependencyAnalysisResult
            master_job_name: Name for the master orchestration job
            project_name: Project name for metadata
            global_result: Global DependencyAnalysisResult from analyzing all flowgroups.
                          REQUIRED for correct dependency resolution.

        Returns:
            YAML string for master orchestration job with proper depends_on clauses

        Raises:
            ValueError: If global_result is None (required for correct dependencies)
        """
        if not project_name:
            project_name = "lhp_project"

        if global_result is None:
            raise ErrorFactory.validation_error(
                codes.VAL_009,
                title="Missing global_result for master job generation",
                details=(
                    "global_result is required for generate_master_job(). "
                    "Pass the global DependencyAnalysisResult from analyze_dependencies_by_job()."
                ),
                suggestions=[
                    "Provide the global DependencyAnalysisResult when calling generate_master_job()",
                    "Use analyze_dependencies_by_job() to get the global result",
                ],
                context={"Master Job Name": master_job_name},
            )

        self.logger.info(f"Generating master orchestration job: {master_job_name}")

        pipeline_to_job = build_pipeline_to_job_mapping(job_results)
        jobs_info = analyze_cross_job_dependencies(
            job_results, pipeline_to_job, global_result
        )

        context = {
            "master_job_name": master_job_name,
            "project_name": project_name,
            "jobs": jobs_info,
        }

        try:
            template = self.jinja_env.get_template("bundle/master_job_resource.yml.j2")
            master_yaml = template.render(**context)
            self.logger.info(f"Generated master job with {len(jobs_info)} job task(s)")
            return master_yaml

        except Exception as e:
            self.logger.exception(f"Failed to render master job template: {e}")
            raise

    def generate_monitoring_job(
        self,
        pipeline_name: str,
        notebook_path: str,
        job_name: str,
        job_config: Dict[str, Any],
        has_pipeline: bool = True,
    ) -> str:
        """Generate monitoring job YAML (notebook_task → optional pipeline_task).

        Args:
            pipeline_name: Name of the monitoring DLT pipeline
            notebook_path: Workspace path to the union event logs notebook
            job_name: Resolved monitoring job name
            job_config: Resolved monitoring job config (already merged with defaults
                and substituted). The caller is responsible for building this dict —
                typically via ``JobGenerator.resolve_monitoring_job_config``.
            has_pipeline: Whether a DLT pipeline exists (False = notebook-only job)

        Returns:
            Rendered YAML string for the monitoring job resource
        """
        context = {
            "job_name": job_name,
            "pipeline_name": pipeline_name,
            "notebook_path": notebook_path,
            "job_config": job_config,
            "has_pipeline": has_pipeline,
        }

        try:
            template = self.jinja_env.get_template(
                "bundle/monitoring_job_resource.yml.j2"
            )
            return template.render(**context)
        except Exception as e:
            self.logger.exception(f"Failed to render monitoring job template: {e}")
            raise

    @classmethod
    def resolve_monitoring_job_config(
        cls, raw_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Deep-merge a raw monitoring job config dict over ``DEFAULT_JOB_CONFIG``.

        This keeps merge semantics inside the class that owns ``DEFAULT_JOB_CONFIG``
        and is used by the orchestrator to build the per-generate monitoring job
        config from the dedicated YAML file referenced by
        ``monitoring.job_config_path``.

        Args:
            raw_config: Parsed monitoring job config (post token substitution).

        Returns:
            Merged config dict ready to hand to ``generate_monitoring_job``.
        """
        return cls._deep_merge_dicts(cls.DEFAULT_JOB_CONFIG.copy(), raw_config or {})

"""
Job generator service for creating orchestration jobs from dependency analysis.

This module provides the JobGenerator class that creates Databricks job YAML
configurations based on pipeline dependency analysis results.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import yaml

from ...models.dependencies import DependencyAnalysisResult
from ...utils.template_renderer import TemplateRenderer


logger = logging.getLogger(__name__)


@dataclass
class JobPipeline:
    """Represents a pipeline in a job context with dependency information."""
    name: str
    depends_on: List[str]
    stage: int


@dataclass
class JobStage:
    """Represents a job execution stage with pipelines."""
    stage_number: int
    pipelines: List[JobPipeline]
    is_parallel: bool


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
        "performance_target": "STANDARD"
    }

    def __init__(self, 
                 template_dir: Optional[Path] = None,
                 project_root: Optional[Path] = None,
                 config_file_path: Optional[str] = None):
        """
        Initialize the job generator.

        Args:
            template_dir: Directory containing Jinja2 templates. If None, uses default.
            project_root: Root directory of the project for loading custom config.
            config_file_path: Custom config file path (relative to project_root).
        """
        if template_dir is None:
            # Default to the templates directory in the package
            template_dir = Path(__file__).parent.parent.parent / "templates"

        # Create a custom template renderer with different settings for YAML formatting
        from jinja2 import Environment, FileSystemLoader
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            trim_blocks=False,
            lstrip_blocks=False,
            keep_trailing_newline=True
        )
        self.logger = logger
        
        # Load and merge job configuration
        self.job_config = self._load_job_config(project_root, config_file_path)

    def _load_job_config(self, 
                         project_root: Optional[Path] = None,
                         config_file_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load user's custom job config and merge with defaults.
        
        Args:
            project_root: Root directory of the project
            config_file_path: Custom config file path (relative to project_root)
            
        Returns:
            Merged job configuration dictionary
            
        Raises:
            FileNotFoundError: If specified config file doesn't exist
            yaml.YAMLError: If config file has invalid YAML syntax
        """
        # Start with defaults
        config = self.DEFAULT_JOB_CONFIG.copy()
        
        # If no project root, return defaults only
        if project_root is None:
            return config
        
        # Determine config file path
        if config_file_path:
            # Custom path specified
            full_config_path = project_root / config_file_path
            if not full_config_path.exists():
                raise FileNotFoundError(
                    f"Job config file not found: {config_file_path} "
                    f"(looking in {project_root})"
                )
        else:
            # Default path
            full_config_path = project_root / "templates" / "bundle" / "job_config.yaml"
            if not full_config_path.exists():
                # No custom config, return defaults
                self.logger.debug(f"No custom job config found at {full_config_path}, using defaults")
                return config
        
        # Load user config
        try:
            with open(full_config_path, 'r', encoding='utf-8') as f:
                user_config = yaml.safe_load(f)
            
            # If file is empty or only comments, return defaults
            if user_config is None:
                self.logger.debug(f"Empty config file at {full_config_path}, using defaults")
                return config
            
            # Merge user config with defaults (user values override)
            config.update(user_config)
            self.logger.info(f"Loaded custom job config from {full_config_path}")
            
            return config
            
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in job config file {full_config_path}: {e}")
            raise

    def generate_job(self,
                    dependency_result: DependencyAnalysisResult,
                    job_name: Optional[str] = None,
                    project_name: Optional[str] = None) -> str:
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
            raise ValueError("No pipeline execution stages found in dependency results")

        # Set defaults
        if not project_name:
            project_name = "lhp_project"

        if not job_name:
            job_name = f"{project_name}_orchestration"

        # Transform dependency data for template
        job_stages = self._create_job_stages(dependency_result)

        # Build template context
        context = {
            "project_name": project_name,
            "job_name": job_name,
            "execution_stages": job_stages,
            "total_pipelines": len(dependency_result.pipeline_dependencies),
            "total_stages": len(dependency_result.execution_stages),
            "job_config": self.job_config
        }

        # Render template
        try:
            template = self.jinja_env.get_template("bundle/job_resource.yml.j2")
            return template.render(**context)
        except Exception as e:
            self.logger.error(f"Failed to render job template: {e}")
            raise

    def _create_job_stages(self, dependency_result: DependencyAnalysisResult) -> List[JobStage]:
        """
        Transform dependency execution stages into job stages with pipeline information.

        Args:
            dependency_result: Dependency analysis results

        Returns:
            List of JobStage objects with pipeline and dependency information
        """
        job_stages = []

        for stage_idx, stage_pipelines in enumerate(dependency_result.execution_stages):
            stage_number = stage_idx + 1
            is_parallel = len(stage_pipelines) > 1

            # Create JobPipeline objects for this stage
            pipelines = []
            for pipeline_name in stage_pipelines:
                pipeline_info = dependency_result.pipeline_dependencies.get(pipeline_name)
                depends_on = pipeline_info.depends_on if pipeline_info else []

                job_pipeline = JobPipeline(
                    name=pipeline_name,
                    depends_on=depends_on,
                    stage=stage_number
                )
                pipelines.append(job_pipeline)

            job_stage = JobStage(
                stage_number=stage_number,
                pipelines=pipelines,
                is_parallel=is_parallel
            )
            job_stages.append(job_stage)

        return job_stages

    def save_job_to_file(self,
                        dependency_result: DependencyAnalysisResult,
                        output_path: Path,
                        job_name: Optional[str] = None,
                        project_name: Optional[str] = None) -> Path:
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
        # Generate job content
        job_content = self.generate_job(dependency_result, job_name, project_name)

        # Determine output file path
        if output_path.is_dir():
            filename = f"{job_name or 'orchestration_job'}.yml"
            file_path = output_path / filename
        else:
            file_path = output_path

        # Ensure parent directory exists
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Write file
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(job_content)

            self.logger.info(f"Generated job file: {file_path}")
            return file_path

        except IOError as e:
            self.logger.error(f"Failed to write job file to {file_path}: {e}")
            raise
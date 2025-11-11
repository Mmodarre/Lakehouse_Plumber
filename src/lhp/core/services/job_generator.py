"""
Job generator service for creating orchestration jobs from dependency analysis.

This module provides the JobGenerator class that creates Databricks job YAML
configurations based on pipeline dependency analysis results.
"""

import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
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
        self.project_defaults, self.job_specific_configs = self._load_job_config(project_root, config_file_path)
        # For backward compatibility, store merged defaults as job_config
        self.job_config = self._deep_merge_dicts(self.DEFAULT_JOB_CONFIG.copy(), self.project_defaults)

    def _load_job_config(self, 
                         project_root: Optional[Path] = None,
                         config_file_path: Optional[str] = None) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
        """
        Load user's custom job config with multi-document support.
        
        Supports two formats:
        1. Single document (backward compatible): Returns as project_defaults
        2. Multi-document:
           - First document with 'project_defaults' key → global defaults
           - Subsequent documents with 'job_name' key → job-specific configs
        
        Args:
            project_root: Root directory of the project
            config_file_path: Custom config file path (relative to project_root)
            
        Returns:
            Tuple of (project_defaults, job_specific_configs_dict)
            
        Raises:
            FileNotFoundError: If specified config file doesn't exist
            yaml.YAMLError: If config file has invalid YAML syntax
        """
        # If no project root, return empty configs
        if project_root is None:
            return {}, {}
        
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
                # No custom config, return empty
                self.logger.debug(f"No custom job config found at {full_config_path}, using defaults")
                return {}, {}
        
        # Load all YAML documents
        try:
            with open(full_config_path, 'r', encoding='utf-8') as f:
                documents = list(yaml.safe_load_all(f))
            
            # Filter out None/empty documents
            documents = [doc for doc in documents if doc is not None]
            
            if not documents:
                self.logger.debug(f"Empty config file at {full_config_path}, using defaults")
                return {}, {}
            
            # Check if this is multi-document format
            if len(documents) == 1:
                # Single document - check if it has project_defaults key
                doc = documents[0]
                if "project_defaults" in doc:
                    # Multi-doc format with only project_defaults
                    self.logger.info(f"Loaded project_defaults from {full_config_path}")
                    return doc["project_defaults"], {}
                else:
                    # Legacy single-doc format - treat entire doc as project_defaults
                    self.logger.info(f"Loaded single-document job config from {full_config_path}")
                    return doc, {}
            
            # Multi-document format
            project_defaults = {}
            job_specific_configs = {}
            
            for idx, doc in enumerate(documents):
                if "project_defaults" in doc:
                    # This is the project defaults document
                    project_defaults = doc["project_defaults"]
                    self.logger.info(f"Loaded project_defaults from document {idx+1}")
                    
                elif "job_name" in doc:
                    # This is a job-specific config
                    job_name = doc["job_name"]
                    # Extract all fields except job_name
                    job_config = {k: v for k, v in doc.items() if k != "job_name"}
                    job_specific_configs[job_name] = job_config
                    self.logger.info(f"Loaded job-specific config for '{job_name}' from document {idx+1}")
                    
                else:
                    self.logger.warning(
                        f"Document {idx+1} in {full_config_path} has neither 'project_defaults' "
                        f"nor 'job_name' key - skipping"
                    )
            
            return project_defaults, job_specific_configs
            
        except yaml.YAMLError as e:
            self.logger.error(f"Invalid YAML in job config file {full_config_path}: {e}")
            raise
    
    def _deep_merge_dicts(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries. Nested dicts are merged recursively, lists are replaced.
        
        Args:
            base: Base dictionary
            override: Dictionary with override values
            
        Returns:
            Merged dictionary (new dict, does not modify inputs)
        """
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dicts
                result[key] = self._deep_merge_dicts(result[key], value)
            else:
                # Replace value (including lists)
                result[key] = value
        
        return result
    
    def get_job_config_for_job(self, job_name: str) -> Dict[str, Any]:
        """
        Get merged configuration for a specific job.
        
        Merges in order: DEFAULT_JOB_CONFIG → project_defaults → job-specific config
        
        Args:
            job_name: Name of the job
            
        Returns:
            Merged configuration dictionary for the job
        """
        # Start with defaults
        config = self.DEFAULT_JOB_CONFIG.copy()
        
        # Merge project_defaults
        config = self._deep_merge_dicts(config, self.project_defaults)
        
        # Merge job-specific config if exists
        if job_name in self.job_specific_configs:
            config = self._deep_merge_dicts(config, self.job_specific_configs[job_name])
            self.logger.debug(f"Using job-specific config for '{job_name}'")
        else:
            self.logger.debug(f"No job-specific config for '{job_name}', using project_defaults")
        
        return config

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
            filename = f"{job_name or 'orchestration_job'}.job.yml"
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
    
    def generate_jobs_by_name(self,
                              job_results: Dict[str, DependencyAnalysisResult],
                              project_name: Optional[str] = None) -> Dict[str, str]:
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
            
            # Get job-specific config
            job_config = self.get_job_config_for_job(job_name)
            
            # Transform dependency data for template
            job_stages = self._create_job_stages(dep_result)
            
            # Build template context
            context = {
                "project_name": project_name,
                "job_name": job_name,
                "execution_stages": job_stages,
                "total_pipelines": len(dep_result.pipeline_dependencies),
                "total_stages": len(dep_result.execution_stages),
                "job_config": job_config
            }
            
            # Render template
            try:
                template = self.jinja_env.get_template("bundle/job_resource.yml.j2")
                job_yaml = template.render(**context)
                job_yamls[job_name] = job_yaml
                self.logger.debug(f"Generated YAML for job '{job_name}' ({len(job_yaml)} bytes)")
                
            except Exception as e:
                self.logger.error(f"Failed to render template for job '{job_name}': {e}")
                raise
        
        self.logger.info(f"Generated {len(job_yamls)} job YAML file(s)")
        return job_yamls
    
    def generate_master_job(self,
                           job_results: Dict[str, DependencyAnalysisResult],
                           master_job_name: str,
                           project_name: Optional[str] = None) -> str:
        """
        Generate master orchestration job that coordinates all individual jobs.
        
        The master job uses job_task references to execute individual jobs
        in the correct order based on cross-job dependencies.
        
        Args:
            job_results: Dictionary mapping job_name to DependencyAnalysisResult
            master_job_name: Name for the master orchestration job
            project_name: Name of the project
            
        Returns:
            YAML content for the master orchestration job
        """
        if not project_name:
            project_name = "lhp_project"
        
        self.logger.info(f"Generating master orchestration job: {master_job_name}")
        
        # Analyze cross-job dependencies
        # For now, we'll use a simple ordering based on job names
        # In a full implementation, this would analyze pipeline dependencies across jobs
        jobs_info = {}
        
        for job_name, dep_result in job_results.items():
            # For each job, determine which other jobs it depends on
            # based on pipeline dependencies
            depends_on = []
            
            # Get all pipelines in this job
            job_pipelines = set(dep_result.pipeline_dependencies.keys())
            
            # Check dependencies against other jobs
            for other_job_name, other_dep_result in job_results.items():
                if other_job_name == job_name:
                    continue
                
                other_pipelines = set(other_dep_result.pipeline_dependencies.keys())
                
                # Check if any pipeline in current job depends on pipelines in other job
                for pipeline in job_pipelines:
                    pipeline_dep = dep_result.pipeline_dependencies.get(pipeline)
                    if pipeline_dep and any(dep in other_pipelines for dep in pipeline_dep.depends_on):
                        depends_on.append(other_job_name)
                        break
            
            jobs_info[job_name] = {
                "depends_on": list(set(depends_on)),  # Remove duplicates
                "pipeline_count": len(job_pipelines)
            }
        
        # Build template context
        context = {
            "master_job_name": master_job_name,
            "project_name": project_name,
            "jobs": jobs_info
        }
        
        # Render master job template
        try:
            template = self.jinja_env.get_template("bundle/master_job_resource.yml.j2")
            master_yaml = template.render(**context)
            self.logger.info(f"Generated master job with {len(jobs_info)} job task(s)")
            return master_yaml
            
        except Exception as e:
            self.logger.error(f"Failed to render master job template: {e}")
            raise
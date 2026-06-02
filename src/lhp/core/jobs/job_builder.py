"""Stateless helpers for assembling Databricks job-stage data structures.

``JobPipeline`` / ``JobStage`` are internal in-memory containers used by the
Jinja job templates — NOT public DTOs.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ...models.dependencies import DependencyAnalysisResult

logger = logging.getLogger(__name__)


@dataclass
class JobPipeline:
    name: str
    depends_on: List[str]
    stage: int


@dataclass
class JobStage:
    stage_number: int
    pipelines: List[JobPipeline]
    is_parallel: bool


def build_job_stages(
    dependency_result: DependencyAnalysisResult,
    job_name: Optional[str] = None,
) -> List[JobStage]:
    """Transform dependency execution stages into job stages; pipelines within a stage are sorted for deterministic output."""
    job_stages: List[JobStage] = []
    log_suffix = f" for job '{job_name}'" if job_name else ""
    logger.debug(
        f"Creating {len(dependency_result.execution_stages)} job stage(s) "
        f"from dependency analysis{log_suffix}"
    )

    for stage_idx, stage_pipelines in enumerate(dependency_result.execution_stages):
        stage_number = stage_idx + 1
        is_parallel = len(stage_pipelines) > 1

        # Sort pipelines for deterministic output
        pipelines: List[JobPipeline] = []
        for pipeline_name in sorted(stage_pipelines):
            pipeline_info = dependency_result.pipeline_dependencies.get(pipeline_name)
            depends_on = sorted(pipeline_info.depends_on) if pipeline_info else []

            job_pipeline = JobPipeline(
                name=pipeline_name, depends_on=depends_on, stage=stage_number
            )
            pipelines.append(job_pipeline)

        job_stage = JobStage(
            stage_number=stage_number, pipelines=pipelines, is_parallel=is_parallel
        )
        job_stages.append(job_stage)

    return job_stages


def build_pipeline_to_job_mapping(
    job_results: Dict[str, DependencyAnalysisResult],
) -> Dict[str, str]:
    """Map pipeline name -> owning job name across the supplied per-job analysis results."""
    pipeline_to_job: Dict[str, str] = {}
    for job_name, result in job_results.items():
        for pipeline_name in result.pipeline_dependencies.keys():
            pipeline_to_job[pipeline_name] = job_name
    return pipeline_to_job


def analyze_cross_job_dependencies(
    job_results: Dict[str, DependencyAnalysisResult],
    pipeline_to_job: Dict[str, str],
    global_result: DependencyAnalysisResult,
) -> Dict[str, Dict[str, Any]]:
    """Lift pipeline-level edges from ``global_result`` to job-level edges, returning ``{job: {depends_on, pipeline_count}}`` with sorted ``depends_on`` lists."""
    jobs_info: Dict[str, Dict[str, Any]] = {}

    for job_name, job_result in job_results.items():
        depends_on_jobs = set()

        job_pipelines = set(job_result.pipeline_dependencies.keys())

        for pipeline_name in job_pipelines:
            if pipeline_name in global_result.pipeline_dependencies:
                global_pipeline_dep = global_result.pipeline_dependencies[pipeline_name]

                for upstream_pipeline in global_pipeline_dep.depends_on:
                    upstream_job = pipeline_to_job.get(upstream_pipeline)

                    if upstream_job and upstream_job != job_name:
                        depends_on_jobs.add(upstream_job)
                        logger.debug(
                            f"Job '{job_name}' depends on job '{upstream_job}' "
                            f"(pipeline '{pipeline_name}' → '{upstream_pipeline}')"
                        )

        jobs_info[job_name] = {
            "depends_on": sorted(depends_on_jobs),  # Sort for deterministic output
            "pipeline_count": len(job_pipelines),
        }

    return jobs_info

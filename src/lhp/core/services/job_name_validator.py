"""Validator for job_name property in flowgroups."""

import re
import logging
from typing import List
from ...models.config import FlowGroup
from ...utils.error_formatter import LHPError, ErrorCategory

logger = logging.getLogger(__name__)


def validate_job_name_format(job_name: str) -> bool:
    """
    Validate that job_name contains only alphanumeric characters, underscores, and hyphens.
    
    Args:
        job_name: The job name to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not job_name:
        return False
    
    # Pattern: alphanumeric + underscore + hyphen only
    pattern = r'^[a-zA-Z0-9_-]+$'
    return re.match(pattern, job_name) is not None


def validate_job_names(flowgroups: List[FlowGroup]) -> None:
    """
    Validate job_name usage across all flowgroups.
    
    Enforces "all or nothing" rule: if ANY flowgroup has job_name, ALL must have it.
    Also validates job_name format for valid characters.
    
    Args:
        flowgroups: List of flowgroups to validate
        
    Raises:
        LHPError: If validation fails (mixed job_name usage or invalid format)
    """
    if not flowgroups:
        return
    
    # Separate flowgroups with and without job_name
    with_job_name = []
    without_job_name = []
    invalid_format = []
    
    for fg in flowgroups:
        if fg.job_name:
            # Validate format
            if not validate_job_name_format(fg.job_name):
                invalid_format.append((fg.flowgroup, fg.job_name))
            with_job_name.append(fg.flowgroup)
        else:
            without_job_name.append(fg.flowgroup)
    
    # Check for invalid formats first
    if invalid_format:
        invalid_list = "\n".join([
            f"  - {flowgroup}: '{job_name}'" 
            for flowgroup, job_name in invalid_format
        ])
        
        raise LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="001",
            title="Invalid job_name format",
            details=(
                f"Found {len(invalid_format)} flowgroup(s) with invalid job_name format.\n"
                f"job_name must contain only alphanumeric characters, underscores, and hyphens.\n\n"
                f"Invalid job_name(s):\n{invalid_list}"
            ),
            suggestions=[
                "Use only letters (a-z, A-Z), numbers (0-9), underscores (_), and hyphens (-)",
                "Remove spaces and special characters from job_name",
                "Example valid names: 'bronze_job', 'silver-transform', 'gold_layer_1'"
            ],
            context={
                "Total flowgroups": len(flowgroups),
                "Invalid count": len(invalid_format)
            }
        )
    
    # Check for "all or nothing" violation
    if with_job_name and without_job_name:
        with_list = "\n".join([f"  - {fg}" for fg in with_job_name])
        without_list = "\n".join([f"  - {fg}" for fg in without_job_name])
        
        raise LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="002",
            title="Inconsistent job_name usage",
            details=(
                f"Found {len(with_job_name)} flowgroup(s) WITH job_name and "
                f"{len(without_job_name)} WITHOUT job_name.\n\n"
                f"When using job_name, ALL flowgroups must have it defined.\n\n"
                f"Flowgroups WITH job_name:\n{with_list}\n\n"
                f"Flowgroups WITHOUT job_name (missing):\n{without_list}"
            ),
            suggestions=[
                "Add job_name property to all flowgroups that are missing it",
                "Or remove job_name from all flowgroups to use single-job mode",
                "Group related flowgroups by giving them the same job_name value"
            ],
            context={
                "Total flowgroups": len(flowgroups),
                "With job_name": len(with_job_name),
                "Without job_name": len(without_job_name)
            }
        )
    
    # Log validation success
    if with_job_name:
        unique_jobs = set(fg.job_name for fg in flowgroups if fg.job_name)
        logger.info(
            f"job_name validation passed: {len(flowgroups)} flowgroups across "
            f"{len(unique_jobs)} job(s): {', '.join(sorted(unique_jobs))}"
        )
    else:
        logger.debug(f"No job_name defined - using single-job mode for {len(flowgroups)} flowgroups")


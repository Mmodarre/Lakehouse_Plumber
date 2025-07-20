"""
Bundle detection utilities for LHP Databricks Asset Bundle integration.

This module contains the core logic for determining when bundle support
should be enabled based on project structure and CLI flags.
"""

import logging
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def should_enable_bundle_support(
    project_root: Union[Path, str], 
    cli_no_bundle: bool = False
) -> bool:
    """
    Determine if bundle support should be enabled.
    
    Detection priority:
    1. CLI override (--no-bundle) takes highest precedence
    2. databricks.yml file existence determines bundle support
    
    Args:
        project_root: Path to the project root directory
        cli_no_bundle: CLI flag to disable bundle support
        
    Returns:
        True if bundle support should be enabled, False otherwise
        
    Raises:
        TypeError: If project_root is None
    """
    if project_root is None:
        raise TypeError("project_root cannot be None")
    
    # CLI override takes precedence
    if cli_no_bundle:
        logger.debug("Bundle support disabled by --no-bundle CLI flag")
        return False
    
    # Convert string to Path if necessary
    if isinstance(project_root, str):
        project_root = Path(project_root)
    
    # Check for databricks.yml existence
    bundle_enabled = is_databricks_yml_present(project_root)
    
    if bundle_enabled:
        logger.debug(f"Bundle support enabled - databricks.yml found in {project_root}")
    else:
        logger.debug(f"Bundle support disabled - no databricks.yml found in {project_root}")
    
    return bundle_enabled


def is_databricks_yml_present(project_root: Path) -> bool:
    """
    Check if databricks.yml file exists in the project root.
    
    This function only checks for file existence, not content validity.
    It specifically looks for 'databricks.yml' (not .yaml extension).
    
    Args:
        project_root: Path to the project root directory
        
    Returns:
        True if databricks.yml exists as a file, False otherwise
    """
    try:
        databricks_yml = project_root / "databricks.yml"
        
        # Check if file exists and is actually a file (not directory)
        return databricks_yml.exists() and databricks_yml.is_file()
        
    except (OSError, PermissionError) as e:
        # Handle file system errors gracefully
        logger.warning(f"Error checking for databricks.yml in {project_root}: {e}")
        return False
    except Exception as e:
        # Handle any other unexpected errors
        logger.error(f"Unexpected error checking for databricks.yml in {project_root}: {e}")
        return False 
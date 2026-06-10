import logging
from pathlib import Path
from typing import Union

from ..errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


def should_enable_bundle_support(
    project_root: Union[Path, str], cli_no_bundle: bool = False
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
        LHPConfigError: If project_root is None

    :stability: provisional
    """
    if project_root is None:
        raise ErrorFactory.config_error(
            codes.CFG_028,
            title="Invalid project_root argument",
            details="project_root cannot be None when checking bundle support.",
            suggestions=[
                "Ensure project_root is set before calling bundle detection",
                "Run 'lhp init' to create a project structure",
            ],
        )

    if cli_no_bundle:
        logger.debug("Bundle support disabled by --no-bundle CLI flag")
        return False

    if isinstance(project_root, str):
        project_root = Path(project_root)

    bundle_enabled = is_databricks_yml_present(project_root)

    if bundle_enabled:
        logger.debug(f"Bundle support enabled - databricks.yml found in {project_root}")
    else:
        logger.debug(
            f"Bundle support disabled - no databricks.yml found in {project_root}"
        )

    return bundle_enabled


def is_databricks_yml_present(project_root: Path) -> bool:
    """Only checks for 'databricks.yml' (not .yaml extension) and not content validity."""
    try:
        databricks_yml = project_root / "databricks.yml"

        return databricks_yml.exists() and databricks_yml.is_file()

    except (OSError, PermissionError) as e:
        logger.warning(f"Error checking for databricks.yml in {project_root}: {e}")
        return False
    except Exception:
        logger.exception(
            f"Unexpected error checking for databricks.yml in {project_root}"
        )
        return False

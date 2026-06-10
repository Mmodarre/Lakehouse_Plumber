"""Stateless YAML writer for orchestration jobs."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def write_job_yaml(yaml_string: str, output_path: Path) -> Path:
    """Write a job YAML string to disk, creating parent dirs.

    Args:
        yaml_string: Rendered YAML content to write.
        output_path: Fully-resolved file path (not a directory) to write to.

    Returns:
        The resolved output path.

    Raises:
        IOError: If the file cannot be written.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(yaml_string)

        logger.info(f"Generated job file: {output_path}")
        return output_path

    except IOError:
        logger.exception(f"Failed to write job file to {output_path}")
        raise

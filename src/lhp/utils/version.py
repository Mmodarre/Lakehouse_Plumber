"""Version utilities for LakehousePlumber."""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)

try:
    from importlib.metadata import version
except ImportError:
    from importlib_metadata import version


def get_version() -> str:
    """Get the package version dynamically from package metadata."""
    try:
        return version("lakehouse-plumber")
    except Exception:
        try:
            current_dir = Path(__file__).parent
            for _ in range(5):  # Look up to 5 levels
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                current_dir = current_dir.parent
        except Exception as e:
            logger.debug(f"Could not read version from pyproject.toml: {e}")

        # Final fallback
        return "0.4.1"

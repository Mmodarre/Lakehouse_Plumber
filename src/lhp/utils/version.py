"""Version utilities for LakehousePlumber."""

import re
from pathlib import Path

# Import for dynamic version detection
try:
    from importlib.metadata import version
except ImportError:
    # Fallback for Python < 3.8
    from importlib_metadata import version


def get_version() -> str:
    """Get the package version dynamically from package metadata.
    
    Returns:
        Package version string
    """
    try:
        # Try to get version from installed package metadata
        return version("lakehouse-plumber")
    except Exception:
        try:
            # Fallback: try reading from pyproject.toml (for development)
            # Find pyproject.toml - look up the directory tree
            current_dir = Path(__file__).parent
            for _ in range(5):  # Look up to 5 levels
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    # Use regex to find version = "x.y.z"
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                current_dir = current_dir.parent
        except Exception:
            pass

        # Final fallback
        return "0.4.1"

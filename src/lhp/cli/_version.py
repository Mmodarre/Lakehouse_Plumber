"""Version resolution for the LHP CLI.

``get_version()`` reads the installed distribution version via
``importlib.metadata`` and falls back to parsing ``pyproject.toml`` (then a
hard-coded default) when LHP is not installed as a distribution — e.g. running
from a source checkout without an editable install. CLI-layer module: stdlib
only.
"""

from __future__ import annotations

import logging

try:
    from importlib.metadata import version
except ImportError:
    try:
        from importlib_metadata import version
    except Exception:  # pragma: no cover - best-effort fallback

        def version(package: str) -> str:  # type: ignore
            return "0.0.0"


def get_version() -> str:
    try:
        return version("lakehouse-plumber")
    except Exception:
        try:
            import re
            from pathlib import Path

            current_dir = Path(__file__).parent
            for _ in range(5):
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                    break
                current_dir = current_dir.parent
        except Exception as e:
            logging.getLogger(__name__).debug(
                f"Could not read version from pyproject.toml: {e}"
            )

        return "0.2.11"

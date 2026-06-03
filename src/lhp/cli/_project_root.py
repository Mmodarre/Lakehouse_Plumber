"""Project-root discovery for the LHP CLI (locates the dir containing lhp.yaml)."""

from pathlib import Path
from typing import Optional


def _find_project_root() -> Optional[Path]:
    current = Path.cwd().resolve()
    for path in [current, *list(current.parents)]:
        if (path / "lhp.yaml").exists():
            return path
    return None

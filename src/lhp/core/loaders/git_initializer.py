"""Initialize a git repository inside a freshly scaffolded project.

``lhp init --sample`` runs ``git init`` in the new project root so the
sample is a self-contained git repository from the start. This matters in
Databricks workspaces: ``databricks bundle deploy`` shells out to git to
read repo info and walks *up* the directory tree, so a project nested under
a folder whose ``.git`` is owned by another user trips git's "dubious
ownership" guard. A project-local ``.git`` stops that upward walk, removing
the need to whitelist the path via
``git config --global --add safe.directory <path>``.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


def init_git_repository(project_path: Path) -> bool:
    """Run ``git init`` in ``project_path``; return whether a repo was created.

    Non-fatal by contract: the project is already fully scaffolded by the
    time this runs, so a missing ``git`` binary or a non-zero ``git init``
    is logged at WARNING and reported as ``False`` rather than raised. A
    pre-existing ``.git`` is left untouched and also reported as ``False``.
    """
    git_dir = project_path / ".git"
    if git_dir.exists():
        logger.debug(f"Skipping git init: {git_dir} already exists")
        return False

    git_exe = shutil.which("git")
    if git_exe is None:
        logger.warning(
            f"git not found on PATH; skipped initializing a repository in "
            f"{project_path}. Run `git init` there manually for version control."
        )
        return False

    result = subprocess.run(
        [git_exe, "init"],
        cwd=str(project_path),
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        logger.warning(
            f"git init failed in {project_path} (exit {result.returncode}): "
            f"{(result.stderr or '').strip()}"
        )
        return False

    logger.debug(f"Initialized git repository in {project_path}")
    return True

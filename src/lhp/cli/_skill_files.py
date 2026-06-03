"""File-system and version logic for the LHP Claude Code skill.

The skill content ships inside the package at ``lhp.resources.skills.lhp``
and is copied into ``<cwd>/.claude/skills/lhp/`` (or ``~/.claude/skills/lhp/``
with ``--user``) on demand. This module owns the pure I/O and version math:
enumerating, copying, and clearing skill files, reading/writing the install
marker, resolving the target directory, and comparing versions.

It depends only on the standard library, ``importlib.resources`` (the
``Traversable`` API), ``importlib.metadata`` and ``packaging`` — no Rich, no
Click, no ``lhp.errors``. The command layer raises domain errors; the
presenter layer renders. This module decides nothing about exit codes or
output (constitution §9.5 / §9.11).
"""

from __future__ import annotations

import logging
import shutil
from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import List, Literal, Optional

from packaging.version import InvalidVersion
from packaging.version import parse as parse_version

logger = logging.getLogger(__name__)

SKILL_PACKAGE = "lhp.resources.skills.lhp"
SKILL_DIRNAME = "lhp"
MARKER_FILE = ".lhp_skill_version"
EXCLUDED_NAMES = {"__init__.py", "__pycache__"}

VersionComparison = Literal["same", "older", "newer"]


def resolve_install_dir(user: bool) -> Path:
    """Return the skill install directory.

    ``user=True`` targets ``~/.claude/skills/lhp/``; otherwise the directory
    is rooted at the current working directory.
    """
    base = Path.home() if user else Path.cwd()
    return base / ".claude" / "skills" / SKILL_DIRNAME


def current_version() -> str:
    """Return the installed ``lakehouse-plumber`` distribution version.

    Falls back to ``"0.0.0"`` when the package has no installed metadata
    (e.g. a bare source tree), mirroring the CLI's own last-resort default.
    """
    try:
        return str(version("lakehouse-plumber"))
    except PackageNotFoundError:
        logger.debug("lakehouse-plumber metadata not found; using fallback version")
        return "0.0.0"


def is_installed(install_dir: Path) -> bool:
    """Return whether ``install_dir`` already holds a skill installation.

    True when either the version marker or ``SKILL.md`` is present, matching
    the historical "already installed" guard the install command enforces.
    """
    return (install_dir / MARKER_FILE).exists() or (install_dir / "SKILL.md").exists()


def read_marker(install_dir: Path) -> Optional[str]:
    """Return the version recorded in the marker file, or ``None`` if absent."""
    marker_path = install_dir / MARKER_FILE
    if not marker_path.is_file():
        return None
    return marker_path.read_text(encoding="utf-8").strip() or None


def write_marker(install_dir: Path, version_str: str) -> None:
    """Write ``version_str`` to the marker file in ``install_dir``."""
    marker_path = install_dir / MARKER_FILE
    marker_path.write_text(version_str + "\n", encoding="utf-8")


def enumerate_skill_files() -> List[str]:
    """Sorted relative POSIX paths of the packaged skill files.

    Excludes ``__init__.py`` and ``__pycache__`` so only the rendered skill
    content (markdown and references) is reported.
    """
    package_root = files(SKILL_PACKAGE)
    collected: List[str] = []

    def collect(node: Traversable, rel: str = "") -> None:
        for item in node.iterdir():
            name = item.name
            if name in EXCLUDED_NAMES:
                continue
            child_rel = f"{rel}/{name}" if rel else name
            if item.is_dir():
                collect(item, child_rel)
            elif item.is_file():
                collected.append(child_rel)

    collect(package_root)
    collected.sort()
    return collected


def copy_skill_files(install_dir: Path) -> None:
    """Copy every packaged skill file into ``install_dir``.

    Creates ``install_dir`` and any intermediate directories. Existing files
    at the destination are overwritten.
    """
    install_dir.mkdir(parents=True, exist_ok=True)
    package_root = files(SKILL_PACKAGE)

    for rel in enumerate_skill_files():
        target = install_dir / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        source = package_root
        for part in rel.split("/"):
            source = source / part
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
        logger.debug(f"Wrote skill file: {target}")


def clear_install_dir(install_dir: Path) -> None:
    """Remove every child of ``install_dir`` (file or subtree); keep the dir.

    No-op when the directory does not exist.
    """
    if not install_dir.exists():
        return
    for child in install_dir.iterdir():
        if child.is_dir():
            shutil.rmtree(child)
        else:
            child.unlink()


def remove_install_dir(install_dir: Path) -> None:
    """Recursively remove ``install_dir`` and all of its contents."""
    shutil.rmtree(install_dir)


def extra_files(install_dir: Path) -> List[str]:
    """Files present in ``install_dir`` but not in the packaged source.

    Excludes the marker file (``update`` rewrites it). Returns an empty list
    when the install directory does not exist.
    """
    if not install_dir.exists():
        return []

    expected = set(enumerate_skill_files())
    extras: List[str] = []
    for path in install_dir.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(install_dir).as_posix()
        if rel == MARKER_FILE:
            continue
        if rel not in expected:
            extras.append(rel)
    extras.sort()
    return extras


def compare_versions(installed: str, current: str) -> VersionComparison:
    """Compare two version strings.

    Returns ``"same"``, ``"older"`` (installed predates current) or
    ``"newer"`` (installed exceeds current). Falls back to string equality
    when either value is not a valid PEP 440 version.
    """
    try:
        installed_v = parse_version(installed)
        current_v = parse_version(current)
    except InvalidVersion:
        if installed == current:
            return "same"
        return "older"

    if installed_v == current_v:
        return "same"
    if installed_v < current_v:
        return "older"
    return "newer"

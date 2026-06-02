"""Skill command implementation for LakehousePlumber CLI.

Manages installation, update, and removal of the LHP Claude Code skill.
The skill content lives in the package at ``lhp.resources.skills.lhp`` and
is copied into ``<cwd>/.claude/skills/lhp/`` (or ``~/.claude/skills/lhp/``
with ``--user``) on demand.
"""

import logging
import shutil
from importlib.metadata import version
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import List, Literal, Optional

import click
from packaging.version import InvalidVersion
from packaging.version import parse as parse_version
from rich.text import Text

from .. import console as _console_module
from ..render import render_command_header
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


SKILL_PACKAGE = "lhp.resources.skills.lhp"
SKILL_DIRNAME = "lhp"
MARKER_FILE = ".lhp_skill_version"
EXCLUDED_NAMES = {"__init__.py", "__pycache__"}


class SkillCommand(BaseCommand):
    """Handles install/update/status/uninstall of the LHP Claude Code skill."""

    def install(self, user: bool, force: bool) -> None:
        """Install the skill. ``user=True`` targets ``~/.claude/skills/lhp/``."""
        render_command_header("lhp skill install")
        self.setup_from_context()

        install_dir = self._resolve_install_dir(user)
        marker_path = install_dir / MARKER_FILE
        skill_md_path = install_dir / "SKILL.md"

        if not force and (marker_path.exists() or skill_md_path.exists()):
            from ...errors import ErrorFactory, codes

            raise ErrorFactory.io_error(
                codes.IO_020,
                title="LHP skill already installed",
                details=(
                    f"A skill installation already exists at {install_dir}. "
                    "Use --force to overwrite or run `lhp skill update` to "
                    "refresh from the current LHP version."
                ),
                suggestions=[
                    "Run `lhp skill update` to refresh the install",
                    "Run `lhp skill install --force` to overwrite",
                    "Run `lhp skill uninstall` first, then re-install",
                ],
                context={"Install dir": str(install_dir)},
            )

        current_version = self._current_version()
        logger.info(f"Installing LHP skill v{current_version} to {install_dir}")

        self._copy_skill_files(install_dir)
        self._write_marker(install_dir, current_version)

        _console_module.console.print(
            Text.assemble(
                ("✓ ", "bold green"),
                f"Installed LHP skill v{current_version} to {install_dir}",
            )
        )
        _console_module.console.print(
            "Reload your Claude Code session to pick up the skill."
        )

    def update(self, user: bool, yes: bool) -> None:
        """Update an existing install to the current LHP version.

        ``yes`` skips the downgrade-confirmation prompt.
        """
        render_command_header("lhp skill update")
        self.setup_from_context()

        install_dir = self._resolve_install_dir(user)
        installed_version = self._read_marker(install_dir)

        if installed_version is None:
            from ...errors import ErrorFactory, codes

            raise ErrorFactory.io_error(
                codes.IO_021,
                title="LHP skill is not installed",
                details=(
                    f"No marker file found at {install_dir / MARKER_FILE}. "
                    "Use `lhp skill install` for a fresh install."
                ),
                suggestions=[
                    "Run `lhp skill install` to install the skill",
                    "Run `lhp skill install --force` to take over a foreign install",
                ],
                context={"Install dir": str(install_dir)},
            )

        current_version = self._current_version()
        comparison = self._compare_versions(installed_version, current_version)

        if comparison == "newer" and not yes:
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    f"Installed skill v{installed_version} is newer than "
                    f"the current LHP version v{current_version}.",
                )
            )
            if not click.confirm("Continue and downgrade the skill?", default=False):
                _console_module.console.print("Aborted.")
                return

        logger.info(
            f"Updating LHP skill: {installed_version} -> {current_version} "
            f"at {install_dir}"
        )

        # Wipe and re-copy: simpler than computing a diff.
        self._clear_install_dir(install_dir)
        self._copy_skill_files(install_dir)
        self._write_marker(install_dir, current_version)

        if comparison == "same":
            _console_module.console.print(
                Text.assemble(
                    ("✓ ", "bold green"),
                    f"LHP skill is already at v{current_version}; "
                    f"refreshed files at {install_dir}",
                )
            )
        elif comparison == "older":
            _console_module.console.print(
                Text.assemble(
                    ("✓ ", "bold green"),
                    f"Updated LHP skill: v{installed_version} -> "
                    f"v{current_version} at {install_dir}",
                )
            )
        else:
            _console_module.console.print(
                Text.assemble(
                    ("✓ ", "bold green"),
                    f"Replaced LHP skill v{installed_version} with "
                    f"v{current_version} at {install_dir}",
                )
            )

    def status(self, user: bool) -> None:
        """Print the install state of the skill."""
        render_command_header("lhp skill status")
        self.setup_from_context()

        install_dir = self._resolve_install_dir(user)
        current_version = self._current_version()

        _console_module.console.print(
            Text.assemble(("Install location: ", "dim"), str(install_dir))
        )
        _console_module.console.print(
            Text.assemble(("Current LHP version: ", "dim"), f"v{current_version}")
        )

        if not install_dir.exists():
            _console_module.console.print(
                Text.assemble(
                    ("✗ ", "bold red"),
                    "Not installed. Run `lhp skill install`.",
                )
            )
            return

        installed_version = self._read_marker(install_dir)

        if installed_version is None:
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    "Foreign install detected (no marker file). "
                    "Run `lhp skill install --force` to take over.",
                )
            )
            return

        comparison = self._compare_versions(installed_version, current_version)

        if comparison == "same":
            _console_module.console.print(
                Text.assemble(
                    ("✓ ", "bold green"),
                    f"v{installed_version} (up-to-date)",
                )
            )
        elif comparison == "older":
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    f"Update available: v{installed_version} -> "
                    f"v{current_version}. Run `lhp skill update`.",
                )
            )
        else:
            _console_module.err_console.print(
                Text.assemble(
                    ("⚠ ", "bold yellow"),
                    f"Installed v{installed_version} is newer than "
                    f"CLI v{current_version}. "
                    f"Run `pip install -U lakehouse-plumber`.",
                )
            )

        extras = self._extra_files(install_dir)
        if extras:
            _console_module.console.print("")
            _console_module.console.print(
                "Extra files (will be removed by `lhp skill update`):"
            )
            for rel in extras:
                _console_module.console.print(f"  {rel}")

    def uninstall(self, user: bool, force: bool) -> None:
        """Remove the skill install. ``force`` skips the confirmation prompt."""
        render_command_header("lhp skill uninstall")
        self.setup_from_context()

        install_dir = self._resolve_install_dir(user)

        if not install_dir.exists():
            _console_module.console.print(
                f"Nothing to remove: {install_dir} does not exist."
            )
            return

        if not force:
            if not click.confirm(
                f"Remove LHP skill from {install_dir}?", default=False
            ):
                _console_module.console.print("Aborted.")
                return

        shutil.rmtree(install_dir)
        _console_module.console.print(
            Text.assemble(
                ("✓ ", "bold green"),
                f"Removed LHP skill from {install_dir}",
            )
        )

    def _resolve_install_dir(self, user: bool) -> Path:
        base = Path.home() if user else Path.cwd()
        return base / ".claude" / "skills" / SKILL_DIRNAME

    def _current_version(self) -> str:
        try:
            return str(version("lakehouse-plumber"))
        except Exception:
            # Editable installs without metadata fall back to the main
            # module's version helper for symmetry with --version.
            from ..main import get_version

            return str(get_version())

    def _read_marker(self, install_dir: Path) -> Optional[str]:
        marker_path = install_dir / MARKER_FILE
        if not marker_path.is_file():
            return None
        return marker_path.read_text(encoding="utf-8").strip() or None

    def _write_marker(self, install_dir: Path, version_str: str) -> None:
        marker_path = install_dir / MARKER_FILE
        marker_path.write_text(version_str + "\n", encoding="utf-8")

    def _enumerate_skill_files(self) -> List[str]:
        """Sorted relative POSIX paths under the skill package.

        Excludes ``__init__.py`` and ``__pycache__`` so the rendered skill
        contains only the markdown content.
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

    def _copy_skill_files(self, install_dir: Path) -> None:
        install_dir.mkdir(parents=True, exist_ok=True)
        package_root = files(SKILL_PACKAGE)

        for rel in self._enumerate_skill_files():
            target = install_dir / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            source = package_root
            for part in rel.split("/"):
                source = source / part
            target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
            logger.debug(f"Wrote skill file: {target}")

    def _clear_install_dir(self, install_dir: Path) -> None:
        if not install_dir.exists():
            return
        for child in install_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child)
            else:
                child.unlink()

    def _extra_files(self, install_dir: Path) -> List[str]:
        """Paths present in install_dir but not in current source.

        Excludes the marker file (``update`` rewrites it).
        """
        if not install_dir.exists():
            return []

        expected = set(self._enumerate_skill_files())
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

    def _compare_versions(
        self, installed: str, current: str
    ) -> Literal["same", "older", "newer"]:
        """Falls back to string equality if either version can't be parsed."""
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

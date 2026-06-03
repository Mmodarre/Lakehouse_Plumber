"""Rich rendering for the ``lhp skill`` command family.

Pure presentation: each function takes already-resolved primitives
(directories, version strings, comparison verdicts, file lists) and writes
to the shared CLI consoles. Success and data lines go to ``console``;
advisory or warning lines go to ``err_console``. This module never decides
exit codes and never imports ``lhp.errors`` (constitution §5.2 / §9.5).
"""

from __future__ import annotations

from pathlib import Path
from typing import Sequence

from rich.text import Text

from .. import console as _console_module


def render_installed(install_dir: Path, version_str: str) -> None:
    """Confirm a fresh install of the skill at ``install_dir``."""
    _console_module.console.print(
        Text.assemble(
            ("✓ ", "bold green"),
            f"Installed LHP skill v{version_str} to {install_dir}",
        )
    )
    _console_module.console.print(
        "Reload your Claude Code session to pick up the skill."
    )


def render_downgrade_warning(installed: str, current: str) -> None:
    """Warn that the installed skill is newer than the current LHP version."""
    _console_module.err_console.print(
        Text.assemble(
            ("⚠ ", "bold yellow"),
            f"Installed skill v{installed} is newer than "
            f"the current LHP version v{current}.",
        )
    )


def render_aborted() -> None:
    """Report that the user declined a confirmation prompt."""
    _console_module.console.print("Aborted.")


def render_updated(
    install_dir: Path,
    installed: str,
    current: str,
    comparison: str,
) -> None:
    """Confirm the result of an update, phrased by ``comparison``.

    ``comparison`` is one of ``"same"`` (refresh), ``"older"`` (upgrade) or
    ``"newer"`` (downgrade); any other value falls back to the downgrade
    wording.
    """
    if comparison == "same":
        message = (
            f"LHP skill is already at v{current}; refreshed files at {install_dir}"
        )
    elif comparison == "older":
        message = f"Updated LHP skill: v{installed} -> v{current} at {install_dir}"
    else:
        message = f"Replaced LHP skill v{installed} with v{current} at {install_dir}"

    _console_module.console.print(Text.assemble(("✓ ", "bold green"), message))


def render_status(
    install_dir: Path,
    current: str,
    *,
    installed: str | None,
    is_installed: bool,
    comparison: str | None,
    extras: Sequence[str],
) -> None:
    """Render the full ``skill status`` report.

    ``is_installed`` is the directory-exists check; ``installed`` is the
    marker version (``None`` when the directory exists but carries no marker,
    i.e. a foreign install). ``comparison`` is the verdict from
    ``_skill_files.compare_versions`` and is consulted only when a marker
    version is present.
    """
    _console_module.console.print(
        Text.assemble(("Install location: ", "dim"), str(install_dir))
    )
    _console_module.console.print(
        Text.assemble(("Current LHP version: ", "dim"), f"v{current}")
    )

    if not is_installed:
        _console_module.console.print(
            Text.assemble(
                ("✗ ", "bold red"),
                "Not installed. Run `lhp skill install`.",
            )
        )
        return

    if installed is None:
        _console_module.err_console.print(
            Text.assemble(
                ("⚠ ", "bold yellow"),
                "Foreign install detected (no marker file). "
                "Run `lhp skill install --force` to take over.",
            )
        )
        return

    if comparison == "same":
        _console_module.console.print(
            Text.assemble(("✓ ", "bold green"), f"v{installed} (up-to-date)")
        )
    elif comparison == "older":
        _console_module.err_console.print(
            Text.assemble(
                ("⚠ ", "bold yellow"),
                f"Update available: v{installed} -> v{current}. "
                f"Run `lhp skill update`.",
            )
        )
    else:
        _console_module.err_console.print(
            Text.assemble(
                ("⚠ ", "bold yellow"),
                f"Installed v{installed} is newer than CLI v{current}. "
                f"Run `pip install -U lakehouse-plumber`.",
            )
        )

    if extras:
        _console_module.console.print("")
        _console_module.console.print(
            "Extra files (will be removed by `lhp skill update`):"
        )
        for rel in extras:
            _console_module.console.print(f"  {rel}")


def render_nothing_to_uninstall(install_dir: Path) -> None:
    """Report that there is no install directory to remove."""
    _console_module.console.print(f"Nothing to remove: {install_dir} does not exist.")


def render_uninstalled(install_dir: Path) -> None:
    """Confirm removal of the skill install at ``install_dir``."""
    _console_module.console.print(
        Text.assemble(("✓ ", "bold green"), f"Removed LHP skill from {install_dir}")
    )

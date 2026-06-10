"""``lhp skill`` command group: install/update/status/uninstall.

Thin Click wiring (§9.11): file I/O in ``cli/_skill_files`` and
``cli/_claude_setup``, Rich output in ``cli/presenters/skill_presenter``;
domain failures raise ``LHPError``.
"""

import logging
from pathlib import Path

import click
from rich_click import RichGroup

from lhp.errors import ErrorFactory, codes

from .. import _claude_setup as cs
from .. import _skill_files as sf
from ..error_boundary import cli_error_boundary
from ..presenters import skill_presenter as view

logger = logging.getLogger(__name__)

_user_option = click.option("--user", is_flag=True, help="Target ~/.claude/ not cwd.")


def _sync_routing_block(user: bool) -> "cs.WriteStatus | None":
    """Write the project ``CLAUDE.md`` routing block for a project install.

    A ``--user`` install is global and has no single project ``CLAUDE.md`` to
    own, so it is skipped (returns ``None``). Project installs target the cwd —
    the same root ``resolve_install_dir(user=False)`` uses for the skill files.
    """
    if user:
        return None
    return cs.write_routing_block(Path.cwd())


@click.group(cls=RichGroup, name="skill")
def skill() -> None:
    """Manage the LHP Claude Code skill."""


@skill.command(name="install")
@_user_option
@click.option("--force", is_flag=True, help="Overwrite an existing install.")
@cli_error_boundary("skill install")
def install(user: bool, force: bool) -> None:
    """Install the skill into the Claude config."""
    install_dir = sf.resolve_install_dir(user)
    if not force and sf.is_installed(install_dir):
        raise ErrorFactory.io_error(
            codes.IO_020,
            title="LHP skill already installed",
            details=f"A skill installation already exists at {install_dir}.",
            suggestions=["Run `lhp skill install --force` to overwrite or update"],
        )
    version_str = sf.current_version()
    logger.info(f"Installing LHP skill v{version_str} to {install_dir}")
    sf.copy_skill_files(install_dir)
    sf.write_marker(install_dir, version_str)
    routing = _sync_routing_block(user)
    view.render_installed(install_dir, version_str, routing=routing)


@skill.command(name="update")
@_user_option
@click.option("--yes", is_flag=True, help="Skip the downgrade prompt.")
@cli_error_boundary("skill update")
def update(user: bool, yes: bool) -> None:
    """Refresh an install to the current LHP version."""
    install_dir = sf.resolve_install_dir(user)
    installed = sf.read_marker(install_dir)
    if installed is None:
        raise ErrorFactory.io_error(
            codes.IO_021,
            title="LHP skill is not installed",
            details=f"No marker file found at {install_dir / sf.MARKER_FILE}.",
            suggestions=["Run `lhp skill install` to install the skill"],
        )
    current = sf.current_version()
    comparison = sf.compare_versions(installed, current)
    if comparison == "newer" and not yes:
        view.render_downgrade_warning(installed, current)
        if not click.confirm("Continue and downgrade the skill?", default=False):
            view.render_aborted()
            return
    logger.info(f"Updating LHP skill: {installed} -> {current} at {install_dir}")
    sf.clear_install_dir(install_dir)
    sf.copy_skill_files(install_dir)
    sf.write_marker(install_dir, current)
    routing = _sync_routing_block(user)
    view.render_updated(install_dir, installed, current, comparison, routing=routing)


@skill.command(name="status")
@_user_option
@cli_error_boundary("skill status")
def status(user: bool) -> None:
    """Report the install state of the skill."""
    install_dir = sf.resolve_install_dir(user)
    installed = sf.read_marker(install_dir)
    current = sf.current_version()
    comparison = sf.compare_versions(installed, current) if installed else None
    view.render_status(
        install_dir,
        current,
        installed=installed,
        is_installed=install_dir.exists(),
        comparison=comparison,
        extras=sf.extra_files(install_dir),
    )


@skill.command(name="uninstall")
@_user_option
@click.option("--force", is_flag=True, help="Skip the confirmation prompt.")
@cli_error_boundary("skill uninstall")
def uninstall(user: bool, force: bool) -> None:
    """Remove the skill install."""
    install_dir = sf.resolve_install_dir(user)
    if not install_dir.exists():
        view.render_nothing_to_uninstall(install_dir)
        return
    if not force and not click.confirm(
        f"Remove LHP skill from {install_dir}?", default=False
    ):
        view.render_aborted()
        return
    sf.remove_install_dir(install_dir)
    routing = None if user else cs.remove_routing_block(Path.cwd())
    view.render_uninstalled(install_dir, routing=routing)

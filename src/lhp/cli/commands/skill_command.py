"""``lhp skill`` command group: install/update/status/uninstall.

Thin Click wiring (§9.11): project installs go through the public
:class:`lhp.api.SkillFacade`; ``--user`` installs and the read-only
subcommands use the file/version primitives in ``lhp.api._skill_assets``
and ``lhp.api._claude_routing`` (``cli -> api`` is the sanctioned
bridge). Rich output in ``cli/presenters/skill_presenter``; domain
failures raise ``LHPError``.
"""

import logging
from pathlib import Path

import click
from rich_click import RichGroup

from lhp.api import _claude_routing as cr
from lhp.api import _skill_assets as sa
from lhp.errors import ErrorFactory, codes

from .._app_context import resolve_project_root
from ..error_boundary import cli_error_boundary
from ..presenters import skill_presenter as view

logger = logging.getLogger(__name__)

_user_option = click.option("--user", is_flag=True, help="Target ~/.claude/ not cwd.")


@click.group(cls=RichGroup, name="skill")
def skill() -> None:
    """Manage the LHP Claude Code skill."""


@skill.command(name="install")
@_user_option
@click.option("--force", is_flag=True, help="Overwrite an existing install.")
@cli_error_boundary("skill install")
def install(user: bool, force: bool) -> None:
    """Install the skill into the Claude config."""
    if not user:
        from lhp.api import SkillFacade

        project_root = resolve_project_root()
        result = SkillFacade(project_root).install_project_skill(force=force)
        view.render_install_result(result)
        return

    install_dir = sa.skill_install_dir(Path.home())
    if not force and sa.is_installed(install_dir):
        raise ErrorFactory.io_error(
            codes.IO_020,
            title="LHP skill already installed",
            details=f"A skill installation already exists at {install_dir}.",
            suggestions=["Run `lhp skill install --force` to overwrite or update"],
        )
    version_str = sa.current_version()
    logger.info(f"Installing LHP skill v{version_str} to {install_dir}")
    sa.copy_skill_files(install_dir)
    sa.write_marker(install_dir, version_str)
    view.render_installed(install_dir, version_str, routing=None)


@skill.command(name="update")
@_user_option
@click.option("--yes", is_flag=True, help="Skip the downgrade prompt.")
@cli_error_boundary("skill update")
def update(user: bool, yes: bool) -> None:
    """Refresh an install to the current LHP version."""
    project_root = None if user else resolve_project_root()
    base = Path.home() if project_root is None else project_root
    install_dir = sa.skill_install_dir(base)
    installed = sa.read_marker(install_dir)
    if installed is None:
        raise ErrorFactory.io_error(
            codes.IO_021,
            title="LHP skill is not installed",
            details=f"No marker file found at {install_dir / sa.MARKER_FILE}.",
            suggestions=["Run `lhp skill install` to install the skill"],
        )
    current = sa.current_version()
    comparison = sa.compare_versions(installed, current)
    if comparison == "newer" and not yes:
        view.render_downgrade_warning(installed, current)
        if not click.confirm("Continue and downgrade the skill?", default=False):
            view.render_aborted()
            return
    logger.info(f"Updating LHP skill: {installed} -> {current} at {install_dir}")
    if project_root is None:
        sa.clear_install_dir(install_dir)
        sa.copy_skill_files(install_dir)
        sa.write_marker(install_dir, current)
        routing = None
    else:
        from lhp.api import SkillFacade

        result = SkillFacade(project_root).install_project_skill(force=True)
        routing = result.routing_block_status
    view.render_updated(install_dir, installed, current, comparison, routing=routing)


@skill.command(name="status")
@_user_option
@cli_error_boundary("skill status")
def status(user: bool) -> None:
    """Report the install state of the skill."""
    install_dir = sa.skill_install_dir(Path.home() if user else Path.cwd())
    installed = sa.read_marker(install_dir)
    current = sa.current_version()
    comparison = sa.compare_versions(installed, current) if installed else None
    view.render_status(
        install_dir,
        current,
        installed=installed,
        is_installed=install_dir.exists(),
        comparison=comparison,
        extras=sa.extra_files(install_dir),
    )


@skill.command(name="uninstall")
@_user_option
@click.option("--force", is_flag=True, help="Skip the confirmation prompt.")
@cli_error_boundary("skill uninstall")
def uninstall(user: bool, force: bool) -> None:
    """Remove the skill install."""
    install_dir = sa.skill_install_dir(Path.home() if user else Path.cwd())
    if not install_dir.exists():
        view.render_nothing_to_uninstall(install_dir)
        return
    if not force and not click.confirm(
        f"Remove LHP skill from {install_dir}?", default=False
    ):
        view.render_aborted()
        return
    sa.remove_install_dir(install_dir)
    routing = None if user else cr.remove_routing_block(Path.cwd())
    view.render_uninstalled(install_dir, routing=routing)

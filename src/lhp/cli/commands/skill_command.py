"""``lhp skill`` command group: install/export/update/status/uninstall.

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

_name_option = click.option(
    "--name",
    "skill_name",
    type=click.Choice(["lhp", "lhp-learn"]),
    default="lhp",
    show_default=True,
    help="Skill to install or manage.",
)

_user_option = click.option("--user", is_flag=True, help="Target ~/.claude/ not cwd.")


@click.group(cls=RichGroup, name="skill")
def skill() -> None:
    """Install or export the bundled LHP agent skill."""


@skill.command(name="install")
@_name_option
@_user_option
@click.option("--force", is_flag=True, help="Overwrite an existing install.")
@click.option(
    "--agent",
    type=click.Choice(["claude", "genie"]),
    default="claude",
    show_default=True,
)
@cli_error_boundary("skill install")
def install(user: bool, force: bool, agent: str, skill_name: str) -> None:
    """Install into Claude config or the authenticated Genie workspace."""
    if agent == "genie":
        from lhp.api import SkillFacade

        if user or force:
            raise click.UsageError(
                "Genie installs are always user-scoped and never force-overwrite. Omit --user/--force."
            )
        result = SkillFacade().install_genie_skill(skill_name=skill_name)
        view.render_export_result(result, genie=True)
        return
    if not user:
        from lhp.api import SkillFacade

        project_root = resolve_project_root()
        result = SkillFacade(project_root).install_project_skill(
            force=force, skill_name=skill_name
        )
        view.render_install_result(result)
        return

    install_dir = sa.skill_install_dir(Path.home(), skill_name)
    if not force and sa.is_installed(install_dir):
        raise ErrorFactory.io_error(
            codes.IO_020,
            title="LHP skill already installed",
            details=f"A skill installation already exists at {install_dir}.",
            suggestions=[
                f"Run `lhp skill install --name {skill_name} --force` to overwrite or update"
            ],
        )
    version_str = sa.current_version()
    logger.info(f"Installing LHP skill v{version_str} to {install_dir}")
    if force and install_dir.exists():
        sa.clear_install_dir(install_dir)
    sa.copy_skill_files(install_dir, skill_name)
    sa.write_marker(install_dir, version_str)
    view.render_installed(install_dir, version_str, routing=None)


@skill.command(name="export")
@_name_option
@click.option(
    "--output",
    type=click.Path(path_type=Path),
    required=True,
    help="Exact destination skill directory (including lhp or lhp-learn).",
)
@cli_error_boundary("skill export")
def export(output: Path, skill_name: str) -> None:
    """Export for any agent; preserve existing files that differ."""
    from lhp.api import SkillFacade

    view.render_export_result(SkillFacade().export_skill(output, skill_name=skill_name))


@skill.command(name="update")
@_name_option
@_user_option
@click.option("--yes", is_flag=True, help="Skip the downgrade prompt.")
@cli_error_boundary("skill update")
def update(user: bool, yes: bool, skill_name: str) -> None:
    """Refresh an install to the current LHP version."""
    project_root = None if user else resolve_project_root()
    base = Path.home() if project_root is None else project_root
    install_dir = sa.skill_install_dir(base, skill_name)
    installed = sa.read_marker(install_dir)
    if installed is None:
        raise ErrorFactory.io_error(
            codes.IO_021,
            title="LHP skill is not installed",
            details=f"No marker file found at {install_dir / sa.MARKER_FILE}.",
            suggestions=[
                f"Run `lhp skill install --name {skill_name}` to install the skill"
            ],
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
        sa.copy_skill_files(install_dir, skill_name)
        sa.write_marker(install_dir, current)
        routing = None
    else:
        from lhp.api import SkillFacade

        result = SkillFacade(project_root).install_project_skill(
            force=True, skill_name=skill_name
        )
        routing = result.routing_block_status
    view.render_updated(install_dir, installed, current, comparison, routing=routing)


@skill.command(name="status")
@_name_option
@_user_option
@cli_error_boundary("skill status")
def status(user: bool, skill_name: str) -> None:
    """Report the install state of the skill."""
    install_dir = sa.skill_install_dir(Path.home() if user else Path.cwd(), skill_name)
    installed = sa.read_marker(install_dir)
    current = sa.current_version()
    comparison = sa.compare_versions(installed, current) if installed else None
    view.render_status(
        install_dir,
        current,
        installed=installed,
        is_installed=install_dir.exists(),
        comparison=comparison,
        extras=sa.extra_files(install_dir, skill_name),
        skill_name=skill_name,
    )


@skill.command(name="uninstall")
@_name_option
@_user_option
@click.option("--force", is_flag=True, help="Skip the confirmation prompt.")
@cli_error_boundary("skill uninstall")
def uninstall(user: bool, force: bool, skill_name: str) -> None:
    """Remove the skill install."""
    install_dir = sa.skill_install_dir(Path.home() if user else Path.cwd(), skill_name)
    if not install_dir.exists():
        view.render_nothing_to_uninstall(install_dir)
        return
    if not force and not click.confirm(
        f"Remove LHP skill from {install_dir}?", default=False
    ):
        view.render_aborted()
        return
    sa.remove_install_dir(install_dir)
    routing = (
        cr.remove_routing_block(Path.cwd())
        if not user and skill_name == "lhp"
        else None
    )
    view.render_uninstalled(install_dir, routing=routing)

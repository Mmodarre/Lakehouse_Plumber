"""Private module — implementation of the public :class:`SkillFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`SkillFacade` from :mod:`lhp.api`. Deliberately
standalone — NOT a sub-facade on
:class:`~lhp.api.facade.LakehousePlumberApplicationFacade`, whose
composition is locked (§1.11): installing the skill needs only a
project root on disk, never the orchestrator, so constructing the full
service graph for it would be waste. Wraps the file/version primitives
in :mod:`lhp.api._skill_assets` and the ``CLAUDE.md`` routing-block
writer in :mod:`lhp.api._claude_routing`, and converts their primitives
into the frozen public DTO (:class:`SkillInstallResult`).

:stability: internal
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Literal

from lhp.api._claude_routing import write_routing_block
from lhp.api._skill_assets import (
    clear_install_dir,
    copy_skill_files,
    current_version,
    enumerate_skill_files,
    is_installed,
    read_marker,
    skill_install_dir,
    write_marker,
)
from lhp.api.responses import SkillInstallResult
from lhp.errors import ErrorFactory, codes

logger = logging.getLogger(__name__)


class SkillFacade:
    """Claude Code skill operations on an existing LHP project.

    Construct directly with the project root — this is the canonical
    construction path (§4.5); there is no factory because no service
    graph is wired. The root must contain ``lhp.yaml``: the facade
    validates this eagerly at construction so misuse fails at the same
    point :meth:`~lhp.api.facade.LakehousePlumberApplicationFacade.for_project`
    would, not at first operation.

    :stability: provisional
    :raises lhp.errors.LHPError: ``LHP-CFG-011`` when ``project_root``
        does not contain an ``lhp.yaml`` file (not an LHP project).
    """

    def __init__(self, project_root: Path) -> None:
        if not (project_root / "lhp.yaml").is_file():
            raise ErrorFactory.config_error(
                codes.CFG_011,
                title="Not a LakehousePlumber project directory",
                details=f"No lhp.yaml file found at {project_root}.",
                suggestions=[
                    "Run 'lhp init <project_name>' to create a new project",
                    "Point SkillFacade at a directory containing lhp.yaml",
                ],
            )
        self._project_root = project_root

    def install_project_skill(self, *, force: bool = False) -> SkillInstallResult:
        """Install (or force-refresh) the LHP skill into the project.

        Copies the packaged skill files into
        ``<project_root>/.claude/skills/lhp/``, writes the
        ``.lhp_skill_version`` marker, and creates or refreshes the
        ``CLAUDE.md`` routing block. When an installation already
        exists and ``force`` is set, the install directory is cleared
        first so stale or extra files never survive the refresh; the
        returned ``action`` is ``"updated"`` and ``previous_version``
        carries the replaced marker version (``None`` for a foreign
        install without a marker).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-IO-020`` when a skill
            installation already exists and ``force`` is not set.
        """
        install_dir = skill_install_dir(self._project_root)
        already_installed = is_installed(install_dir)
        if already_installed and not force:
            raise ErrorFactory.io_error(
                codes.IO_020,
                title="LHP skill already installed",
                details=f"A skill installation already exists at {install_dir}.",
                suggestions=["Run `lhp skill install --force` to overwrite or update"],
            )

        previous_version = read_marker(install_dir)
        version_str = current_version()
        action: Literal["installed", "updated"] = (
            "updated" if already_installed else "installed"
        )
        logger.info(f"Installing LHP skill v{version_str} to {install_dir}")

        if already_installed:
            clear_install_dir(install_dir)
        copy_skill_files(install_dir)
        write_marker(install_dir, version_str)
        routing_status = write_routing_block(self._project_root)

        return SkillInstallResult(
            install_dir=install_dir,
            skill_version=version_str,
            previous_version=previous_version,
            action=action,
            installed_files=tuple(enumerate_skill_files()),
            routing_block_status=routing_status,
        )

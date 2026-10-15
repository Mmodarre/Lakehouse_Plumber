"""Private module — implementation of the public :class:`SkillFacade`.

Underscore-prefixed: not part of the import surface; external callers
MUST import :class:`SkillFacade` from :mod:`lhp.api`. Deliberately
standalone — NOT a sub-facade on
:class:`~lhp.api.facade.LakehousePlumberApplicationFacade`, whose
composition is locked (§1.11): skill installation uses packaged resources
and a local or workspace destination, never the orchestrator. Wraps the file/version primitives
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
from lhp.errors import ErrorFactory, LHPError, codes

logger = logging.getLogger(__name__)


class SkillFacade:
    """Install bundled skills locally or in the authenticated Genie workspace.

    Construct with no arguments for portable export or Genie installation.
    Pass a project root for the legacy Claude project integration. A supplied
    root must contain ``lhp.yaml`` and is checked eagerly. No service graph
    is needed for these package-resource operations.

    :stability: provisional
    :raises lhp.errors.LHPError: ``LHP-CFG-011`` when ``project_root``
        does not contain an ``lhp.yaml`` file (not an LHP project).
    """

    def __init__(self, project_root: Path | None = None) -> None:
        if project_root is not None and not (project_root / "lhp.yaml").is_file():
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

    def install_project_skill(
        self, *, force: bool = False, skill_name: str = "lhp"
    ) -> SkillInstallResult:
        """Install (or force-refresh) the LHP skill into the project.

        Select ``lhp`` (default) or ``lhp-learn``. Teaching installs write no
        project-wide routing block. Unknown names raise ``ValueError``.

        Copies the packaged skill files into
        ``<project_root>/.claude/skills/<skill_name>/``, writes the
        ``.lhp_skill_version`` marker, and for the development skill creates
        or refreshes the ``CLAUDE.md`` routing block. When an installation already
        exists and ``force`` is set, the install directory is cleared
        first so stale or extra files never survive the refresh; the
        returned ``action`` is ``"updated"`` and ``previous_version``
        carries the replaced marker version (``None`` for a foreign
        install without a marker).

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-IO-020`` when a skill
            installation already exists and ``force`` is not set.
        """
        if self._project_root is None:
            raise ErrorFactory.config_error(
                codes.CFG_011,
                title="Project skill installation requires a project root",
                details="Construct SkillFacade(project_root) for a Claude project install.",
                suggestions=[
                    "Use export_skill(destination) or install_genie_skill() before scaffolding."
                ],
            )
        install_dir = skill_install_dir(self._project_root, skill_name)
        already_installed = is_installed(install_dir)
        if already_installed and not force:
            raise ErrorFactory.io_error(
                codes.IO_020,
                title="LHP skill already installed",
                details=f"A skill installation already exists at {install_dir}.",
                suggestions=[
                    f"Run `lhp skill install --name {skill_name} --force` to overwrite or update"
                ],
            )

        previous_version = read_marker(install_dir)
        version_str = current_version()
        action: Literal["installed", "updated"] = (
            "updated" if already_installed else "installed"
        )
        logger.info(f"Installing LHP skill v{version_str} to {install_dir}")

        if already_installed:
            clear_install_dir(install_dir)
        copy_skill_files(install_dir, skill_name)
        write_marker(install_dir, version_str)
        routing_status = (
            write_routing_block(self._project_root)
            if skill_name == "lhp"
            else "unchanged"
        )

        return SkillInstallResult(
            install_dir=install_dir,
            skill_version=version_str,
            previous_version=previous_version,
            action=action,
            installed_files=tuple(enumerate_skill_files(skill_name)),
            routing_block_status=routing_status,
        )

    def export_skill(
        self, destination: Path, *, skill_name: str = "lhp"
    ) -> SkillInstallResult:
        """Export into an exact skill directory without requiring a project.

        Select ``lhp`` for development or ``lhp-learn`` for teaching. Both
        exports include their own technical references. Unknown names fail
        with ``LHP-IO-029`` before any writes.

        Identical and partial installs can be resumed. Differing files are
        preserved and reported; this operation never force-overwrites them.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-IO-020`` on conflicting files;
            ``LHP-IO-029`` on filesystem or verification failure.
        """
        from lhp.api._skill_export import export_files, skill_payload

        try:
            payload = skill_payload(skill_name)
            export_files(destination, payload)
        except LHPError:
            raise
        except (OSError, ValueError) as exc:
            raise _installation_error(exc) from exc
        return _export_result(destination, tuple(payload))

    def install_genie_skill(self, *, skill_name: str = "lhp") -> SkillInstallResult:
        """Install into the authenticated user's default Genie skills folder.

        Select ``lhp`` (default) or ``lhp-learn``; unknown names fail with
        ``LHP-IO-029`` before contacting the workspace.
        Call in notebook Python to retain notebook authentication. Requires
        the optional ``genie`` extra or a compatible installed Databricks SDK.
        Verification covers workspace file contents, not Genie chat discovery.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-IO-020`` on conflicting files;
            ``LHP-IO-029`` on missing SDK, authentication, upload or verification failure.
        """
        from lhp.api._genie_skill import install_files
        from lhp.api._skill_export import skill_payload

        try:
            payload = skill_payload(skill_name)
            destination = install_files(payload, skill_name)
        except LHPError:
            raise
        except Exception as exc:
            raise _installation_error(exc) from exc
        return _export_result(destination, tuple(payload))


def _installation_error(exc: Exception) -> LHPError:
    category = type(exc).__name__
    reasons = {
        "ModuleNotFoundError": "A required installation dependency is missing.",
        "ImportError": "The installed Databricks SDK is missing or incompatible.",
        "PermissionDenied": "The authenticated user cannot write the skill destination.",
        "PermissionError": "The destination is not writable by this process.",
        "Unauthenticated": "Workspace authentication failed; use authenticated notebook Python.",
        "ValueError": "The skill name, workspace identity or authentication configuration is invalid.",
        "OSError": "File transfer or content verification failed.",
    }
    reason = reasons.get(
        category,
        "An installation operation failed; inspect the chained cause in notebook Python.",
    )
    return ErrorFactory.io_error(
        codes.IO_029,
        title="Could not install or verify the LHP skill",
        details=f"{reason} Failure category: {category}. Existing conflicting files are preserved.",
        suggestions=[
            "Install lakehouse-plumber[genie] or use its offline installation bundle if the SDK is missing.",
            "Use export_skill(Path(...)) and the agent's workspace file tools if notebook authentication is unavailable.",
        ],
    )


def _export_result(
    destination: Path, installed_files: tuple[str, ...]
) -> SkillInstallResult:
    return SkillInstallResult(
        install_dir=destination,
        skill_version=current_version(),
        previous_version=None,
        action="installed",
        installed_files=installed_files,
        routing_block_status="unchanged",
    )

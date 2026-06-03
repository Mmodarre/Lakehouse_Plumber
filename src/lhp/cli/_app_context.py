"""Cross-command stateless wiring for the LHP CLI.

Only cross-command stateless wiring lives here: project-root discovery,
facade construction, substitution-file resolution, and
outcome-to-exit-code mapping. Anything command-specific stays
in the command file. This module
holds no class and no module state (§3.4 / §3.6) so it cannot accrete into a
god-object — every export is a free function.

CLI-layer module: it consumes the public facade (``lhp.api``) and the error
taxonomy (``lhp.errors``) only, never a domain submodule (§5.3 / §9.7).
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, NoReturn, Optional

from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.cli.exit_codes import ExitCode
from lhp.errors import ErrorFactory, codes

if TYPE_CHECKING:
    # TYPE_CHECKING guard keeps the runtime decoupled from the
    # event-stream presenter package.
    from lhp.cli.presenters.event_stream._model import RunOutcome

logger = logging.getLogger(__name__)


def resolve_project_root() -> Path:
    """Return the project root containing ``lhp.yaml``.

    Walks the current working directory and its parents looking for an
    ``lhp.yaml`` marker. Raises ``LHP-CFG-011`` if none is found.
    """
    current = Path.cwd().resolve()
    for path in [current, *list(current.parents)]:
        if (path / "lhp.yaml").exists():
            return path

    raise ErrorFactory.config_error(
        codes.CFG_011,
        title="Not in a LakehousePlumber project directory",
        details="No lhp.yaml file found in the current directory or any parent.",
        suggestions=[
            "Run 'lhp init <project_name>' to create a new project",
            "Navigate to an existing project directory",
        ],
    )


def build_facade(
    project_root: Path,
    *,
    pipeline_config: Optional[str] = None,
    max_workers: Optional[int] = None,
) -> LakehousePlumberApplicationFacade:
    """Construct the application facade for ``project_root``.

    Threads ``pipeline_config`` into the facade's ``pipeline_config_path``
    parameter and ``max_workers`` into its worker-pool size. All other
    composition is delegated to the facade's own ``for_project`` factory.
    """
    return LakehousePlumberApplicationFacade.for_project(
        project_root,
        pipeline_config_path=pipeline_config,
        max_workers=max_workers,
    )


def require_substitution_file(project_root: Path, env: str) -> Path:
    """Return the substitution file path for ``env``.

    The file is located at ``<project_root>/substitutions/<env>.yaml``. If it
    is missing, raises ``LHP-IO-006`` listing the available environments (the
    stems of the YAML files already present in ``substitutions/``) as
    suggestions.
    """
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if substitution_file.exists():
        return substitution_file

    sub_dir = project_root / "substitutions"
    available_envs: list[str] = []
    if sub_dir.exists():
        available_envs = sorted(f.stem for f in sub_dir.glob("*.yaml"))

    suggestions = [
        f"Create the substitution file: substitutions/{env}.yaml",
    ]
    if available_envs:
        suggestions.insert(
            0,
            f"Available environments: {', '.join(available_envs)}",
        )

    raise ErrorFactory.io_error(
        codes.IO_006,
        title=f"Substitution file not found for environment '{env}'",
        details=f"Expected file: {substitution_file}",
        suggestions=suggestions,
        context={"Environment": env},
    )


def exit_for_outcome(outcome: "RunOutcome", *, strict: bool) -> NoReturn:
    """Terminate the process with the exit code implied by ``outcome``.

    Exits ``ExitCode.ERROR`` when the run had failures, errored, or — under
    ``strict`` — produced warnings; otherwise exits ``ExitCode.SUCCESS``.
    Duck-typed on ``outcome.failures`` / ``outcome.errored`` /
    ``outcome.warnings`` so it stays decoupled from the concrete model.
    """
    if outcome.failures or outcome.errored or (strict and outcome.warnings):
        sys.exit(ExitCode.ERROR)
    sys.exit(ExitCode.SUCCESS)

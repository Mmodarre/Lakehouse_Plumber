"""Read-side sandbox scope projection shared by the ``facade.sandbox`` read.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Turns a project + environment into the frozen public
:class:`~lhp.api.SandboxScopeResult`: report whether the personal
``.lhp/profile.yaml`` exists, and — when it does — the profile's raw
selection plus the concrete pipelines a ``--sandbox`` run would cover,
resolving the profile's globs against real discovery exactly as
:func:`lhp.api._sandbox_run._resolve_sandbox_run` does for the run path
(:func:`lhp.core.sandbox.resolve_sandbox_run`, monitoring excluded).

Unlike the run seam this is a READ: sandbox-specific failures (a malformed
profile ``LHP-CFG-064``, a scope matching no pipeline ``LHP-VAL-064``, an
``env`` the team policy does not enable ``LHP-CFG-065``) are CAPTURED into
:attr:`SandboxScopeResult.error` instead of raised, so the caller can always
render the panel. Only project-level failures (e.g. an unloadable
``lhp.yaml`` surfacing from discovery) propagate.

Import discipline: this module imports ``lhp.core`` at module level, so it
must only be imported LAZILY at call time (the way :mod:`lhp.api._sandbox_run`
is) — an eager import from a module loaded by ``import lhp.api`` would drag
the core stack into API import time.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, List, Optional

from lhp.api.responses import SandboxScopeResult
from lhp.core.coordination import resolve_monitoring_pipeline_name
from lhp.core.loaders import load_sandbox_profile
from lhp.core.sandbox import resolve_sandbox_run
from lhp.errors import LHPError

if TYPE_CHECKING:
    from lhp.models import ProjectConfig, SandboxConfig

    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


def _describe_sandbox_scope(
    orchestrator: "_Orchestrator", env: Optional[str]
) -> SandboxScopeResult:
    """Project the sandbox profile + resolved scope for ``env`` (non-raising).

    ``env`` is optional: when ``None`` the env-only allowed_envs gate
    (``LHP-CFG-065``) is skipped and the full env-independent scope is
    returned; when given, that gate is applied and any failure surfaced.

    ``allowed_envs`` is read from the team ``sandbox:`` policy and reported
    regardless of whether a personal profile exists. When
    ``.lhp/profile.yaml`` is absent the result is the normal "not opted in"
    state (``profile_exists=False``, no ``error``). A malformed profile or a
    scope that resolves to nothing is surfaced through
    :attr:`SandboxScopeResult.error`; discovery only runs once the profile
    has loaded, and any :class:`~lhp.errors.LHPError` it raises propagates.
    """
    project_config: Optional["ProjectConfig"] = orchestrator.project_config
    sandbox_config: Optional["SandboxConfig"] = (
        project_config.sandbox if project_config is not None else None
    )
    allowed_envs: Optional[tuple[str, ...]] = (
        tuple(sandbox_config.allowed_envs)
        if sandbox_config is not None and sandbox_config.allowed_envs is not None
        else None
    )

    profile_path = orchestrator.project_root / ".lhp" / "profile.yaml"
    if not profile_path.exists():
        return SandboxScopeResult(profile_exists=False, allowed_envs=allowed_envs)

    try:
        profile = load_sandbox_profile(orchestrator.project_root)
    except LHPError as exc:
        return SandboxScopeResult(
            profile_exists=True,
            allowed_envs=allowed_envs,
            error=f"{exc.code}: {exc.details}",
        )

    patterns = tuple(profile.pipelines)
    flowgroups = orchestrator.bootstrap.discover_all_flowgroups()
    discovered: List[str] = sorted({fg.pipeline for fg in flowgroups})
    monitoring_name = resolve_monitoring_pipeline_name(project_config)

    # ``env`` gates ONLY the allowed_envs check (CFG-065); the scope itself is
    # env-independent. When ``env`` is omitted, drop that gate so a healthy
    # profile still reports its full resolved scope.
    resolve_config = sandbox_config
    if env is None and sandbox_config is not None:
        resolve_config = sandbox_config.model_copy(update={"allowed_envs": None})

    try:
        run = resolve_sandbox_run(
            resolve_config, profile, env or "", discovered, monitoring_name
        )
    except LHPError as exc:
        return SandboxScopeResult(
            profile_exists=True,
            namespace=profile.namespace,
            patterns=patterns,
            allowed_envs=allowed_envs,
            error=f"{exc.code}: {exc.details}",
        )

    return SandboxScopeResult(
        profile_exists=True,
        namespace=profile.namespace,
        patterns=patterns,
        resolved_pipelines=run.pipelines,
        allowed_envs=allowed_envs,
    )

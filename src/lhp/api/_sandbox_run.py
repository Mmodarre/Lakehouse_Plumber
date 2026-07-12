"""Sandbox preflight resolution shared by the generate and validate streams.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the single seam that turns a ``sandbox=True`` run's inputs into the
frozen :class:`~lhp.models.processing.SandboxRunConfig` the workers consume:
load the personal profile (``.lhp/profile.yaml``), read the team policy
(``lhp.yaml`` ``sandbox:`` block off the orchestrator's project config), and
delegate the merge + scope resolution to
:func:`lhp.core.sandbox.resolve_sandbox_run`.

Consumed by the generate stream (:mod:`lhp.api._generate_stream`) and the
validate stream (:mod:`lhp.api._validation_facade`) inside their ``sandbox``
branches. NO event emission happens here — the STREAMS own the §1.4
``ErrorEmitted`` + raise rendezvous for the :class:`~lhp.errors.LHPError`
failures this helper propagates.

Import discipline: this module imports ``lhp.core`` at module level, so it
must only be imported LAZILY at call time (the way the generation facade
defer-imports :mod:`lhp.api._generate_stream`) — an eager import from a
module loaded by ``import lhp.api`` would drag the core stack into API
import time.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, Sequence

from lhp.core.coordination import resolve_monitoring_pipeline_name
from lhp.core.loaders import load_sandbox_profile
from lhp.core.sandbox import resolve_sandbox_run

if TYPE_CHECKING:
    from lhp.models import FlowGroup, ProjectConfig, SandboxConfig
    from lhp.models.processing import SandboxRunConfig

    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


def _resolve_sandbox_run(
    orchestrator: "_Orchestrator",
    env: str,
    flowgroups: Sequence["FlowGroup"],
) -> "SandboxRunConfig":
    """Resolve the frozen sandbox run config for ONE ``sandbox=True`` run.

    ``flowgroups`` is the stream's post-discover resolved flowgroup set (the
    ``resolved_flowgroups`` both streams thread out of their ``discover``
    phase); the candidate pipeline names are its deduplicated, sorted
    ``pipeline`` fields — the same derivation the bundle facade uses. The
    monitoring pipeline's synthetic name is resolved off the project config
    (``None`` when monitoring is absent/disabled) so the scope resolver can
    exclude it from sandbox scope.

    Raises :class:`~lhp.errors.LHPError` (propagated, never emitted here):
    ``LHP-IO-025`` (missing ``.lhp/profile.yaml``), ``LHP-CFG-064``
    (malformed profile) from the profile loader; ``LHP-CFG-065``
    (environment not sandbox-enabled), ``LHP-VAL-064`` (profile scope
    matched no pipelines, or an exact entry names the monitoring pipeline)
    from the scope resolver.
    """
    profile = load_sandbox_profile(orchestrator.project_root)
    project_config: Optional["ProjectConfig"] = orchestrator.project_config
    sandbox_config: Optional["SandboxConfig"] = (
        project_config.sandbox if project_config is not None else None
    )
    discovered_pipelines = sorted({fg.pipeline for fg in flowgroups})
    monitoring_pipeline_name = resolve_monitoring_pipeline_name(project_config)
    return resolve_sandbox_run(
        sandbox_config,
        profile,
        env,
        discovered_pipelines,
        monitoring_pipeline_name,
    )

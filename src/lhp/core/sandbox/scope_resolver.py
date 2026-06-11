"""Sandbox scope resolution: profile entries -> concrete in-scope pipelines.

Resolves which discovered pipelines a ``--sandbox`` run covers: each
``SandboxProfile.pipelines`` entry — an exact name or an ``fnmatchcase`` glob
(case-sensitive, deterministic cross-platform) — is expanded against the
discovered pipeline names, with the monitoring pipeline silently excluded
from glob expansion. Scope is explicit, never auto-detected.

Pure functions only: no I/O, no filesystem, no loader imports — data in,
data out (plus :class:`~lhp.errors.LHPError` raises). Pattern application is
NOT here; the single rename choke point is
:func:`lhp.core.sandbox.rename_parts`.
"""

from fnmatch import fnmatchcase
from typing import Iterable, List, Optional

from lhp.errors import ErrorFactory, codes
from lhp.models import SandboxConfig, SandboxProfile
from lhp.models.processing import SandboxRunConfig

#: Characters that make a ``pipelines`` entry a glob rather than an exact name.
_GLOB_CHARS = frozenset("*?[")


def resolve_sandbox_run(
    sandbox_config: Optional[SandboxConfig],
    profile: SandboxProfile,
    env: str,
    discovered_pipelines: Iterable[str],
    monitoring_pipeline_name: Optional[str],
) -> SandboxRunConfig:
    """Merge team policy and personal profile into a :class:`SandboxRunConfig`.

    ``sandbox_config`` is ``None`` when ``lhp.yaml`` has no ``sandbox:``
    block; team defaults then apply (strategy ``table``, table_pattern
    ``{namespace}_{table}``, any environment allowed). Only the personal
    profile is mandatory, and that is enforced by the loader — never here.

    Raises :class:`~lhp.errors.LHPError`:

    - ``LHP-CFG-065`` — ``env`` is absent from ``sandbox.allowed_envs``.
    - ``LHP-VAL-064`` — an exact (non-glob) entry names the monitoring
      pipeline, or one or more entries match zero pipelines (a single error
      listing every offending entry plus the available pipeline names).
    """
    config = sandbox_config if sandbox_config is not None else SandboxConfig()

    if config.allowed_envs is not None and env not in config.allowed_envs:
        allowed = ", ".join(config.allowed_envs)
        raise ErrorFactory.config_error(
            codes.CFG_065,
            title="Environment not sandbox-enabled",
            details=(
                f"Environment '{env}' is not enabled for sandbox mode: "
                f"sandbox.allowed_envs permits only [{allowed}]."
            ),
            suggestions=[
                f"Run --sandbox against one of the allowed environments: {allowed}",
                f"Or add '{env}' to sandbox.allowed_envs in lhp.yaml",
                "Or remove allowed_envs entirely to leave sandbox mode unrestricted",
            ],
        )

    if monitoring_pipeline_name is not None:
        for entry in profile.pipelines:
            if entry == monitoring_pipeline_name and not _is_glob(entry):
                raise ErrorFactory.validation_error(
                    codes.VAL_064,
                    title="Monitoring pipeline cannot be sandboxed",
                    details=(
                        f"Profile pipelines entry '{entry}' names the "
                        f"monitoring pipeline '{monitoring_pipeline_name}'. "
                        "The monitoring pipeline cannot be sandboxed."
                    ),
                    suggestions=[
                        f"Remove '{entry}' from pipelines in .lhp/profile.yaml",
                        "Sandbox scope may only list regular project pipelines",
                    ],
                )

    candidates = sorted(
        {name for name in discovered_pipelines if name != monitoring_pipeline_name}
    )

    matched: set[str] = set()
    offenders: List[str] = []
    for entry in profile.pipelines:
        hits = [name for name in candidates if fnmatchcase(name, entry)]
        if hits:
            matched.update(hits)
        else:
            offenders.append(entry)

    if offenders:
        offender_list = ", ".join(f"'{entry}'" for entry in offenders)
        available = (
            ", ".join(candidates)
            if candidates
            else "none — no pipelines discovered in this project"
        )
        raise ErrorFactory.validation_error(
            codes.VAL_064,
            title="Sandbox profile entries matched no pipelines",
            details=(
                f"Profile pipelines entries matched zero pipelines: "
                f"{offender_list}. Available pipelines: {available}."
            ),
            suggestions=[
                "Fix the entries in .lhp/profile.yaml to name existing pipelines",
                "Glob matching is case-sensitive (fnmatchcase) — check the casing",
            ],
        )

    return SandboxRunConfig(
        namespace=profile.namespace,
        table_pattern=config.table_pattern,
        strategy=config.strategy,
        pipelines=tuple(sorted(matched)),
    )


def _is_glob(entry: str) -> bool:
    """True when ``entry`` contains glob metacharacters (``*``, ``?``, ``[``)."""
    return any(char in _GLOB_CHARS for char in entry)

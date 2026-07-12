"""Sandbox sub-facade — read-only developer-sandbox scope inspection.

Composed into :class:`~lhp.api.LakehousePlumberApplicationFacade` as
``facade.sandbox`` (the §1.11 sub-facade composition pattern), it groups the
read side of developer-sandbox mode: what the personal ``.lhp/profile.yaml``
selects and which concrete pipelines a ``--sandbox`` run would cover. Profile
WRITES are not a facade concern — the profile is an ordinary project file.

Kept off :class:`~lhp.api.InspectionFacade` because that facade is at the
§3.2 fifteen-public-method cap; a dedicated sub-facade is the
constitution-legal home (and the natural anchor as the sandbox surface grows)
rather than exceeding the cap or lifting a one-shot read onto the main
facade's long-running-shortcut top level.

:stability: internal
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from lhp.api.responses import SandboxScopeResult

if TYPE_CHECKING:
    # Internal orchestrator type, referenced only as a quoted annotation
    # (§1.10, §9.13) — never named directly in the public API surface.
    _Orchestrator = Any


class SandboxFacade:
    """Read-only developer-sandbox scope inspection.

    One operation: :meth:`describe_scope`. Constructed by the application
    facade's composition root; callers reach it as ``facade.sandbox``.

    :stability: provisional
    """

    def __init__(self, orchestrator: "_Orchestrator") -> None:
        self._orchestrator = orchestrator

    def describe_scope(self, *, env: Optional[str] = None) -> SandboxScopeResult:
        """Resolve the sandbox profile and its pipeline scope for ``env``.

        Returns a :class:`~lhp.api.SandboxScopeResult` reporting whether the
        personal ``.lhp/profile.yaml`` exists, the raw selection it declares,
        and the concrete pipelines a ``--sandbox`` run would cover — resolving
        the profile's globs against the discovered pipelines exactly as the
        CLI does, with the monitoring pipeline excluded. ``env`` is optional:
        when given it scopes the team-policy ``allowed_envs`` gate (surfacing
        ``LHP-CFG-065`` when the env is not enabled), matching the CLI's
        per-environment resolution; when ``None`` (the default) that gate is
        skipped and the full env-independent scope is returned.

        Sandbox-specific failures (malformed profile ``LHP-CFG-064``, a scope
        matching no pipeline ``LHP-VAL-064``, an ``env`` the team policy does
        not enable ``LHP-CFG-065``) are surfaced on
        :attr:`SandboxScopeResult.error` rather than raised, so the read never
        fails for a merely-unusable sandbox configuration.

        :stability: provisional
        :raises lhp.errors.LHPError: ``LHP-CFG-*`` / ``LHP-VAL-*`` /
            ``LHP-FILE-*`` / ``LHP-MULT-*`` propagated from flowgroup
            discovery when a profile is present — the same project-level
            families as :meth:`InspectionFacade.list_flowgroups`.
        """
        # Lazy import: ``_sandbox_view`` imports ``lhp.core`` at module level
        # and must not load at api-import time (mirrors the generation
        # facade's defer-import of ``_generate_stream``).
        from lhp.api._sandbox_view import _describe_sandbox_scope

        return _describe_sandbox_scope(self._orchestrator, env)

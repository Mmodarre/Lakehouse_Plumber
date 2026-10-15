"""Sandbox metadata for complete and stale editor projections."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from lhp.api._editor_overlay import _checked_profile_path
from lhp.api.editor_views import (
    EditorDiagnosticView,
    EditorProjectView,
    EditorSourceView,
)
from lhp.api.facade import LakehousePlumberApplicationFacade
from lhp.api.responses import SandboxScopeResult


def _with_sandbox_scope(
    view: EditorProjectView,
    facade: LakehousePlumberApplicationFacade,
    root: Path,
    enabled: bool,
) -> EditorProjectView:
    """Attach canonical scope without filtering the full authored graph."""
    try:
        _checked_profile_path(root)
        scope = facade.sandbox.describe_scope(env=view.environment)
    except ValueError as exc:
        scope = SandboxScopeResult(error=str(exc))
    diagnostics = view.diagnostics
    problem = scope.error or (
        "Create .lhp/profile.yaml before enabling sandbox mode"
        if not scope.profile_exists
        else None
    )
    if enabled and problem:
        diagnostics += (
            EditorDiagnosticView(
                severity="error",
                message=problem,
                layer="configuration",
                source=EditorSourceView(path=".lhp/profile.yaml"),
            ),
        )
    return replace(
        view, sandbox_enabled=enabled, sandbox=scope, diagnostics=diagnostics
    )


def _stale_sandbox_scope(
    view: EditorProjectView, *, profile_exists: bool
) -> EditorProjectView:
    """Never present a saved scope as the successfully resolved draft scope."""
    scope = replace(
        view.sandbox or SandboxScopeResult(),
        profile_exists=profile_exists,
        resolved_pipelines=(),
        error="Sandbox scope is unavailable until the invalid draft is corrected",
    )
    return replace(view, sandbox=scope)

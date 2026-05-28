"""Project ``required_lhp_version`` enforcement.

Extracted from :class:`ActionOrchestrator` so the orchestrator stays
under the §9.3 800-line hard cap. The check is pure-function over the
loaded :class:`ProjectConfig` and the current installed version; no
orchestrator state is touched.

:stability: internal
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

from ...errors import ErrorCategory, LHPError
from ...utils.version import get_version


logger = logging.getLogger(__name__)


def enforce_version_requirements(
    project_config: Optional[Any],
    *,
    actual_version: Optional[str] = None,
) -> None:
    """Enforce ``required_lhp_version`` if specified in the project config.

    Raises :class:`LHPError` (codes 006, 007, 008) when the installed
    LHP version does not satisfy the project's PEP 440 specifier set.
    Returns silently when no requirement is set, or when the
    ``LHP_IGNORE_VERSION`` environment variable is truthy
    (``"1"``, ``"true"``, ``"yes"`` — case-insensitive).

    ``actual_version`` is an explicit override for the currently-installed
    LHP version; orchestrator callers pass ``get_version()`` from their own
    module namespace so the existing tests that
    ``patch('lhp.core.coordination.orchestrator.get_version', ...)`` still take effect.
    When ``None`` (the default — internal callers that need no test seam),
    we look up the version via :func:`lhp.utils.version.get_version`.
    """
    if not project_config or not project_config.required_lhp_version:
        return

    if os.environ.get("LHP_IGNORE_VERSION", "").lower() in ("1", "true", "yes"):
        logger.warning(
            f"Version requirement bypass enabled via LHP_IGNORE_VERSION. "
            f"Required: {project_config.required_lhp_version}"
        )
        return

    try:
        from packaging.specifiers import SpecifierSet
        from packaging.version import Version
    except ImportError as exc:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="006",
            title="Missing packaging dependency",
            details="The 'packaging' library is required for version range checking but is not installed.",
            suggestions=[
                "Install packaging: pip install packaging>=23.2",
                "Or set LHP_IGNORE_VERSION=1 to bypass version checking",
            ],
        ) from exc

    required_spec = project_config.required_lhp_version
    if actual_version is None:
        actual_version = get_version()

    try:
        spec_set = SpecifierSet(required_spec)
        actual_ver = Version(actual_version)

        if actual_ver not in spec_set:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="007",
                title="LakehousePlumber version requirement not satisfied",
                details=f"Project requires LakehousePlumber version '{required_spec}', but version '{actual_version}' is installed.",
                suggestions=[
                    f"Install a compatible version: pip install 'lakehouse-plumber{required_spec}'",
                    "Or update the project's version requirement in lhp.yaml if you intend to upgrade",
                    "Or set LHP_IGNORE_VERSION=1 to bypass version checking (not recommended for production)",
                ],
                context={
                    "Required Version": required_spec,
                    "Installed Version": actual_version,
                    "Project Name": project_config.name,
                },
            )
    except Exception as e:
        if isinstance(e, LHPError):
            raise
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="008",
            title="Invalid version requirement specification",
            details=f"Could not parse version requirement '{required_spec}': {e}",
            suggestions=[
                "Use valid PEP 440 version specifiers (e.g., '>=0.4.1,<0.5.0')",
                "Check the required_lhp_version field in lhp.yaml",
                "Examples: '==0.4.1', '~=0.4.1', '>=0.4.1,<0.5.0'",
            ],
        ) from e

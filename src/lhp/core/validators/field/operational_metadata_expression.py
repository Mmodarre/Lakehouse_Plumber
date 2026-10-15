"""Validate the operational-metadata expressions a flowgroup's generated code renders.

Runs during flowgroup resolution, which ``lhp validate`` and ``lhp generate``
share, and resolves each expression through the same service and resolver the
render path uses. ``lhp validate --env X`` therefore reports exactly the
``LHP-CFG-010`` / ``LHP-CFG-070`` errors ``lhp generate --env X`` would raise.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, Mapping, Optional

from lhp.models import Action, FlowGroup, ProjectConfig

if TYPE_CHECKING:
    from ...processing import EnhancedSubstitutionManager
    from ...registry import ActionRegistry

logger = logging.getLogger(__name__)

#: Resolves the column expressions code generation renders for one action,
#: ``(action, flowgroup, preset_config, project_config, substitution_mgr)`` to
#: ``{column: resolved expression}``. The composition root injects
#: ``OperationalMetadataService.resolve_selected_columns``, so this package
#: does not import ``core/codegen``.
SelectedColumnsResolver = Callable[
    [
        Action,
        FlowGroup,
        Dict[str, Any],
        Optional[ProjectConfig],
        "EnhancedSubstitutionManager",
    ],
    Mapping[str, str],
]


class OperationalMetadataExpressionValidator:
    """Resolve the ``lhp.yaml`` metadata expressions a flowgroup's code renders.

    Checks exactly the (action, column) pairs generation emits: only actions
    whose generator renders operational metadata (its
    ``renders_operational_metadata`` declaration, read through the
    ``ActionRegistry``), and for those the preset + flowgroup + action
    selection, limited to columns whose ``applies_to`` includes ``view``.
    A column selected only by, say, a streaming-table write or a test action
    never reaches generated code and is not checked.

    Holds the project configuration, the registry and the bound service
    method, all picklable, so it crosses the worker ``spawn`` boundary with
    :class:`FlowgroupResolutionService`.
    """

    def __init__(
        self,
        project_config: Optional[ProjectConfig],
        action_registry: "ActionRegistry",
        resolve_columns: SelectedColumnsResolver,
    ) -> None:
        self._project_config = project_config
        self._action_registry = action_registry
        self._resolve_columns = resolve_columns

    def validate(
        self,
        flowgroup: FlowGroup,
        substitution_mgr: "EnhancedSubstitutionManager",
        preset_config: Mapping[str, Any],
    ) -> None:
        """Resolve every rendered expression for ``flowgroup`` in this environment.

        ``preset_config`` is the flowgroup's resolved preset chain, the same
        mapping code generation hands to generators.

        :raises LHPConfigError: ``LHP-CFG-010`` when a token is unresolved,
            ``LHP-CFG-070`` when an expression references a secret.
        """
        for action in flowgroup.actions:
            if not self._action_registry.renders_operational_metadata(action):
                continue
            columns = self._resolve_columns(
                action,
                flowgroup,
                dict(preset_config),
                self._project_config,
                substitution_mgr,
            )
            if columns:
                logger.debug(
                    f"Operational metadata expressions resolve for action "
                    f"'{action.name}': {sorted(columns)}"
                )

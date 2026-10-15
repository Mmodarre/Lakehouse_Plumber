"""Service for handling operational metadata across all generators."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Optional

from .expression import action_context_tokens
from .metadata import OperationalMetadataCatalog

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lhp.models import Action, FlowGroup, ProjectConfig

    from ...processing import EnhancedSubstitutionManager


class OperationalMetadataService:
    """Eliminates code duplication across generators — single point of configuration for metadata columns."""

    def get_metadata_and_imports(
        self,
        action: "Action",
        flowgroup,
        preset_config: Dict[str, Any],
        project_config,
        target_type: str = "view",
        import_manager=None,
        substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
    ):
        """Uses a single OperationalMetadataCatalog instance to ensure consistent
        expression adaptation and import detection.

        ``substitution_mgr`` is the run's environment; with it, expressions
        get environment ``${token}`` values and are checked for unresolved
        tokens and secrets (see :func:`.expression.resolve_metadata_expression`).

        Returns:
            Tuple of (add_metadata: bool, metadata_columns: dict, required_imports: list)

        :raises LHPConfigError: ``LHP-CFG-010`` / ``LHP-CFG-070`` from expression
            resolution.
        """
        operational_metadata = _build_catalog(
            action, flowgroup, project_config, substitution_mgr
        )

        if import_manager:
            operational_metadata.adapt_expressions_for_imports(import_manager)

        metadata_columns = _select_columns(
            operational_metadata, action, flowgroup, preset_config, target_type
        )

        # Get required imports from the same instance
        required_imports = operational_metadata.get_required_imports(metadata_columns)

        logger.debug(
            f"Operational metadata result for '{getattr(action, 'name', 'unknown')}': {len(metadata_columns)} column(s), {len(required_imports)} import(s)"
        )
        return bool(metadata_columns), metadata_columns, list(required_imports)

    def resolve_selected_columns(
        self,
        action: "Action",
        flowgroup: Optional["FlowGroup"],
        preset_config: Dict[str, Any],
        project_config: Optional["ProjectConfig"],
        substitution_mgr: Optional["EnhancedSubstitutionManager"] = None,
        target_type: str = "view",
    ) -> Dict[str, str]:
        """Resolve the column expressions :meth:`get_metadata_and_imports` would
        render for ``action``, without import handling.

        Same catalog construction, selection and resolution as the render
        path, so the validate path reports exactly what generation would raise.
        The validate path injects this bound method as the
        ``OperationalMetadataExpressionValidator`` column resolver.

        :raises LHPConfigError: ``LHP-CFG-010`` / ``LHP-CFG-070`` from expression
            resolution.
        """
        operational_metadata = _build_catalog(
            action, flowgroup, project_config, substitution_mgr
        )
        return _select_columns(
            operational_metadata, action, flowgroup, preset_config, target_type
        )

    def get_all_metadata_column_names(self, project_config) -> set:
        operational_metadata = OperationalMetadataCatalog(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )
        return operational_metadata.get_all_column_names()


def _build_catalog(
    action: "Action",
    flowgroup: Optional["FlowGroup"],
    project_config: Optional["ProjectConfig"],
    substitution_mgr: Optional["EnhancedSubstitutionManager"],
) -> OperationalMetadataCatalog:
    logger.debug(
        f"Resolving operational metadata for action '{getattr(action, 'name', 'unknown')}'"
    )
    operational_metadata = OperationalMetadataCatalog(
        project_config=(project_config.operational_metadata if project_config else None)
    )
    operational_metadata.update_context(
        flowgroup.pipeline if flowgroup else None,
        flowgroup.flowgroup if flowgroup else None,
        substitution_mgr=substitution_mgr,
        extra_context_tokens=action_context_tokens(action),
    )
    return operational_metadata


def _select_columns(
    operational_metadata: OperationalMetadataCatalog,
    action: "Action",
    flowgroup: Optional["FlowGroup"],
    preset_config: Dict[str, Any],
    target_type: str,
) -> Dict[str, str]:
    action_name = getattr(action, "name", "unknown")
    selection = operational_metadata.resolve_metadata_selection(
        flowgroup, action, preset_config
    )
    if selection:
        logger.debug(
            f"Metadata selection for '{action_name}': {list(selection.keys())} (sources: action > flowgroup > preset > project)"
        )
    else:
        logger.debug(f"No operational metadata selection for action '{action_name}'")
    return operational_metadata.get_selected_columns(selection or {}, target_type)

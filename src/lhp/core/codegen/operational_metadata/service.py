"""Service for handling operational metadata across all generators."""

import logging
from typing import TYPE_CHECKING, Any, Dict

from .metadata import OperationalMetadataCatalog

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from lhp.models import Action


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
    ):
        """Uses a single OperationalMetadataCatalog instance to ensure consistent
        expression adaptation and import detection.

        Returns:
            Tuple of (add_metadata: bool, metadata_columns: dict, required_imports: list)
        """
        action_name = getattr(action, "name", "unknown")
        logger.debug(
            f"Resolving operational metadata for action '{action_name}', target_type='{target_type}'"
        )

        operational_metadata = OperationalMetadataCatalog(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )

        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)

        if import_manager:
            operational_metadata.adapt_expressions_for_imports(import_manager)

        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        if selection:
            logger.debug(
                f"Metadata selection for '{action_name}': {list(selection.keys())} (sources: action > flowgroup > preset > project)"
            )
        else:
            logger.debug(
                f"No operational metadata selection for action '{action_name}'"
            )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, target_type
        )

        # Get required imports from the same instance
        required_imports = operational_metadata.get_required_imports(metadata_columns)

        logger.debug(
            f"Operational metadata result for '{action_name}': {len(metadata_columns)} column(s), {len(required_imports)} import(s)"
        )
        return bool(metadata_columns), metadata_columns, list(required_imports)

    def get_all_metadata_column_names(self, project_config) -> set:
        operational_metadata = OperationalMetadataCatalog(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )
        return operational_metadata.get_all_column_names()

"""Service for handling operational metadata across all generators."""

from typing import Dict, Any, Optional, TYPE_CHECKING
from ...utils.operational_metadata import OperationalMetadata

if TYPE_CHECKING:
    from ...models.config import Action


class OperationalMetadataService:
    """Centralized service for operational metadata handling.
    
    This service eliminates code duplication across generators by providing
    a single point of configuration for operational metadata columns.
    """
    
    def get_metadata_and_imports(
        self,
        action: 'Action',
        flowgroup,
        preset_config: Dict[str, Any],
        project_config,
        target_type: str = "view",
        import_manager=None
    ):
        """Get operational metadata configuration AND required imports in one call.
        
        Uses a single OperationalMetadata instance to ensure consistent
        expression adaptation and import detection.
        
        Args:
            action: Action configuration
            flowgroup: FlowGroup containing the action
            preset_config: Preset configuration dictionary
            project_config: Project-level configuration
            target_type: Type of target (view, streaming_table, materialized_view)
            import_manager: Optional ImportManager for advanced import handling
            
        Returns:
            Tuple of (add_metadata: bool, metadata_columns: dict, required_imports: list)
        """
        # Initialize operational metadata handler (single instance)
        operational_metadata = OperationalMetadata(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )
        
        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)
        
        # Adapt expressions if import manager is available
        if import_manager:
            operational_metadata.adapt_expressions_for_imports(import_manager)
        
        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, target_type
        )
        
        # Get required imports from the same instance
        required_imports = operational_metadata.get_required_imports(metadata_columns)
        
        return bool(metadata_columns), metadata_columns, list(required_imports)


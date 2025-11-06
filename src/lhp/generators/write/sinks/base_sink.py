"""Base generator for all sink types."""

from abc import abstractmethod
from typing import Dict, Any, List
from ....core.base_generator import BaseActionGenerator
from ....models.config import Action
from ....utils.operational_metadata import OperationalMetadata


class BaseSinkWriteGenerator(BaseActionGenerator):
    """Base class for sink write generators."""
    
    def __init__(self):
        super().__init__(use_import_manager=True)
        self.add_import("import dlt")
        self.add_import("from pyspark.sql import functions as F")
    
    @abstractmethod
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate sink code - must be implemented by subclasses."""
        pass
    
    def _extract_source_views(self, source) -> List[str]:
        """Extract source views from source configuration.
        
        Args:
            source: Source configuration (string, list, or dict)
            
        Returns:
            List of source view names
        """
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            # Handle list of strings or dicts with view names
            views = []
            for item in source:
                if isinstance(item, str):
                    views.append(item)
                elif isinstance(item, dict) and "view" in item:
                    views.append(item["view"])
            return views
        elif isinstance(source, dict):
            # Single dict with view name
            if "view" in source:
                return [source["view"]]
        return []
    
    def _get_operational_metadata(
        self, 
        action: Action, 
        context: Dict[str, Any]
    ) -> tuple:
        """Get operational metadata configuration.
        
        Args:
            action: Action configuration
            context: Context dictionary with flowgroup and project info
            
        Returns:
            Tuple of (add_metadata: bool, metadata_columns: dict)
        """
        flowgroup = context.get("flowgroup")
        preset_config = context.get("preset_config", {})
        project_config = context.get("project_config")
        
        # Initialize operational metadata handler
        operational_metadata = OperationalMetadata(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )
        
        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)
        
        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, "view"
        )
        
        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)
        
        return bool(metadata_columns), metadata_columns


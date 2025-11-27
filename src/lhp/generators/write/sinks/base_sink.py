"""Base generator for all sink types."""

from abc import abstractmethod
from typing import Dict, Any, List
from ....core.base_generator import BaseActionGenerator
from ....models.config import Action


class BaseSinkWriteGenerator(BaseActionGenerator):
    """Base class for sink write generators."""
    
    def __init__(self):
        super().__init__(use_import_manager=True)
        self.add_import("from pyspark import pipelines as dp")
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


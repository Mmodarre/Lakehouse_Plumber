"""Delta sink generator."""

from typing import Dict, Any
from .base_sink import BaseSinkWriteGenerator
from ....models.config import Action


class DeltaSinkWriteGenerator(BaseSinkWriteGenerator):
    """Generate Delta sink write actions."""
    
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Delta sink code.
        
        Args:
            action: Action configuration
            context: Context dictionary with flowgroup and project info
            
        Returns:
            Generated Python code for Delta sink
        """
        sink_config = action.write_target
        
        # Extract configuration
        sink_name = sink_config.get("sink_name")
        options = sink_config.get("options", {})
        comment = sink_config.get("comment") or action.description or f"Delta sink to {options.get('tableName', 'external table')}"
        
        # Extract source views
        source_views = self._extract_source_views(action.source)
        
        # Get operational metadata configuration
        add_metadata, metadata_columns = self._get_operational_metadata(action, context)
        
        # Build template context
        template_context = {
            "action_name": action.name,
            "sink_name": sink_name,
            "format": "delta",
            "options": options,
            "source_views": source_views,
            "comment": comment,
            "description": action.description or comment,
            "add_operational_metadata": add_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }
        
        return self.render_template("write/sinks/delta_sink.py.j2", template_context)








"""Kafka/Event Hubs sink generator."""

from typing import Dict, Any
from .base_sink import BaseSinkWriteGenerator
from ....models.config import Action
from ....utils.kafka_validator import KafkaOptionsValidator


class KafkaSinkWriteGenerator(BaseSinkWriteGenerator):
    """Generate Kafka/Event Hubs sink write actions."""
    
    def __init__(self):
        super().__init__()
        self.kafka_validator = KafkaOptionsValidator()
    
    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Kafka sink code.
        
        Args:
            action: Action configuration
            context: Context dictionary with flowgroup and project info
            
        Returns:
            Generated Python code for Kafka sink
        """
        sink_config = action.write_target
        
        # Extract and validate configuration
        bootstrap_servers = sink_config.get("bootstrap_servers")
        topic = sink_config.get("topic")
        sink_name = sink_config.get("sink_name")
        
        # Build sink options
        sink_options = {
            "kafka.bootstrap.servers": bootstrap_servers,
            "topic": topic
        }
        
        # Process additional options
        if sink_config.get("options"):
            processed = self.kafka_validator.process_options(
                sink_config["options"],
                action.name,
                is_source=False
            )
            sink_options.update(processed)
        
        # Detect Event Hubs by OAuth mechanism
        is_event_hubs = sink_options.get("kafka.sasl.mechanism") == "OAUTHBEARER"
        
        # Extract source views
        source_views = self._extract_source_views(action.source)
        
        # Get operational metadata configuration
        add_metadata, metadata_columns = self._get_operational_metadata(action, context)
        
        # Build comment
        if is_event_hubs:
            comment = sink_config.get("comment") or action.description or f"Event Hubs sink to {topic}"
        else:
            comment = sink_config.get("comment") or action.description or f"Kafka sink to {topic}"
        
        # Build template context
        template_context = {
            "action_name": action.name,
            "sink_name": sink_name,
            "format": "kafka",
            "sink_options": sink_options,
            "source_views": source_views,
            "comment": comment,
            "description": action.description or comment,
            "add_operational_metadata": add_metadata,
            "metadata_columns": metadata_columns,
            "is_event_hubs": is_event_hubs,
            "flowgroup": context.get("flowgroup"),
        }
        
        return self.render_template("write/sinks/kafka_sink.py.j2", template_context)








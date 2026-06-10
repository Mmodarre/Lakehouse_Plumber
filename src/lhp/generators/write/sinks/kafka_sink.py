import logging
from typing import Any, Dict

from lhp.models import Action

from ....core.validators.field.kafka_options import KafkaOptionsValidator
from .base_sink import BaseSinkWriteGenerator

logger = logging.getLogger(__name__)


class KafkaSinkWriteGenerator(BaseSinkWriteGenerator):
    def __init__(self):
        super().__init__()
        self.kafka_validator = KafkaOptionsValidator()

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        sink_config = action.write_target

        bootstrap_servers = sink_config.get("bootstrap_servers")
        topic = sink_config.get("topic")
        sink_name = sink_config.get("sink_name")
        logger.debug(
            f"Generating Kafka sink for action '{action.name}': topic='{topic}', sink_name='{sink_name}'"
        )

        sink_options = {"kafka.bootstrap.servers": bootstrap_servers, "topic": topic}

        if sink_config.get("options"):
            processed = self.kafka_validator.process_options(
                sink_config["options"], action.name, is_source=False
            )
            sink_options.update(processed)

        # Detect Event Hubs by OAuth mechanism
        is_event_hubs = sink_options.get("kafka.sasl.mechanism") == "OAUTHBEARER"

        source_views = self._extract_source_views(action.source)

        add_metadata, metadata_columns = self._get_operational_metadata(action, context)

        if is_event_hubs:
            comment = (
                sink_config.get("comment")
                or action.description
                or f"Event Hubs sink to {topic}"
            )
        else:
            comment = (
                sink_config.get("comment")
                or action.description
                or f"Kafka sink to {topic}"
            )

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

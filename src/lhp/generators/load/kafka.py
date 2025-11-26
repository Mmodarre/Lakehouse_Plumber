"""Kafka load generator"""

import logging
from pathlib import Path
from typing import Dict, Any, List
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter, LHPError
from ...utils.kafka_validator import KafkaOptionsValidator


class KafkaLoadGenerator(BaseActionGenerator):
    """Generate Kafka streaming load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)
        self.kafka_validator = KafkaOptionsValidator()

        # Mandatory options that must be present
        self.mandatory_options = {"kafka.bootstrap.servers"}

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Kafka load code."""
        source_config = action.source if isinstance(action.source, dict) else {}

        # Kafka is always streaming
        readMode = action.readMode or source_config.get("readMode", "stream")
        if readMode != "stream":
            raise ValueError(
                f"Kafka action '{action.name}' requires readMode='stream', got '{readMode}'"
            )

        # Extract configuration
        bootstrap_servers = source_config.get("bootstrap_servers")
        if not bootstrap_servers:
            raise ValueError(
                f"Kafka action '{action.name}' must have 'bootstrap_servers'"
            )

        # Validate subscription method
        self._validate_subscription_method(source_config, action.name)

        # Check for conflicts between old and new approaches (if we ever add legacy support)
        self._check_conflicts(source_config, action.name)

        # Process options
        reader_options = {}
        
        # Add mandatory kafka.bootstrap.servers
        reader_options["kafka.bootstrap.servers"] = bootstrap_servers
        
        # Add subscription method
        self._add_subscription_method(source_config, reader_options)

        # Process additional options from options dict
        if source_config.get("options"):
            reader_options.update(
                self.kafka_validator.process_options(
                    source_config["options"], action.name, is_source=True
                )
            )

        # Validate mandatory options are present
        self._validate_mandatory_options(reader_options, action.name)

        # Handle operational metadata
        flowgroup = context.get("flowgroup")
        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "reader_options": reader_options,
            "description": action.description
            or f"Load data from Kafka topics at {bootstrap_servers}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/kafka.py.j2", template_context)

    def _validate_subscription_method(
        self, source_config: Dict[str, Any], action_name: str
    ):
        """Validate that exactly one subscription method is provided.

        Args:
            source_config: Source configuration dictionary
            action_name: Name of the action for error messages
        """
        subscription_methods = {
            "subscribe": source_config.get("subscribe"),
            "subscribePattern": source_config.get("subscribePattern"),
            "assign": source_config.get("assign"),
        }

        provided_methods = [k for k, v in subscription_methods.items() if v is not None]

        if len(provided_methods) == 0:
            raise ValueError(
                f"Kafka action '{action_name}' must have one of: 'subscribe', "
                f"'subscribePattern', or 'assign'"
            )
        elif len(provided_methods) > 1:
            raise ValueError(
                f"Kafka action '{action_name}' can only have ONE of: 'subscribe', "
                f"'subscribePattern', or 'assign'. Found: {', '.join(provided_methods)}"
            )

    def _add_subscription_method(
        self, source_config: Dict[str, Any], reader_options: Dict[str, Any]
    ):
        """Add the subscription method to reader options.

        Args:
            source_config: Source configuration dictionary
            reader_options: Dictionary to add options to
        """
        if source_config.get("subscribe"):
            reader_options["subscribe"] = source_config["subscribe"]
        elif source_config.get("subscribePattern"):
            reader_options["subscribePattern"] = source_config["subscribePattern"]
        elif source_config.get("assign"):
            reader_options["assign"] = source_config["assign"]

    def _check_conflicts(self, source_config: Dict[str, Any], action_name: str):
        """Check for conflicts in configuration.

        Args:
            source_config: Source configuration dictionary
            action_name: Name of the action for error messages
        """
        # Currently no legacy options to check, but keep method for future compatibility
        pass

    def _validate_mandatory_options(
        self, reader_options: Dict[str, Any], action_name: str
    ):
        """Validate that mandatory kafka options are present.

        Args:
            reader_options: Combined reader options
            action_name: Action name for error messages
        """
        # Check for mandatory kafka.bootstrap.servers
        if "kafka.bootstrap.servers" not in reader_options:
            raise ValueError(
                f"Kafka action '{action_name}' must have 'kafka.bootstrap.servers' option"
            )


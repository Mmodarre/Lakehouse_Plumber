"""Kafka load generator"""

import logging
from pathlib import Path
from typing import Any, Dict, List

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import (
    ErrorCategory,
    ErrorFormatter,
    LHPError,
    LHPValidationError,
)
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
        self.logger.debug(
            f"Generating Kafka load for target '{action.target}', action '{action.name}'"
        )

        # Kafka is always streaming
        readMode = action.readMode or source_config.get("readMode", "stream")
        if readMode != "stream":
            raise ErrorFormatter.invalid_read_mode(
                action_name=action.name,
                action_type="kafka",
                provided=readMode,
                valid_modes=["stream"],
            )

        # Extract configuration
        bootstrap_servers = source_config.get("bootstrap_servers")
        if not bootstrap_servers:
            raise ErrorFormatter.missing_required_field(
                field_name="bootstrap_servers",
                component_type="Kafka load action",
                component_name=action.name,
                field_description="The Kafka bootstrap servers address is required to connect to the Kafka cluster.",
                example_config="""actions:
  - name: load_kafka_events
    type: load
    sub_type: kafka
    target: v_events
    source:
      bootstrap_servers: "broker1:9092,broker2:9092"
      subscribe: "my-topic" """,
            )

        # Determine subscription method for logging
        sub_method = next(
            (
                k
                for k in ("subscribe", "subscribePattern", "assign")
                if source_config.get(k)
            ),
            "unknown",
        )
        sub_value = source_config.get(sub_method, "")
        self.logger.debug(
            f"Kafka load '{action.name}': bootstrap_servers='{bootstrap_servers}', subscription={sub_method}='{sub_value}'"
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
            options = source_config["options"]
            # Validate options is a dictionary
            if not isinstance(options, dict):
                raise ErrorFormatter.invalid_field_type(
                    action_name=action.name,
                    field_name="options",
                    expected_type="a dictionary (mapping)",
                    actual_type=type(options).__name__,
                    example="""options:
  kafka.group.id: "my-consumer-group"
  startingOffsets: "earliest" """,
                )
            reader_options.update(
                self.kafka_validator.process_options(
                    options, action.name, is_source=True
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
            raise ErrorFormatter.missing_required_field(
                field_name="subscribe/subscribePattern/assign",
                component_type="Kafka load action",
                component_name=action_name,
                field_description="Exactly one Kafka subscription method must be specified.",
                example_config="""source:
  bootstrap_servers: "broker:9092"
  subscribe: "my-topic"            # Option 1: specific topic(s)
  # subscribePattern: "topic-.*"   # Option 2: topic pattern
  # assign: '{"topic": [0, 1]}'   # Option 3: specific partitions""",
            )
        elif len(provided_methods) > 1:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="009",
                title=f"Multiple subscription methods in Kafka action '{action_name}'",
                details=(
                    f"Kafka action '{action_name}' specifies multiple subscription methods: "
                    f"{', '.join(provided_methods)}. Only one is allowed."
                ),
                suggestions=[
                    "Use only ONE of: 'subscribe', 'subscribePattern', or 'assign'",
                    "Remove the extra subscription method(s) from your configuration",
                ],
                context={
                    "Action": action_name,
                    "Provided Methods": ", ".join(provided_methods),
                },
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
            raise ErrorFormatter.missing_required_field(
                field_name="kafka.bootstrap.servers",
                component_type="Kafka load action",
                component_name=action_name,
                field_description="The 'kafka.bootstrap.servers' option must be present in the final reader options.",
                example_config="""source:
  bootstrap_servers: "broker1:9092,broker2:9092"
  subscribe: "my-topic" """,
            )

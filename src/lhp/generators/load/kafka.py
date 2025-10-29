"""Kafka load generator"""

import logging
from pathlib import Path
from typing import Dict, Any, List
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata
from ...utils.error_formatter import ErrorFormatter, LHPError


class KafkaLoadGenerator(BaseActionGenerator):
    """Generate Kafka streaming load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
        self.logger = logging.getLogger(__name__)

        # Known kafka options (from Databricks Kafka connector documentation)
        self.known_kafka_options = {
            "bootstrap.servers",
            "group.id",
            "session.timeout.ms",
            "heartbeat.interval.ms",
            "max.poll.records",
            "max.poll.interval.ms",
            "enable.auto.commit",
            "auto.commit.interval.ms",
            "auto.offset.reset",
            "ssl.truststore.location",
            "ssl.truststore.password",
            "ssl.truststore.type",
            "ssl.keystore.location",
            "ssl.keystore.password",
            "ssl.keystore.type",
            "ssl.key.password",
            "ssl.protocol",
            "ssl.enabled.protocols",
            "ssl.truststore.certificates",
            "ssl.keystore.certificate.chain",
            "ssl.keystore.key",
            "sasl.mechanism",
            "sasl.jaas.config",
            "sasl.client.callback.handler.class",
            "sasl.login.callback.handler.class",
            "sasl.login.class",
            "sasl.kerberos.service.name",
            "sasl.kerberos.kinit.cmd",
            "sasl.kerberos.ticket.renew.window.factor",
            "sasl.kerberos.ticket.renew.jitter",
            "sasl.kerberos.min.time.before.relogin",
            "sasl.login.refresh.window.factor",
            "sasl.login.refresh.window.jitter",
            "sasl.login.refresh.min.period.seconds",
            "sasl.login.refresh.buffer.seconds",
            "sasl.oauthbearer.token.endpoint.url",
            "sasl.oauthbearer.scope.claim.name",
            "sasl.oauthbearer.sub.claim.name",
            "security.protocol",
            "connections.max.idle.ms",
            "request.timeout.ms",
            "metadata.max.age.ms",
            "reconnect.backoff.ms",
            "reconnect.backoff.max.ms",
            "retry.backoff.ms",
            "fetch.min.bytes",
            "fetch.max.wait.ms",
            "fetch.max.bytes",
            "max.partition.fetch.bytes",
            "check.crcs",
            "key.deserializer",
            "value.deserializer",
            "partition.assignment.strategy",
            "client.id",
            "client.dns.lookup",
            "client.rack",
        }

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
                self._process_options(
                    source_config["options"], action.name
                )
            )

        # Validate mandatory options are present
        self._validate_mandatory_options(reader_options, action.name)

        # Handle operational metadata
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

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "reader_options": reader_options,
            "description": action.description
            or f"Load data from Kafka topics at {bootstrap_servers}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
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

    def _validate_msk_iam_auth(self, options: Dict[str, Any], action_name: str) -> None:
        """Validate AWS MSK IAM authentication configuration."""
        if options.get("kafka.sasl.mechanism") == "AWS_MSK_IAM":
            required_msk_options = {
                "kafka.sasl.jaas.config",
                "kafka.security.protocol",
                "kafka.sasl.client.callback.handler.class"
            }
            missing = required_msk_options - set(options.keys())
            if missing:
                raise ValueError(
                    f"Kafka action '{action_name}': AWS MSK IAM authentication requires: {', '.join(sorted(missing))}"
                )

    def _validate_event_hubs_oauth(self, options: Dict[str, Any], action_name: str) -> None:
        """Validate Azure Event Hubs OAuth configuration."""
        if options.get("kafka.sasl.mechanism") == "OAUTHBEARER":
            required_oauth_options = {
                "kafka.sasl.jaas.config",
                "kafka.sasl.oauthbearer.token.endpoint.url",
                "kafka.security.protocol",
                "kafka.sasl.login.callback.handler.class"
            }
            missing = required_oauth_options - set(options.keys())
            if missing:
                raise ValueError(
                    f"Kafka action '{action_name}': OAuth authentication requires: {', '.join(sorted(missing))}"
                )

    def _process_options(
        self, options: Dict[str, Any], action_name: str
    ) -> Dict[str, Any]:
        """Process the options field and validate kafka options.

        Args:
            options: Options dictionary from YAML
            action_name: Name of the action for error messages

        Returns:
            Processed options dictionary
        """
        processed_options = {}

        for key, value in options.items():
            # Check if this looks like a kafka option without prefix
            if (
                not key.startswith("kafka.")
                and key not in ["subscribe", "subscribePattern", "assign", 
                               "startingOffsets", "endingOffsets", "failOnDataLoss",
                               "minPartitions", "maxOffsetsPerTrigger", "includeHeaders"]
            ):
                # Check if it's a known kafka option that should have prefix
                option_without_prefix = key.split(".")[-1]
                if option_without_prefix in self.known_kafka_options:
                    raise ErrorFormatter.configuration_conflict(
                        action_name=action_name,
                        field_pairs=[(key, f"kafka.{key}")],
                        preset_name=None,
                    )

            # Preserve original type for all options
            processed_options[key] = value

        # Validate MSK IAM if configured
        if processed_options.get("kafka.sasl.mechanism") == "AWS_MSK_IAM":
            self._validate_msk_iam_auth(processed_options, action_name)

        # Validate Event Hubs OAuth if configured
        if processed_options.get("kafka.sasl.mechanism") == "OAUTHBEARER":
            self._validate_event_hubs_oauth(processed_options, action_name)

        return processed_options

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


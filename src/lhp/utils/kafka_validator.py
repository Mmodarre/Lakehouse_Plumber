"""Shared Kafka options validator for sources and sinks."""

from typing import Dict, Any
from .error_formatter import ErrorFormatter


class KafkaOptionsValidator:
    """Validate Kafka and Event Hubs options for both sources and sinks."""
    
    # Known kafka options (from Databricks Kafka connector documentation)
    KNOWN_KAFKA_OPTIONS = {
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
    
    # Source-only options (for subscription methods and reading)
    SOURCE_ONLY_OPTIONS = {
        "subscribe",
        "subscribePattern",
        "assign",
        "startingOffsets",
        "endingOffsets",
        "failOnDataLoss",
        "minPartitions",
        "maxOffsetsPerTrigger",
        "includeHeaders",
    }
    
    # Sink-only options (for writing)
    SINK_ONLY_OPTIONS = {
        "topic",  # Sinks use single topic, sources use subscribe/subscribePattern/assign
    }
    
    @staticmethod
    def validate_msk_iam_auth(options: Dict[str, Any], action_name: str) -> None:
        """Validate AWS MSK IAM authentication configuration.
        
        Args:
            options: Options dictionary with kafka.* prefixed keys
            action_name: Name of the action for error messages
            
        Raises:
            ValueError: If required MSK IAM options are missing
        """
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
    
    @staticmethod
    def validate_event_hubs_oauth(options: Dict[str, Any], action_name: str) -> None:
        """Validate Azure Event Hubs OAuth configuration.
        
        Args:
            options: Options dictionary with kafka.* prefixed keys
            action_name: Name of the action for error messages
            
        Raises:
            ValueError: If required OAuth options are missing
        """
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
    
    @classmethod
    def process_options(
        cls, 
        options: Dict[str, Any], 
        action_name: str,
        is_source: bool = True
    ) -> Dict[str, Any]:
        """Process and validate Kafka options.
        
        Args:
            options: Options dictionary from YAML
            action_name: Name of the action for error messages
            is_source: True for sources (subscribe/subscribePattern/assign allowed),
                      False for sinks (topic allowed)
        
        Returns:
            Processed options dictionary
            
        Raises:
            ValueError: If invalid options are found
        """
        processed_options = {}
        
        # Determine which special options are allowed
        allowed_special = cls.SOURCE_ONLY_OPTIONS if is_source else cls.SINK_ONLY_OPTIONS
        
        for key, value in options.items():
            # Check if this looks like a kafka option without prefix
            if (
                not key.startswith("kafka.")
                and key not in allowed_special
            ):
                # Check if it's a known kafka option that should have prefix
                if key in cls.KNOWN_KAFKA_OPTIONS:
                    raise ErrorFormatter.configuration_conflict(
                        action_name=action_name,
                        field_pairs=[(key, f"kafka.{key}")],
                        preset_name=None,
                    )
            
            # Preserve original type for all options
            processed_options[key] = value
        
        # Validate MSK IAM if configured
        if processed_options.get("kafka.sasl.mechanism") == "AWS_MSK_IAM":
            cls.validate_msk_iam_auth(processed_options, action_name)
        
        # Validate Event Hubs OAuth if configured
        if processed_options.get("kafka.sasl.mechanism") == "OAUTHBEARER":
            cls.validate_event_hubs_oauth(processed_options, action_name)
        
        return processed_options


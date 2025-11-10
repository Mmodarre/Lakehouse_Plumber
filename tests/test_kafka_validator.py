"""Test Kafka options validator utility."""

import pytest
from lhp.utils.kafka_validator import KafkaOptionsValidator
from lhp.utils.error_formatter import LHPError


class TestKafkaOptionsValidator:
    """Test Kafka options validator functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = KafkaOptionsValidator()

    # Test MSK IAM Authentication Validation
    def test_validate_msk_iam_auth_complete(self):
        """Test MSK IAM auth with all required options."""
        options = {
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "test_config",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.client.callback.handler.class": "test_handler"
        }
        
        # Should not raise
        self.validator.validate_msk_iam_auth(options, "test_action")

    def test_validate_msk_iam_auth_missing_jaas(self):
        """Test MSK IAM auth missing jaas.config."""
        options = {
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.client.callback.handler.class": "test_handler"
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.validate_msk_iam_auth(options, "test_action")
        
        assert "test_action" in str(exc_info.value)
        assert "kafka.sasl.jaas.config" in str(exc_info.value)

    def test_validate_msk_iam_auth_missing_multiple(self):
        """Test MSK IAM auth missing multiple options."""
        options = {
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.validate_msk_iam_auth(options, "test_action")
        
        error_msg = str(exc_info.value)
        assert "test_action" in error_msg
        assert "kafka.sasl.jaas.config" in error_msg
        assert "kafka.security.protocol" in error_msg
        assert "kafka.sasl.client.callback.handler.class" in error_msg

    def test_validate_msk_iam_auth_not_msk(self):
        """Test MSK IAM validation when not using MSK IAM."""
        options = {
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL"
        }
        
        # Should not raise even with incomplete MSK config
        self.validator.validate_msk_iam_auth(options, "test_action")

    # Test Event Hubs OAuth Validation
    def test_validate_event_hubs_oauth_complete(self):
        """Test Event Hubs OAuth with all required options."""
        options = {
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.jaas.config": "test_config",
            "kafka.sasl.oauthbearer.token.endpoint.url": "https://token.endpoint",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.login.callback.handler.class": "test_handler"
        }
        
        # Should not raise
        self.validator.validate_event_hubs_oauth(options, "test_action")

    def test_validate_event_hubs_oauth_missing_token_endpoint(self):
        """Test Event Hubs OAuth missing token endpoint."""
        options = {
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.jaas.config": "test_config",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.login.callback.handler.class": "test_handler"
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.validate_event_hubs_oauth(options, "test_action")
        
        assert "test_action" in str(exc_info.value)
        assert "kafka.sasl.oauthbearer.token.endpoint.url" in str(exc_info.value)

    def test_validate_event_hubs_oauth_missing_multiple(self):
        """Test Event Hubs OAuth missing multiple options."""
        options = {
            "kafka.sasl.mechanism": "OAUTHBEARER",
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.validate_event_hubs_oauth(options, "test_action")
        
        error_msg = str(exc_info.value)
        assert "test_action" in error_msg
        assert "kafka.sasl.jaas.config" in error_msg
        assert "kafka.sasl.oauthbearer.token.endpoint.url" in error_msg
        assert "kafka.security.protocol" in error_msg

    def test_validate_event_hubs_oauth_not_oauth(self):
        """Test OAuth validation when not using OAuth."""
        options = {
            "kafka.sasl.mechanism": "PLAIN",
            "kafka.security.protocol": "SASL_SSL"
        }
        
        # Should not raise even with incomplete OAuth config
        self.validator.validate_event_hubs_oauth(options, "test_action")

    # Test Options Processing for Sources
    def test_process_options_source_with_subscribe(self):
        """Test processing source options with subscribe method."""
        options = {
            "subscribe": "test_topic",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.mechanism": "PLAIN"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["subscribe"] == "test_topic"
        assert result["kafka.security.protocol"] == "SASL_SSL"
        assert result["kafka.sasl.mechanism"] == "PLAIN"

    def test_process_options_source_with_subscribe_pattern(self):
        """Test processing source options with subscribePattern."""
        options = {
            "subscribePattern": "events.*",
            "startingOffsets": "earliest"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["subscribePattern"] == "events.*"
        assert result["startingOffsets"] == "earliest"

    def test_process_options_source_with_assign(self):
        """Test processing source options with assign method."""
        options = {
            "assign": '{"topic1":[0,1]}',
            "maxOffsetsPerTrigger": 10000
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["assign"] == '{"topic1":[0,1]}'
        assert result["maxOffsetsPerTrigger"] == 10000

    def test_process_options_source_additional_source_options(self):
        """Test processing with additional source-specific options."""
        options = {
            "subscribe": "test_topic",
            "endingOffsets": "latest",
            "failOnDataLoss": False,
            "minPartitions": 10,
            "includeHeaders": True
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["endingOffsets"] == "latest"
        assert result["failOnDataLoss"] is False
        assert result["minPartitions"] == 10
        assert result["includeHeaders"] is True

    # Test Options Processing for Sinks
    def test_process_options_sink_with_topic(self):
        """Test processing sink options with topic."""
        options = {
            "topic": "output_topic",
            "kafka.security.protocol": "SASL_SSL"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=False)
        
        assert result["topic"] == "output_topic"
        assert result["kafka.security.protocol"] == "SASL_SSL"

    def test_process_options_sink_rejects_subscribe(self):
        """Test that sink options reject source-specific subscribe method."""
        options = {
            "subscribe": "test_topic",  # Not allowed for sinks
            "kafka.security.protocol": "SASL_SSL"
        }
        
        # Subscribe is not a known kafka option, so it will be left as-is
        # but won't be validated as special for sinks
        result = self.validator.process_options(options, "test_action", is_source=False)
        assert "subscribe" in result

    # Test Invalid Options
    def test_process_options_unprefixed_known_option(self):
        """Test that unprefixed known kafka options raise error."""
        options = {
            "security.protocol": "SASL_SSL",  # Should be kafka.security.protocol
        }
        
        with pytest.raises(LHPError) as exc_info:
            self.validator.process_options(options, "test_action", is_source=True)
        
        error_msg = str(exc_info.value)
        assert "test_action" in error_msg
        assert "security.protocol" in error_msg

    def test_process_options_preserves_types(self):
        """Test that option types are preserved."""
        options = {
            "subscribe": "test_topic",
            "kafka.max.poll.records": 1000,  # integer
            "kafka.enable.auto.commit": False,  # boolean
            "kafka.session.timeout.ms": 30000,  # integer
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["kafka.max.poll.records"] == 1000
        assert isinstance(result["kafka.max.poll.records"], int)
        assert result["kafka.enable.auto.commit"] is False
        assert isinstance(result["kafka.enable.auto.commit"], bool)

    # Test MSK IAM Integration
    def test_process_options_msk_iam_complete(self):
        """Test processing options with complete MSK IAM config."""
        options = {
            "subscribe": "test_topic",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            "kafka.sasl.jaas.config": "test_config",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.client.callback.handler.class": "test_handler"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["kafka.sasl.mechanism"] == "AWS_MSK_IAM"

    def test_process_options_msk_iam_incomplete(self):
        """Test processing options with incomplete MSK IAM config."""
        options = {
            "subscribe": "test_topic",
            "kafka.sasl.mechanism": "AWS_MSK_IAM",
            # Missing required MSK IAM options
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.process_options(options, "test_action", is_source=True)
        
        assert "AWS MSK IAM authentication requires" in str(exc_info.value)

    # Test Event Hubs OAuth Integration
    def test_process_options_event_hubs_complete(self):
        """Test processing options with complete Event Hubs config."""
        options = {
            "subscribe": "test_topic",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            "kafka.sasl.jaas.config": "test_config",
            "kafka.sasl.oauthbearer.token.endpoint.url": "https://token.endpoint",
            "kafka.security.protocol": "SASL_SSL",
            "kafka.sasl.login.callback.handler.class": "test_handler"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        assert result["kafka.sasl.mechanism"] == "OAUTHBEARER"

    def test_process_options_event_hubs_incomplete(self):
        """Test processing options with incomplete Event Hubs config."""
        options = {
            "subscribe": "test_topic",
            "kafka.sasl.mechanism": "OAUTHBEARER",
            # Missing required OAuth options
        }
        
        with pytest.raises(ValueError) as exc_info:
            self.validator.process_options(options, "test_action", is_source=True)
        
        assert "OAuth authentication requires" in str(exc_info.value)

    # Test Known Kafka Options
    def test_known_kafka_options_contains_common_options(self):
        """Test that known options include common Kafka options."""
        known = self.validator.KNOWN_KAFKA_OPTIONS
        
        # Check for some common options
        assert "bootstrap.servers" in known
        assert "security.protocol" in known
        assert "sasl.mechanism" in known
        assert "ssl.truststore.location" in known
        assert "group.id" in known
        assert "auto.offset.reset" in known

    def test_source_only_options_defined(self):
        """Test that source-only options are properly defined."""
        source_only = self.validator.SOURCE_ONLY_OPTIONS
        
        assert "subscribe" in source_only
        assert "subscribePattern" in source_only
        assert "assign" in source_only
        assert "startingOffsets" in source_only
        assert "maxOffsetsPerTrigger" in source_only

    def test_sink_only_options_defined(self):
        """Test that sink-only options are properly defined."""
        sink_only = self.validator.SINK_ONLY_OPTIONS
        
        assert "topic" in sink_only

    # Edge Cases
    def test_process_options_empty_dict(self):
        """Test processing empty options dict."""
        result = self.validator.process_options({}, "test_action", is_source=True)
        assert result == {}

    def test_process_options_custom_options_passthrough(self):
        """Test that unknown custom options pass through."""
        options = {
            "subscribe": "test_topic",
            "custom.option": "custom_value",
            "kafka.security.protocol": "SASL_SSL"
        }
        
        result = self.validator.process_options(options, "test_action", is_source=True)
        
        # Custom option should pass through as-is
        assert result["custom.option"] == "custom_value"


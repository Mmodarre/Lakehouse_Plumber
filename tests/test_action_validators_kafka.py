"""
Tests for Kafka-specific action validation in action_validators.py
"""
import pytest
from lhp.core.validators import LoadActionValidator
from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
from lhp.models.config import Action


class TestKafkaActionValidator:
    """Test suite for Kafka source validation."""
    
    def setup_method(self):
        """Set up test fixtures."""
        action_registry = ActionRegistry()
        field_validator = ConfigFieldValidator()
        self.validator = LoadActionValidator(action_registry, field_validator)
    
    def test_valid_kafka_with_subscribe(self):
        """Test valid Kafka configuration with subscribe method."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic"
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert len(errors) == 0
    
    def test_valid_kafka_with_subscribe_pattern(self):
        """Test valid Kafka configuration with subscribePattern method."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribePattern": "test.*"
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert len(errors) == 0
    
    def test_valid_kafka_with_assign(self):
        """Test valid Kafka configuration with assign method."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "assign": '[{"topic":"test","partition":0,"offset":0}]'
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert len(errors) == 0
    
    def test_missing_bootstrap_servers(self):
        """Test that missing bootstrap_servers is detected."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "subscribe": "test_topic"
                # Missing bootstrap_servers
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert any("bootstrap_servers" in error for error in errors)
    
    def test_missing_subscription_method(self):
        """Test that missing subscription method is detected."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092"
                # Missing subscribe/subscribePattern/assign
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert any("subscribe" in error or "subscribePattern" in error or "assign" in error 
                  for error in errors)
    
    def test_multiple_subscription_methods(self):
        """Test that multiple subscription methods are detected as error."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "subscribePattern": "test.*"  # Can't have both
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        # Check if any error mentions having only one subscription method
        assert any("ONE" in error for error in errors) or any("subscribePattern" in error for error in errors)
    
    def test_all_three_subscription_methods(self):
        """Test that all three subscription methods together is an error."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "subscribePattern": "test.*",
                "assign": '[{"topic":"test","partition":0}]'
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        # Check if any error mentions having only one subscription method or complains about extra fields
        assert any("ONE" in error for error in errors) or any("subscribePattern" in error for error in errors) or any("assign" in error for error in errors)
    
    def test_kafka_with_options(self):
        """Test valid Kafka configuration with additional options."""
        action = Action(
            name="test_kafka",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "startingOffsets": "earliest"
                }
            },
            target="v_test",
            readMode="stream"
        )
        errors = self.validator.validate(action, "test_action")
        assert len(errors) == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


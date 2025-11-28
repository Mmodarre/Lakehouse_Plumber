"""Test sink action validation."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.core.validators import WriteActionValidator
from lhp.core.action_registry import ActionRegistry
from lhp.core.config_field_validator import ConfigFieldValidator
import logging


class TestSinkActionValidation:
    """Test sink write action validation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.logger = logging.getLogger(__name__)
        self.action_registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()
        self.validator = WriteActionValidator(
            self.action_registry, self.field_validator, self.logger
        )

    # Delta Sink Validation
    def test_valid_delta_sink(self):
        """Test valid Delta sink configuration."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {"tableName": "external_catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        
        # Should have no errors (or only warnings)
        assert len(errors) == 0

    def test_delta_sink_missing_options(self):
        """Test Delta sink without options."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta"
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        
        # Should error about missing options
        assert any("options" in err.lower() for err in errors)

    def test_delta_sink_with_path(self):
        """Test valid Delta sink with path instead of tableName."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {"path": "/mnt/delta/my_table"}
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        assert len(errors) == 0

    def test_delta_sink_with_both_tablename_and_path(self):
        """Test Delta sink with both tableName and path (should error)."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {
                    "tableName": "catalog.schema.table",
                    "path": "/mnt/delta/table"
                }
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        assert any("cannot have both" in err.lower() for err in errors)

    def test_delta_sink_with_neither_tablename_nor_path(self):
        """Test Delta sink without tableName or path (should error)."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {"checkpointLocation": "/tmp/cp"}
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        assert any("must include either" in err.lower() for err in errors)

    def test_delta_sink_passes_through_extra_options(self):
        """Test Delta sink allows extra options to pass through."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {
                    "tableName": "catalog.schema.table",
                    "checkpointLocation": "/tmp/cp",
                    "mergeSchema": "true",
                    "optimizeWrite": "true"
                }
            }
        )
        
        errors = self.validator.validate(action, "test_delta_sink")
        # Should have no errors - extra options pass through
        assert len(errors) == 0

    # Kafka Sink Validation
    def test_valid_kafka_sink(self):
        """Test valid Kafka sink configuration."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "bootstrap_servers": "localhost:9092",
                "topic": "output_topic"
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        assert len(errors) == 0

    def test_kafka_sink_with_options(self):
        """Test Kafka sink with additional options."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "bootstrap_servers": "kafka1:9092,kafka2:9092",
                "topic": "events_topic",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "PLAIN"
                }
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        assert len(errors) == 0

    def test_kafka_sink_missing_bootstrap_servers(self):
        """Test Kafka sink without bootstrap_servers."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "topic": "output_topic"
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        assert any("bootstrap_servers" in err.lower() for err in errors)

    def test_kafka_sink_missing_topic(self):
        """Test Kafka sink without topic."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "bootstrap_servers": "localhost:9092"
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        assert any("topic" in err.lower() for err in errors)

    # Event Hubs Sink Validation (as Kafka)
    def test_valid_event_hubs_sink(self):
        """Test valid Event Hubs sink configuration."""
        action = Action(
            name="test_event_hubs_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "event_hubs_output",
                "bootstrap_servers": "my-ns.servicebus.windows.net:9093",
                "topic": "my-event-hub",
                "options": {
                    "kafka.sasl.mechanism": "OAUTHBEARER",
                    "kafka.sasl.jaas.config": "test_config",
                    "kafka.sasl.oauthbearer.token.endpoint.url": "https://token.endpoint",
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.login.callback.handler.class": "test_handler"
                }
            }
        )
        
        errors = self.validator.validate(action, "test_event_hubs_sink")
        
        assert len(errors) == 0

    # Custom Sink Validation
    def test_valid_custom_sink(self):
        """Test valid custom sink configuration."""
        action = Action(
            name="test_custom_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "my_custom_sink",
                "module_path": "sinks/my_sink.py",
                "custom_sink_class": "MyCustomDataSink",
                "options": {"endpoint": "https://api.example.com"}
            }
        )
        
        errors = self.validator.validate(action, "test_custom_sink")
        
        assert len(errors) == 0

    def test_custom_sink_missing_module_path(self):
        """Test custom sink without module_path."""
        action = Action(
            name="test_custom_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "my_custom_sink",
                "custom_sink_class": "MyCustomDataSink"
            }
        )
        
        errors = self.validator.validate(action, "test_custom_sink")
        
        assert any("module_path" in err.lower() for err in errors)

    def test_custom_sink_missing_class_name(self):
        """Test custom sink without custom_sink_class."""
        action = Action(
            name="test_custom_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "custom",
                "sink_name": "my_custom_sink",
                "module_path": "sinks/my_sink.py"
            }
        )
        
        errors = self.validator.validate(action, "test_custom_sink")
        
        assert any("custom_sink_class" in err.lower() for err in errors)

    # General Sink Validation
    def test_sink_missing_sink_type(self):
        """Test sink without sink_type."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_name": "test_sink"
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        assert any("sink_type" in err.lower() for err in errors)

    def test_sink_missing_sink_name(self):
        """Test sink without sink_name."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        assert any("sink_name" in err.lower() for err in errors)

    def test_sink_missing_source(self):
        """Test sink without source."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "test_sink",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        assert any("source" in err.lower() for err in errors)

    def test_sink_invalid_source_type(self):
        """Test sink with invalid source type."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source={"invalid": "source"},  # Should be string or list
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "test_sink",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        assert any("source must be a string or list" in err.lower() for err in errors)

    def test_sink_with_list_of_sources(self):
        """Test sink with multiple sources."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source=["v_data1", "v_data2"],
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "test_sink",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        # Should accept list of sources
        # May have an error about options but not about source format
        assert not any("source must be a string or list" in err.lower() for err in errors)

    def test_sink_unknown_sink_type(self):
        """Test sink with unknown sink_type."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "unknown_type",
                "sink_name": "test_sink"
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        assert any("unknown sink_type" in err.lower() for err in errors)

    def test_sink_kafka_invalid_options(self):
        """Test Kafka sink with invalid options (unprefixed)."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "bootstrap_servers": "localhost:9092",
                "topic": "output_topic",
                "options": {
                    "security.protocol": "SASL_SSL"  # Should be kafka.security.protocol
                }
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        # Should catch the unprefixed option
        assert len(errors) > 0
    
    def test_sink_missing_sink_type_early_return(self):
        """Test that missing sink_type causes early return (sink_name not checked)."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                # Missing sink_type - should return early
                "sink_name": "test_sink"
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        # Should have error about sink_type but NOT sink_name (early return)
        assert any("sink_type" in err.lower() for err in errors)
        # sink_name error should not be present due to early return
        assert not any("sink_name" in err.lower() for err in errors)
    
    def test_sink_missing_sink_name_with_sink_type(self):
        """Test sink with sink_type but missing sink_name."""
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                # Missing sink_name
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator.validate(action, "test_sink")
        
        # Should have error about sink_name
        assert any("sink_name" in err.lower() for err in errors)
    
    def test_kafka_sink_validator_exception_handling(self):
        """Test Kafka sink when validator raises exception."""
        action = Action(
            name="test_kafka_sink",
            type=ActionType.WRITE,
            source="v_processed_data",
            write_target={
                "type": "sink",
                "sink_type": "kafka",
                "sink_name": "kafka_output",
                "bootstrap_servers": "localhost:9092",
                "topic": "output_topic",
                "options": {
                    "kafka.sasl.mechanism": "OAUTHBEARER"
                    # Missing required OAuth options - validator will raise exception
                }
            }
        )
        
        errors = self.validator.validate(action, "test_kafka_sink")
        
        # Should catch validator exception and add to errors
        assert len(errors) > 0
        # Error should be from validator
        assert any("oauth" in err.lower() or "required" in err.lower() for err in errors)
    
    def test_delta_sink_both_tablename_and_path_error(self):
        """Test Delta sink error when both tableName and path are provided."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {
                    "tableName": "catalog.schema.table",
                    "path": "/mnt/delta/table"
                }
            }
        )
        
        errors = self.validator._validate_delta_sink(action, "test_delta_sink")
        
        # Should have error about both being present
        assert any("cannot have both" in err.lower() for err in errors)
    
    def test_delta_sink_neither_tablename_nor_path_error(self):
        """Test Delta sink error when neither tableName nor path are provided."""
        action = Action(
            name="test_delta_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "external_delta",
                "options": {}  # Empty options - has options dict but no tableName or path
            }
        )
        
        errors = self.validator._validate_delta_sink(action, "test_delta_sink")
        
        # Should have error about missing both tableName and path
        assert len(errors) > 0
        # Error message mentions both tableName and path
        assert any("tablename" in err.lower() or "path" in err.lower() for err in errors)
    
    def test_validate_sink_error_paths(self):
        """Test error paths in _validate_sink method."""
        # Test missing sink_type (early return)
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_name": "test_sink"
            }
        )
        
        errors = self.validator._validate_sink(action, "test_sink")
        assert len(errors) == 1
        assert "sink_type" in errors[0].lower()
        
        # Test missing sink_name (continues after sink_type check)
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source="v_input_data",
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator._validate_sink(action, "test_sink")
        assert any("sink_name" in err.lower() for err in errors)
        
        # Test missing source
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "test_sink",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator._validate_sink(action, "test_sink")
        assert any("source" in err.lower() for err in errors)
        
        # Test invalid source type
        action = Action(
            name="test_sink",
            type=ActionType.WRITE,
            source={"invalid": "source"},
            write_target={
                "type": "sink",
                "sink_type": "delta",
                "sink_name": "test_sink",
                "options": {"tableName": "catalog.schema.table"}
            }
        )
        
        errors = self.validator._validate_sink(action, "test_sink")
        assert any("source must be a string or list" in err.lower() for err in errors)


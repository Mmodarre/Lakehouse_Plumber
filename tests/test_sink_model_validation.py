"""Test sink model validation."""

import pytest
from pydantic import ValidationError
from lhp.models.config import WriteTarget, WriteTargetType


class TestSinkModelValidation:
    """Test sink write target model validation."""

    def test_sink_type_enum(self):
        """Test that SINK is a valid WriteTargetType."""
        assert WriteTargetType.SINK == "sink"
        assert "sink" in [t.value for t in WriteTargetType]

    def test_delta_sink_minimal(self):
        """Test minimal valid Delta sink configuration."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="delta",
            sink_name="test_delta_sink",
            options={"tableName": "catalog.schema.table"}
        )
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "delta"
        assert write_target.sink_name == "test_delta_sink"
        assert write_target.options["tableName"] == "catalog.schema.table"

    def test_kafka_sink_minimal(self):
        """Test minimal valid Kafka sink configuration."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="kafka",
            sink_name="test_kafka_sink",
            bootstrap_servers="localhost:9092",
            topic="test_topic"
        )
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "kafka"
        assert write_target.sink_name == "test_kafka_sink"
        assert write_target.bootstrap_servers == "localhost:9092"
        assert write_target.topic == "test_topic"

    def test_kafka_sink_with_options(self):
        """Test Kafka sink with additional options."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="kafka",
            sink_name="test_kafka_sink",
            bootstrap_servers="kafka1:9092,kafka2:9092",
            topic="events_topic",
            options={
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "PLAIN"
            }
        )
        
        assert write_target.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert write_target.topic == "events_topic"
        assert write_target.options["kafka.security.protocol"] == "SASL_SSL"
        assert write_target.options["kafka.sasl.mechanism"] == "PLAIN"

    def test_event_hubs_sink(self):
        """Test Event Hubs sink configuration (as Kafka sink with OAuth)."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="kafka",
            sink_name="test_event_hubs_sink",
            bootstrap_servers="my-ns.servicebus.windows.net:9093",
            topic="my-event-hub",
            options={
                "kafka.sasl.mechanism": "OAUTHBEARER",
                "kafka.sasl.jaas.config": "test_config",
                "kafka.sasl.oauthbearer.token.endpoint.url": "https://token.endpoint",
                "kafka.security.protocol": "SASL_SSL"
            }
        )
        
        assert write_target.sink_type == "kafka"
        assert write_target.bootstrap_servers == "my-ns.servicebus.windows.net:9093"
        assert write_target.options["kafka.sasl.mechanism"] == "OAUTHBEARER"

    def test_custom_sink_minimal(self):
        """Test minimal valid custom sink configuration."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="custom",
            sink_name="test_custom_sink",
            module_path="sinks/my_sink.py",
            custom_sink_class="MyCustomDataSink"
        )
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "custom"
        assert write_target.sink_name == "test_custom_sink"
        assert write_target.module_path == "sinks/my_sink.py"
        assert write_target.custom_sink_class == "MyCustomDataSink"

    def test_custom_sink_with_options(self):
        """Test custom sink with custom options."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="custom",
            sink_name="test_custom_sink",
            module_path="sinks/api_sink.py",
            custom_sink_class="APIDataSink",
            options={
                "endpoint": "https://api.example.com",
                "api_key": "secret_key",
                "batch_size": 1000
            }
        )
        
        assert write_target.options["endpoint"] == "https://api.example.com"
        assert write_target.options["api_key"] == "secret_key"
        assert write_target.options["batch_size"] == 1000

    def test_sink_without_database_table(self):
        """Test that sinks don't require database and table fields."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="kafka",
            sink_name="test_sink",
            bootstrap_servers="localhost:9092",
            topic="test_topic"
        )
        
        # Should not raise - database and table are optional for sinks
        assert write_target.database is None
        assert write_target.table is None

    def test_streaming_table_still_requires_database_table(self):
        """Test that streaming tables still work with database/table."""
        write_target = WriteTarget(
            type=WriteTargetType.STREAMING_TABLE,
            database="catalog.schema",
            table="test_table"
        )
        
        assert write_target.type == WriteTargetType.STREAMING_TABLE
        assert write_target.database == "catalog.schema"
        assert write_target.table == "test_table"

    def test_materialized_view_still_works(self):
        """Test that materialized views still work."""
        write_target = WriteTarget(
            type=WriteTargetType.MATERIALIZED_VIEW,
            database="catalog.schema",
            table="test_view"
        )
        
        assert write_target.type == WriteTargetType.MATERIALIZED_VIEW
        assert write_target.database == "catalog.schema"
        assert write_target.table == "test_view"

    def test_sink_optional_fields_default_to_none(self):
        """Test that sink-specific optional fields default to None."""
        write_target = WriteTarget(
            type=WriteTargetType.STREAMING_TABLE,
            database="catalog.schema",
            table="test_table"
        )
        
        # Sink fields should be None for non-sink write targets
        assert write_target.sink_type is None
        assert write_target.sink_name is None
        assert write_target.bootstrap_servers is None
        assert write_target.topic is None
        assert write_target.module_path is None
        assert write_target.custom_sink_class is None

    def test_sink_with_comment(self):
        """Test sink with comment field."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="delta",
            sink_name="test_sink",
            comment="Test sink for external Delta table",
            options={"tableName": "external.schema.table"}
        )
        
        assert write_target.comment == "Test sink for external Delta table"

    def test_dict_conversion(self):
        """Test that sink write target can be converted to dict."""
        write_target = WriteTarget(
            type=WriteTargetType.SINK,
            sink_type="kafka",
            sink_name="test_sink",
            bootstrap_servers="localhost:9092",
            topic="test_topic",
            options={"kafka.security.protocol": "PLAINTEXT"}
        )
        
        # Convert to dict (Pydantic model_dump)
        write_target_dict = write_target.model_dump()
        
        assert write_target_dict["type"] == "sink"
        assert write_target_dict["sink_type"] == "kafka"
        assert write_target_dict["sink_name"] == "test_sink"
        assert write_target_dict["bootstrap_servers"] == "localhost:9092"
        assert write_target_dict["topic"] == "test_topic"

    def test_from_dict(self):
        """Test creating sink write target from dict."""
        config_dict = {
            "type": "sink",
            "sink_type": "kafka",
            "sink_name": "test_sink",
            "bootstrap_servers": "localhost:9092",
            "topic": "test_topic"
        }
        
        write_target = WriteTarget(**config_dict)
        
        assert write_target.type == WriteTargetType.SINK
        assert write_target.sink_type == "kafka"
        assert write_target.sink_name == "test_sink"








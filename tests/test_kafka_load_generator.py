"""Test Kafka load generator implementation."""

import pytest
from lhp.generators.load.kafka import KafkaLoadGenerator
from lhp.models.config import Action


class TestKafkaLoadGenerator:
    """Test Kafka load generator functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.generator = KafkaLoadGenerator()

    def test_basic_kafka_streaming_read(self):
        """Test basic Kafka streaming read with subscribe."""
        action = Action(
            name="test_kafka_load",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
            },
            target="v_kafka_data",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        # Check basic structure
        assert "@dp.temporary_view()" in result
        assert "def v_kafka_data():" in result
        assert "spark.readStream" in result
        assert '.format("kafka")' in result
        assert '.option("kafka.bootstrap.servers", "localhost:9092")' in result
        assert '.option("subscribe", "test_topic")' in result
        assert ".load()" in result
        assert "return df" in result

    def test_subscribe_method(self):
        """Test subscribe method with multiple topics."""
        action = Action(
            name="test_subscribe",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "kafka1:9092,kafka2:9092",
                "subscribe": "topic1,topic2,topic3",
            },
            target="v_kafka_multi",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        assert '.option("kafka.bootstrap.servers", "kafka1:9092,kafka2:9092")' in result
        assert '.option("subscribe", "topic1,topic2,topic3")' in result

    def test_subscribePattern_method(self):
        """Test subscribePattern method with regex."""
        action = Action(
            name="test_pattern",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribePattern": "events.*",
            },
            target="v_kafka_pattern",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        assert '.option("subscribePattern", "events.*")' in result
        assert "subscribe" not in result or '.option("subscribePattern"' in result

    def test_assign_method(self):
        """Test assign method with specific partitions."""
        action = Action(
            name="test_assign",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "assign": '{"topic1":[0,1],"topic2":[2,4]}',
            },
            target="v_kafka_assign",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        # The JSON string has escaped quotes in the generated code
        assert '.option("assign", "{\\"topic1\\":[0,1],\\"topic2\\":[2,4]}")' in result

    def test_multiple_subscription_methods_error(self):
        """Test error when multiple subscription methods are provided."""
        action = Action(
            name="test_error",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "topic1",
                "subscribePattern": "topic.*",
            },
            target="v_kafka_error",
            readMode="stream"
        )

        with pytest.raises(ValueError, match="can only have ONE of"):
            self.generator.generate(action, {})

    def test_missing_bootstrap_servers_error(self):
        """Test error when bootstrap_servers is missing."""
        action = Action(
            name="test_error",
            type="load",
            source={
                "type": "kafka",
                "subscribe": "topic1",
            },
            target="v_kafka_error",
            readMode="stream"
        )

        with pytest.raises(ValueError, match="must have 'bootstrap_servers'"):
            self.generator.generate(action, {})

    def test_missing_subscription_method_error(self):
        """Test error when no subscription method is provided."""
        action = Action(
            name="test_error",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
            },
            target="v_kafka_error",
            readMode="stream"
        )

        with pytest.raises(ValueError, match="must have one of"):
            self.generator.generate(action, {})

    def test_options_with_kafka_prefix(self):
        """Test options with kafka.* prefix."""
        action = Action(
            name="test_options",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "kafka.group.id": "my-consumer-group",
                    "kafka.session.timeout.ms": 30000,
                    "startingOffsets": "earliest",
                    "failOnDataLoss": False,
                }
            },
            target="v_kafka_options",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        assert '.option("kafka.group.id", "my-consumer-group")' in result
        assert '.option("kafka.session.timeout.ms", 30000)' in result
        assert '.option("startingOffsets", "earliest")' in result
        assert '.option("failOnDataLoss", False)' in result

    def test_ssl_configuration(self):
        """Test SSL configuration options."""
        action = Action(
            name="test_ssl",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9093",
                "subscribe": "secure_topic",
                "options": {
                    "kafka.security.protocol": "SSL",
                    "kafka.ssl.truststore.location": "/path/to/truststore.jks",
                    "kafka.ssl.truststore.password": "truststore-password",
                    "kafka.ssl.keystore.location": "/path/to/keystore.jks",
                    "kafka.ssl.keystore.password": "keystore-password",
                }
            },
            target="v_kafka_ssl",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        assert '.option("kafka.security.protocol", "SSL")' in result
        assert '.option("kafka.ssl.truststore.location", "/path/to/truststore.jks")' in result
        assert '.option("kafka.ssl.truststore.password", "truststore-password")' in result
        assert '.option("kafka.ssl.keystore.location", "/path/to/keystore.jks")' in result
        assert '.option("kafka.ssl.keystore.password", "keystore-password")' in result

    def test_value_type_preservation(self):
        """Test that YAML value types are preserved."""
        action = Action(
            name="test_types",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "failOnDataLoss": False,           # boolean
                    "minPartitions": 10,                # number
                    "startingOffsets": "earliest",     # string
                    "kafka.auto.commit.enable": True,  # boolean
                }
            },
            target="v_kafka_types",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        # Check boolean values are not quoted
        assert '.option("failOnDataLoss", False)' in result
        assert '.option("kafka.auto.commit.enable", True)' in result

        # Check numbers are not quoted
        assert '.option("minPartitions", 10)' in result

        # Check strings are quoted
        assert '.option("startingOffsets", "earliest")' in result

    def test_operational_metadata_integration(self):
        """Test operational metadata integration with proper project config."""
        # Create a project config with operational metadata definitions
        from lhp.models.config import FlowGroup, ProjectConfig, ProjectOperationalMetadataConfig, MetadataColumnConfig
        
        project_config = ProjectConfig(
            name="test_project",
            version="1.0",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_processing_timestamp": MetadataColumnConfig(
                        expression="current_timestamp()",
                        description="Processing timestamp",
                        applies_to=["view", "streaming_table", "materialized_view"]
                    )
                }
            )
        )
        
        action = Action(
            name="test_metadata",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
            },
            target="v_kafka_metadata",
            readMode="stream",
            operational_metadata=["_processing_timestamp"]
        )

        # Create a mock flowgroup context
        flowgroup = FlowGroup(
            pipeline="test_pipeline",
            flowgroup="test_flowgroup",
            actions=[]
        )

        result = self.generator.generate(action, {
            "flowgroup": flowgroup,
            "project_config": project_config
        })

        # Check that operational metadata column is added
        assert "# Add operational metadata columns" in result
        assert "df.withColumn('_processing_timestamp', current_timestamp())" in result

    def test_starting_offsets_options(self):
        """Test startingOffsets configuration."""
        # Test with earliest
        action_earliest = Action(
            name="test_earliest",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "startingOffsets": "earliest"
                }
            },
            target="v_kafka_earliest",
            readMode="stream"
        )

        result = self.generator.generate(action_earliest, {})
        assert '.option("startingOffsets", "earliest")' in result

        # Test with latest
        action_latest = Action(
            name="test_latest",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "startingOffsets": "latest"
                }
            },
            target="v_kafka_latest",
            readMode="stream"
        )

        result = self.generator.generate(action_latest, {})
        assert '.option("startingOffsets", "latest")' in result

    def test_failOnDataLoss_options(self):
        """Test failOnDataLoss configuration."""
        action = Action(
            name="test_fail_on_data_loss",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    "failOnDataLoss": False
                }
            },
            target="v_kafka_no_fail",
            readMode="stream"
        )

        result = self.generator.generate(action, {})
        assert '.option("failOnDataLoss", False)' in result

    def test_comprehensive_example(self):
        """Test a comprehensive example with all features."""
        action = Action(
            name="comprehensive_kafka_load",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "kafka1.example.com:9092,kafka2.example.com:9092",
                "subscribe": "events,logs,metrics",
                "options": {
                    "startingOffsets": "latest",
                    "failOnDataLoss": False,
                    "minPartitions": 5,
                    "kafka.group.id": "lhp-consumer-group",
                    "kafka.session.timeout.ms": 30000,
                    "kafka.heartbeat.interval.ms": 3000,
                    "kafka.max.poll.records": 500,
                    "kafka.security.protocol": "SSL",
                    "kafka.ssl.truststore.location": "/path/to/truststore.jks",
                    "kafka.ssl.truststore.password": "secret-password",
                }
            },
            target="v_kafka_comprehensive",
            readMode="stream",
            description="Comprehensive Kafka load with SSL and custom options"
        )

        result = self.generator.generate(action, {})

        # Check basic structure
        assert "@dp.temporary_view()" in result
        assert "def v_kafka_comprehensive():" in result
        assert "Comprehensive Kafka load with SSL and custom options" in result
        assert "spark.readStream" in result
        assert '.format("kafka")' in result

        # Check all options are included
        assert '.option("kafka.bootstrap.servers", "kafka1.example.com:9092,kafka2.example.com:9092")' in result
        assert '.option("subscribe", "events,logs,metrics")' in result
        assert '.option("startingOffsets", "latest")' in result
        assert '.option("failOnDataLoss", False)' in result
        assert '.option("minPartitions", 5)' in result
        assert '.option("kafka.group.id", "lhp-consumer-group")' in result
        assert '.option("kafka.session.timeout.ms", 30000)' in result
        assert '.option("kafka.heartbeat.interval.ms", 3000)' in result
        assert '.option("kafka.max.poll.records", 500)' in result
        assert '.option("kafka.security.protocol", "SSL")' in result
        assert '.option("kafka.ssl.truststore.location", "/path/to/truststore.jks")' in result

        # Check structure
        assert ".load()" in result
        assert "return df" in result

    def test_readmode_must_be_stream(self):
        """Test that Kafka requires stream mode."""
        action = Action(
            name="test_batch_error",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
            },
            target="v_kafka_batch",
            readMode="batch"
        )

        with pytest.raises(ValueError, match="requires readMode='stream'"):
            self.generator.generate(action, {})

    def test_imports_generated(self):
        """Test that required imports are added."""
        action = Action(
            name="test_imports",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
            },
            target="v_kafka_imports",
            readMode="stream"
        )

        self.generator.generate(action, {})

        # Check that pipeline as dp import was added
        assert "from pyspark import pipelines as dp" in self.generator.imports

    def test_quoted_values_escaped(self):
        """Test that inner quotes in string values are properly escaped."""
        action = Action(
            name="test_quotes",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                "subscribe": "test_topic",
                "options": {
                    # This value contains embedded quotes that must be escaped
                    "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="user" password="pass";'
                }
            },
            target="v_kafka_quotes",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        # Check that the quotes are escaped in the generated code
        # The value should contain \" for escaped quotes
        assert '\\"user\\"' in result or 'username=\\"user\\"' in result
        assert '\\"pass\\"' in result or 'password=\\"pass\\"' in result
        
        # Verify it's valid Python by attempting to compile
        try:
            compile(result, '<string>', 'exec')
            assert True  # Compilation succeeded
        except SyntaxError as e:
            pytest.fail(f"Generated code with quotes is not valid Python syntax: {e}")

    def test_backslash_escaping_in_patterns(self):
        """Test that backslashes in regex patterns are properly escaped."""
        action = Action(
            name="test_pattern",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "localhost:9092",
                # Regex pattern with escaped dots (common in Kafka topic patterns)
                "subscribePattern": r".*\.orders\.avro$",
            },
            target="v_kafka_pattern",
            readMode="stream"
        )

        result = self.generator.generate(action, {})

        # Check that backslashes are escaped in the generated code
        # Should have \\ for proper Python string literal
        assert '\\\\.orders\\\\.avro' in result or r'.*\.orders\.avro' in result
        
        # Verify no SyntaxWarning by compiling with warnings as errors
        import warnings
        warnings.simplefilter('error', SyntaxWarning)
        try:
            compile(result, '<string>', 'exec')
            assert True  # Compilation succeeded without warnings
        except SyntaxWarning as e:
            pytest.fail(f"Generated code has invalid escape sequences: {e}")
        except SyntaxError as e:
            pytest.fail(f"Generated code is not valid Python syntax: {e}")
        finally:
            warnings.simplefilter('default', SyntaxWarning)

    def test_msk_iam_authentication(self):
        """Test AWS MSK IAM authentication with instance profile."""
        action = Action(
            name="test_msk_iam",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "b-1.msk-cluster.amazonaws.com:9098",
                "subscribe": "test_topic",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "AWS_MSK_IAM",
                    "kafka.sasl.jaas.config": "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;",
                    "kafka.sasl.client.callback.handler.class": "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                }
            },
            target="v_msk_data",
            readMode="stream"
        )
        result = self.generator.generate(action, {})
        assert "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule" in result
        assert "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler" in result

    def test_msk_iam_with_role_arn(self):
        """Test MSK IAM with specific role ARN in JAAS config."""
        action = Action(
            name="test_msk_role",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "b-1.msk-cluster.amazonaws.com:9098",
                "subscribe": "test_topic",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "AWS_MSK_IAM",
                    "kafka.sasl.jaas.config": 'shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="arn:aws:iam::123456789012:role/MyRole";',
                    "kafka.sasl.client.callback.handler.class": "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                }
            },
            target="v_msk_role",
            readMode="stream"
        )
        result = self.generator.generate(action, {})
        assert '\\"arn:aws:iam::123456789012:role/MyRole\\"' in result or 'arn:aws:iam::123456789012:role/MyRole' in result

    def test_msk_iam_validation_missing_options(self):
        """Test MSK IAM validation fails when required options missing."""
        action = Action(
            name="test_msk_invalid",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "b-1.msk-cluster.amazonaws.com:9098",
                "subscribe": "test_topic",
                "options": {
                    "kafka.sasl.mechanism": "AWS_MSK_IAM"
                }
            },
            target="v_msk_invalid",
            readMode="stream"
        )
        with pytest.raises(ValueError, match="AWS MSK IAM authentication requires"):
            self.generator.generate(action, {})

    def test_event_hubs_oauth_authentication(self):
        """Test Azure Event Hubs OAuth authentication."""
        action = Action(
            name="test_event_hubs",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "my-namespace.servicebus.windows.net:9093",
                "subscribe": "my-event-hub",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "OAUTHBEARER",
                    "kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="abc123" clientSecret="secret" scope="https://my-namespace.servicebus.windows.net/.default" ssl.protocol="SSL";',
                    "kafka.sasl.oauthbearer.token.endpoint.url": "https://login.microsoft.com/tenant-id/oauth2/v2.0/token",
                    "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
                }
            },
            target="v_event_hubs_data",
            readMode="stream"
        )
        result = self.generator.generate(action, {})
        assert "kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule" in result
        assert "OAuthBearerLoginCallbackHandler" in result

    def test_event_hubs_with_secrets(self):
        """Test Event Hubs OAuth with secret placeholders."""
        action = Action(
            name="test_event_hubs_secrets",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "my-namespace.servicebus.windows.net:9093",
                "subscribe": "my-event-hub",
                "options": {
                    "kafka.security.protocol": "SASL_SSL",
                    "kafka.sasl.mechanism": "OAUTHBEARER",
                    "kafka.sasl.jaas.config": 'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="${secret:azure_secrets/client_id}" clientSecret="${secret:azure_secrets/client_secret}" scope="https://my-namespace.servicebus.windows.net/.default" ssl.protocol="SSL";',
                    "kafka.sasl.oauthbearer.token.endpoint.url": "https://login.microsoft.com/${tenant_id}/oauth2/v2.0/token",
                    "kafka.sasl.login.callback.handler.class": "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
                }
            },
            target="v_event_hubs_secrets",
            readMode="stream"
        )
        result = self.generator.generate(action, {})
        assert "clientId=" in result
        assert "clientSecret=" in result

    def test_event_hubs_oauth_validation_missing_options(self):
        """Test Event Hubs OAuth validation fails when options missing."""
        action = Action(
            name="test_oauth_invalid",
            type="load",
            source={
                "type": "kafka",
                "bootstrap_servers": "my-namespace.servicebus.windows.net:9093",
                "subscribe": "test",
                "options": {
                    "kafka.sasl.mechanism": "OAUTHBEARER"
                }
            },
            target="v_oauth_invalid",
            readMode="stream"
        )
        with pytest.raises(ValueError, match="OAuth authentication requires"):
            self.generator.generate(action, {})
    
    def test_missing_bootstrap_servers_in_options(self):
        """Test that missing bootstrap_servers raises error."""
        action = Action(
            name="test_no_bootstrap",
            type="load",
            source={
                "type": "kafka",
                # bootstrap_servers not in source level
                "subscribe": "test_topic",
                "options": {
                    # and not in options either
                    "startingOffsets": "earliest"
                }
            },
            target="v_test",
            readMode="stream"
        )
        with pytest.raises(ValueError, match="must have 'bootstrap_servers'"):
            self.generator.generate(action, {})


if __name__ == "__main__":
    pytest.main([__file__, "-v"])


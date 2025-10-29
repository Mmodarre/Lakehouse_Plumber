# Implement Kafka Load Source Action

## Overview

Add Kafka as a new load source type following the exact pattern established by CloudFiles implementation, with streaming and batch read support.

## Phase 1: Core Implementation (Basic Auth Only)

### 1. Update Config Models

**File**: `src/lhp/models/config.py`

Add Kafka to LoadSourceType enum:

```python
class LoadSourceType(str, Enum):
    CLOUDFILES = "cloudfiles"
    DELTA = "delta"
    SQL = "sql"
    PYTHON = "python"
    JDBC = "jdbc"
    CUSTOM_DATASOURCE = "custom_datasource"
    KAFKA = "kafka"  # ADD THIS
```

### 2. Create Kafka Generator

**File**: `src/lhp/generators/load/kafka.py` (NEW)

Create KafkaLoadGenerator class mirroring CloudFilesLoadGenerator structure:

- Inherit from BaseActionGenerator
- Define known_kafka_options set (all kafka.* prefixed options from Databricks docs)
- Define mandatory_options = {"kafka.bootstrap.servers"} and one of subscribe/subscribePattern/assign
- Implement generate() method
- Implement _process_options() for validating kafka.* prefix
- Implement _check_conflicts() for subscription method conflicts
- Implement _validate_subscription_method() to ensure exactly one is provided
- Support both readMode: stream and readMode: batch
- No schema processing (keep raw 7-column Kafka schema)
- Support operational metadata integration

**Known Kafka Options to Include**:

```python
self.known_kafka_options = {
    "bootstrap.servers",
    "group.id", 
    "session.timeout.ms",
    "ssl.truststore.location",
    "ssl.truststore.password",
    "ssl.keystore.location",
    "ssl.keystore.password",
    "sasl.mechanism",
    "sasl.jaas.config",
    "security.protocol",
    # Add all other kafka.* options from Databricks docs
}
```

### 3. Create Kafka Template

**File**: `src/lhp/templates/load/kafka.py.j2` (NEW)

Create Jinja2 template following cloudfiles.py.j2 pattern:

- Use @dlt.view() decorator
- Support both spark.readStream.format("kafka") and spark.read.format("kafka")
- Handle boolean/number/string option types correctly (no quotes for bool/number)
- Iterate through reader_options with proper type handling
- Add operational metadata support
- Return df with all 7 Kafka columns intact

Template structure (Kafka is always streaming):

```jinja2
@dlt.view()
def {{ target_view }}():
    """{{ description }}"""
    df = spark.readStream \
        .format("kafka")
    {%- for key, value in reader_options.items() %}
    {%- if value is boolean %} \
        .option("{{ key }}", {{ value|string|title }})
    {%- elif value is number %} \
        .option("{{ key }}", {{ value }})
    {%- else %} \
        .option("{{ key }}", "{{ value }}")
    {%- endif %}
    {%- endfor %} \
        .load()
    
    {% if add_operational_metadata %}
    # Add operational metadata columns
    {% for col_name, expression in metadata_columns.items()|sort %}
    df = df.withColumn('{{ col_name }}', {{ expression }})
    {% endfor %}
    {% endif %}
    
    return df
```

### 4. Register Generator

**File**: `src/lhp/core/action_registry.py`

Add to imports:

```python
from ..generators.load.kafka import KafkaLoadGenerator
```

Add to _initialize_generators():

```python
self._load_generators = {
    LoadSourceType.CLOUDFILES: CloudFilesLoadGenerator,
    LoadSourceType.DELTA: DeltaLoadGenerator,
    LoadSourceType.SQL: SQLLoadGenerator,
    LoadSourceType.JDBC: JDBCLoadGenerator,
    LoadSourceType.PYTHON: PythonLoadGenerator,
    LoadSourceType.CUSTOM_DATASOURCE: CustomDatasourceLoadGenerator,
    LoadSourceType.KAFKA: KafkaLoadGenerator,  # ADD THIS
}
```

### 5. Update Exports

**File**: `src/lhp/generators/load/__init__.py`

Add KafkaLoadGenerator to exports:

```python
from .kafka import KafkaLoadGenerator

__all__ = [
    "CloudFilesLoadGenerator",
    "DeltaLoadGenerator",
    "SQLLoadGenerator",
    "JDBCLoadGenerator",
    "PythonLoadGenerator",
    "CustomDatasourceLoadGenerator",
    "KafkaLoadGenerator",  # ADD THIS
]
```

### 6. Add Field Validator Configuration

**File**: `src/lhp/core/config_field_validator.py`

Add kafka to load_source_fields:

```python
self.load_source_fields = {
    # ... existing entries ...
    "kafka": {
        "type",
        "bootstrap_servers",
        "subscribe",
        "subscribePattern",
        "assign",
        "options",
        "readMode",
    },
}
```

### 7. Add Action Validator

**File**: `src/lhp/core/action_validators.py`

Add _validate_kafka_source method to LoadActionValidator:

```python
def _validate_kafka_source(self, action: Action, prefix: str) -> List[str]:
    """Validate Kafka source configuration."""
    errors = []
    
    # Must have bootstrap_servers
    if not action.source.get("bootstrap_servers"):
        errors.append(f"{prefix}: Kafka source must have 'bootstrap_servers'")
    
    # Must have exactly one subscription method
    subscription_methods = [
        action.source.get("subscribe"),
        action.source.get("subscribePattern"),
        action.source.get("assign")
    ]
    
    provided_methods = [m for m in subscription_methods if m is not None]
    
    if len(provided_methods) == 0:
        errors.append(
            f"{prefix}: Kafka source must have one of: 'subscribe', 'subscribePattern', or 'assign'"
        )
    elif len(provided_methods) > 1:
        errors.append(
            f"{prefix}: Kafka source can only have ONE of: 'subscribe', 'subscribePattern', or 'assign'"
        )
    
    return errors
```

Call it from _validate_source_type():

```python
elif load_type == LoadSourceType.KAFKA:
    errors.extend(self._validate_kafka_source(action, prefix))
```

### 8. Create Comprehensive Tests

**File**: `tests/test_kafka_load_generator.py` (NEW)

Create test suite mirroring test_cloudfiles_options.py structure:

- test_basic_kafka_streaming_read
- test_basic_kafka_batch_read
- test_subscribe_method
- test_subscribePattern_method
- test_assign_method
- test_multiple_subscription_methods_error
- test_missing_bootstrap_servers_error
- test_missing_subscription_method_error
- test_options_with_kafka_prefix
- test_ssl_configuration
- test_value_type_preservation
- test_operational_metadata_integration
- test_starting_offsets_options
- test_failOnDataLoss_options
- test_comprehensive_example

### 9. Add to **init**.py for Generators

**File**: `src/lhp/generators/__init__.py`

Ensure KafkaLoadGenerator is exported at package level if needed.

### 10. Update Documentation

**File**: `docs/actions_reference.rst`

Add Kafka load action documentation section after cloudFiles section:

- Example YAML configuration
- Anatomy explanation
- Generated Python code example
- Link to Databricks Kafka documentation
- Note about 7-column schema requiring explicit deserialization

### 11. Create Reference Template

**File**: `Reference_Templates/kafka_streaming_template.yaml` (NEW)

Create comprehensive template showing all Phase 1 features:

- Basic streaming read
- Batch read example
- All three subscription methods (commented alternatives)
- SSL configuration (commented)
- Common kafka.* options documented

## Phase 2: Advanced Authentication (Future)

### AWS MSK IAM Support

- Add MSK-specific options validation
- Update documentation with IAM examples
- Add tests for MSK configuration

### Azure Event Hubs OAuth Support

- Add Entra ID authentication options
- Update documentation with OAuth examples
- Add tests for Event Hubs configuration

### Unity Catalog Service Credentials

- Add databricks.serviceCredential option support
- Update for DBR 16.1+ requirement
- Add tests for UC service credential configuration

## Key Design Decisions

1. **Top-level required fields**: bootstrap_servers + one subscription method (mirrors CloudFiles path/format pattern)
2. **Options dict**: All kafka.* prefixed and optional Kafka options go here
3. **No schema processing**: Keep raw 7-column Kafka schema, users handle deserialization in transform actions
4. **Strict validation**: Enforce exactly one subscription method, required fields
5. **Type preservation**: Boolean/number/string handling in Jinja2 template matches CloudFiles
6. **Operational metadata**: Full integration like CloudFiles
7. **Both read modes**: Support readMode: stream and readMode: batch

## Example YAML Configuration

```yaml
actions:
  - name: load_kafka_events
    type: load
    readMode: stream
    operational_metadata: ["_processing_timestamp"]
    source:
      type: kafka
      bootstrap_servers: "kafka1.example.com:9092,kafka2.example.com:9092"
      subscribe: "events,logs,metrics"
      options:
        startingOffsets: "latest"
        failOnDataLoss: false
        kafka.group.id: "my-consumer-group"
        kafka.ssl.truststore.location: "/path/to/truststore.jks"
        kafka.ssl.truststore.password: "${secret:scope/truststore-password}"
    target: v_kafka_events_raw
    description: "Load events from Kafka topics"
```

## Files to Create

- src/lhp/generators/load/kafka.py
- src/lhp/templates/load/kafka.py.j2
- tests/test_kafka_load_generator.py
- Reference_Templates/kafka_streaming_template.yaml

## Files to Modify

- src/lhp/models/config.py
- src/lhp/core/action_registry.py
- src/lhp/generators/load/**init**.py
- src/lhp/core/config_field_validator.py
- src/lhp/core/action_validators.py
- docs/actions_reference.rst
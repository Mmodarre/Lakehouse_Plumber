# Metadata Columns in Lakehouse Plumber: Complete Guide

## Overview

Lakehouse Plumber supports operational metadata columns that are automatically added to data tables when the `operational_metadata` flag is enabled. This feature provides lineage tracking and data provenance directly in your data tables.

## Current Implementation

### 1. Built-in Metadata Columns

The application currently provides these standard metadata columns:

```python
metadata_columns = {
    '_ingestion_timestamp': 'current_timestamp()',
    '_source_file': 'input_file_name()',
    '_pipeline_run_id': 'metadata.pipeline_run_id',
    '_pipeline_name': 'metadata.pipeline_name',
    '_flowgroup_name': 'metadata.flowgroup_name'
}
```

### 2. How It Works

The operational metadata system works through these components:

- **OperationalMetadata Class** (`src/lhp/utils/operational_metadata.py`): Core logic for metadata handling
- **Configuration Models** (`src/lhp/models/config.py`): Defines `operational_metadata` flags
- **Generators** (`src/lhp/generators/write/`): Integrate metadata into generated code
- **Templates** (`src/lhp/templates/write/`): Render metadata columns in final code

### 3. Configuration Levels

Metadata can be enabled at three levels (in order of precedence):

1. **Action Level** (highest priority)
2. **FlowGroup Level** 
3. **Preset Level** (lowest priority)

## Step-by-Step Guide to Implement Custom Metadata Columns

### Step 1: Understand Current Architecture

The current metadata system generates this type of code in streaming tables:

```python
# Add operational metadata columns
df = df.withColumn('_ingestion_timestamp', F.current_timestamp())
df = df.withColumn('_source_file', F.input_file_name())
df = df.withColumn('_pipeline_run_id', F.lit(spark.conf.get("pipelines.id", "unknown")))
df = df.withColumn('_pipeline_name', F.lit("my_pipeline"))
df = df.withColumn('_flowgroup_name', F.lit("my_flowgroup"))
```

### Step 2: Extend the OperationalMetadata Class

To add custom metadata columns with PySpark code, you need to extend the `OperationalMetadata` class:

```python
# src/lhp/utils/operational_metadata.py

class OperationalMetadata:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        # Existing metadata columns
        self.metadata_columns = {
            '_ingestion_timestamp': 'current_timestamp()',
            '_source_file': 'input_file_name()',
            '_pipeline_run_id': 'metadata.pipeline_run_id',
            '_pipeline_name': 'metadata.pipeline_name',
            '_flowgroup_name': 'metadata.flowgroup_name'
        }
        
        # NEW: Custom metadata columns from config
        self.custom_metadata_columns = {}
    
    def add_custom_metadata_columns(self, custom_columns: Dict[str, str]):
        """Add custom metadata columns from configuration.
        
        Args:
            custom_columns: Dict of column_name -> pyspark_expression
        """
        self.custom_metadata_columns.update(custom_columns)
    
    def generate_metadata_columns(self, target_type: str) -> Dict[str, str]:
        """Generate metadata column expressions based on target type."""
        # Existing columns
        columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': f'F.lit("{self.pipeline_name}")' if hasattr(self, 'pipeline_name') else 'F.lit("unknown")',
            '_flowgroup_name': f'F.lit("{self.flowgroup_name}")' if hasattr(self, 'flowgroup_name') else 'F.lit("unknown")'
        }
        
        # Add source file for streaming tables
        if target_type == 'streaming_table':
            columns['_source_file'] = 'F.input_file_name()'
        
        # Add pipeline run ID if available
        columns['_pipeline_run_id'] = 'F.lit(spark.conf.get("pipelines.id", "unknown"))'
        
        # NEW: Add custom metadata columns
        columns.update(self.custom_metadata_columns)
        
        return columns
```

### Step 3: Update Configuration Models

Extend the configuration models to support custom metadata:

```python
# src/lhp/models/config.py

class Action(BaseModel):
    name: str
    type: ActionType
    # ... existing fields ...
    operational_metadata: Optional[bool] = None
    # NEW: Custom metadata columns
    custom_metadata_columns: Optional[Dict[str, str]] = None

class FlowGroup(BaseModel):
    pipeline: str
    flowgroup: str
    # ... existing fields ...
    operational_metadata: Optional[bool] = None
    # NEW: Custom metadata columns
    custom_metadata_columns: Optional[Dict[str, str]] = None
```

### Step 4: Create YAML Configuration Schema

Now you can define custom metadata columns in your YAML configurations:

#### Option A: At FlowGroup Level

```yaml
# pipelines/bronze_ingestion/customers.yaml
pipeline: bronze_ingestion
flowgroup: customers
operational_metadata: true
custom_metadata_columns:
  _data_quality_score: "F.when(F.col('customer_key').isNotNull(), F.lit('high')).otherwise(F.lit('low'))"
  _record_hash: "F.sha2(F.concat_ws('|', *[F.col(c) for c in df.columns]), 256)"
  _batch_id: "F.lit(spark.conf.get('spark.databricks.clusterUsageTags.runName', 'unknown'))"
  _source_system: "F.lit('CRM')"

presets:
  - bronze_layer

actions:
  - name: load_customers_raw
    type: load
    source:
      type: cloudfiles
      path: "{{ landing_path }}/customers"
      format: json
    target: v_customers_raw
    
  - name: write_customers_bronze
    type: write
    source: v_customers_raw
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ bronze_schema }}"
      table: "customers"
```

#### Option B: At Action Level

```yaml
# pipelines/silver_transforms/customer_dimension.yaml
pipeline: silver_transforms
flowgroup: customer_dimension

actions:
  - name: transform_customers
    type: transform
    transform_type: sql
    source: "{{ catalog }}.{{ bronze_schema }}.customers"
    target: v_customers_cleansed
    sql: |
      SELECT 
        customer_key,
        UPPER(customer_name) as customer_name,
        phone,
        address
      FROM STREAM(LIVE.customers)
      WHERE customer_key IS NOT NULL
        
  - name: write_customer_dimension
    type: write
    source: v_customers_cleansed
    operational_metadata: true
    custom_metadata_columns:
      _transformation_version: "F.lit('v2.1')"
      _data_lineage: "F.lit('bronze.customers -> silver.dim_customers')"
      _quality_check_passed: "F.when(F.col('customer_key').isNotNull() & F.col('customer_name').isNotNull(), F.lit(true)).otherwise(F.lit(false))"
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ silver_schema }}"
      table: "dim_customers"
```

#### Option C: In Preset Configuration

```yaml
# presets/bronze_with_enhanced_metadata.yaml
name: bronze_with_enhanced_metadata
version: "1.0"
description: "Bronze layer with enhanced operational metadata"

defaults:
  operational_metadata: true
  custom_metadata_columns:
    _data_classification: "F.lit('PII')"
    _retention_days: "F.lit(2555)"  # 7 years
    _created_by_job: "F.lit(spark.conf.get('spark.databricks.clusterUsageTags.jobId', 'unknown'))"
    _file_size_bytes: "F.lit(spark.conf.get('spark.sql.streaming.fileSource.log.fileSize', 0))"
  
  load_actions:
    cloudfiles:
      schema_evolution_mode: addNewColumns
      rescue_data_column: "_rescued_data"
      
  write_actions:
    streaming_table:
      table_properties:
        delta.enableChangeDataFeed: "true"
        delta.autoOptimize.optimizeWrite: "true"
        quality: "bronze"
```

### Step 5: Update Generators to Handle Custom Metadata

Modify the write generators to process custom metadata:

```python
# src/lhp/generators/write/streaming_table.py

class StreamingTableWriteGenerator(BaseActionGenerator):
    def generate(self, action: Action, context: dict) -> str:
        # ... existing code ...
        
        # Add operational metadata imports if needed
        flowgroup = context.get('flowgroup')
        preset_config = context.get('preset_config', {})
        add_operational_metadata = self.operational_metadata.should_add_metadata(flowgroup, action, preset_config)
        
        if add_operational_metadata:
            # Add required imports
            for import_stmt in self.operational_metadata.get_required_imports():
                self.add_import(import_stmt)
            
            # NEW: Add custom metadata columns
            custom_columns = {}
            
            # From preset config
            if 'custom_metadata_columns' in preset_config:
                custom_columns.update(preset_config['custom_metadata_columns'])
            
            # From flowgroup
            if hasattr(flowgroup, 'custom_metadata_columns') and flowgroup.custom_metadata_columns:
                custom_columns.update(flowgroup.custom_metadata_columns)
            
            # From action (highest priority)
            if hasattr(action, 'custom_metadata_columns') and action.custom_metadata_columns:
                custom_columns.update(action.custom_metadata_columns)
            
            # Add custom columns to operational metadata
            if custom_columns:
                self.operational_metadata.add_custom_metadata_columns(custom_columns)
            
            # Update metadata context
            if flowgroup:
                self.operational_metadata.update_context(
                    flowgroup.pipeline, 
                    flowgroup.flowgroup
                )
        
        # ... rest of existing code ...
```

### Step 6: Update Templates

Modify the templates to render custom metadata columns:

```jinja2
{# src/lhp/templates/write/streaming_table.py.j2 #}

{% if add_operational_metadata %}

# Add operational metadata columns
df = df.withColumn('_ingestion_timestamp', F.current_timestamp())
df = df.withColumn('_source_file', F.input_file_name())
df = df.withColumn('_pipeline_run_id', F.lit(spark.conf.get("pipelines.id", "unknown")))
df = df.withColumn('_pipeline_name', F.lit("{{ flowgroup.pipeline if flowgroup else 'unknown' }}"))
df = df.withColumn('_flowgroup_name', F.lit("{{ flowgroup.flowgroup if flowgroup else 'unknown' }}"))

{# NEW: Render custom metadata columns #}
{% if custom_metadata_columns %}
# Add custom metadata columns
{% for col_name, expression in custom_metadata_columns.items() %}
df = df.withColumn('{{ col_name }}', {{ expression }})
{% endfor %}
{% endif %}

{% endif %}
```

### Step 7: Advanced Custom Metadata Examples

Here are sophisticated examples of custom metadata columns you can implement:

#### Data Quality Metrics

```yaml
custom_metadata_columns:
  # Calculate completeness score
  _completeness_score: "F.round((F.size(F.array(*[F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in ['customer_key', 'customer_name', 'email']])) / F.lit(3.0)) * 100, 2)"
  
  # Flag suspicious records
  _anomaly_flag: "F.when((F.col('account_balance') < -10000) | (F.col('account_balance') > 1000000), F.lit('HIGH_RISK')).otherwise(F.lit('NORMAL'))"
  
  # Data freshness indicator
  _data_age_hours: "F.round((F.unix_timestamp() - F.unix_timestamp(F.col('last_updated'))) / 3600, 2)"
```

#### Business Context

```yaml
custom_metadata_columns:
  # Business quarter
  _business_quarter: "F.concat(F.lit('Q'), F.quarter(F.current_date()), F.lit('-'), F.year(F.current_date()))"
  
  # Customer segment
  _customer_segment: "F.when(F.col('account_balance') > 50000, F.lit('PREMIUM')).when(F.col('account_balance') > 10000, F.lit('STANDARD')).otherwise(F.lit('BASIC'))"
  
  # Geographic region from phone prefix
  _region: "F.when(F.substring(F.col('phone'), 1, 3).isin(['416', '647', '437']), F.lit('Toronto')).when(F.substring(F.col('phone'), 1, 3).isin(['604', '778']), F.lit('Vancouver')).otherwise(F.lit('Other'))"
```

#### Technical Lineage

```yaml
custom_metadata_columns:
  # Source table versioning
  _source_table_version: "F.lit(spark.conf.get('spark.databricks.delta.lastCommitVersionInSource', 'unknown'))"
  
  # Processing cluster info
  _cluster_id: "F.lit(spark.conf.get('spark.databricks.clusterUsageTags.clusterId', 'unknown'))"
  
  # Schema evolution tracking
  _schema_version: "F.lit(spark.conf.get('spark.sql.sources.schema.evolution.version', '1.0'))"
```

## Summary

This guide shows how to extend Lakehouse Plumber's metadata column functionality:

1. **Current System**: Built-in operational metadata with 5 standard columns
2. **Extension Points**: OperationalMetadata class, configuration models, generators, templates
3. **Configuration**: YAML-based custom metadata definition at action, flowgroup, or preset level
4. **PySpark Integration**: Direct PySpark expressions in YAML for maximum flexibility
5. **Advanced Use Cases**: Data quality metrics, business context, technical lineage

The system is designed to be extensible while maintaining the declarative YAML-based approach that Lakehouse Plumber is built on.
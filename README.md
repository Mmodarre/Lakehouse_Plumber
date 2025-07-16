# Lakehouse Plumber
**Plumbing the future of data engineering, one pipeline at a time** 🚀

*Because every data lake needs a good plumber to keep the flows running smoothly!* 🚰


<div align="center">
  <img src="lakehouse-plumber-logo.png" alt="LakehousePlumber Logo">
</div>

<div align="center">

[![PyPI version](https://badge.fury.io/py/lakehouse-plumber.svg)](https://badge.fury.io/py/lakehouse-plumber)
[![Tests](https://github.com/Mmodarre/Lakehouse_Plumber/actions/workflows/python_ci.yml/badge.svg)](https://github.com/Mmodarre/Lakehouse_Plumber/actions/workflows/python_ci.yml)
[![Python Support](https://img.shields.io/pypi/pyversions/lakehouse-plumber.svg)](https://pypi.org/project/lakehouse-plumber/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Lines of Code](https://img.shields.io/badge/lines%20of%20code-~15k-blue)](https://github.com/Mmodarre/Lakehouse_Plumber)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![codecov](https://codecov.io/gh/Mmodarre/Lakehouse_Plumber/branch/main/graph/badge.svg?token=80IBHIFAQY)](https://codecov.io/gh/Mmodarre/Lakehouse_Plumber)
[![Documentation](https://img.shields.io/badge/docs-available-brightgreen.svg)](https://lakehouse-plumber.readthedocs.io/)
[![Databricks](https://img.shields.io/badge/Databricks-DLT-orange.svg)](https://databricks.com/product/delta-live-tables)

</div>

**Action-based Lakeflow Declaritive Pipelines (formerly DLT) code generator for Databricks**

LakehousePlumber is a powerful CLI tool that generates Lakeflow Declaritive Pipelines from YAML configurations, enabling data engineers to build robust, scalable data pipelines using a declarative approach.

## 🎯 Key Features

- **Action-Based Architecture**: Define pipelines using composable load, transform, and write actions
- **Append Flow API**: Efficient multi-stream ingestion with automatic table creation management
- **Template System**: Reusable pipeline templates with parameterization
- **Environment Management**: Multi-environment support with token substitution
- **Data Quality Integration**: Built-in expectations and validation
- **Smart Generation**: Only regenerate changed files with state management and content-based file writing
- **Pipeline Validation**: Comprehensive validation rules prevent configuration conflicts
- **Code Formatting**: Automatic Python code formatting with Black
- **Secret Management**: Secure handling of credentials and API keys
- **Operational Metadata**: Automatic lineage tracking and data provenance

## 🏗️ Architecture

### Action Types

LakehousePlumber supports three main action types:

#### 🔄 Load Actions
- **CloudFiles**: Structured streaming from cloud storage (JSON, Parquet, CSV)
- **Delta**: Read from existing Delta tables
- **SQL**: Execute SQL queries as data sources
- **JDBC**: Connect to external databases
- **Python**: Custom Python-based data loading

#### ⚡ Transform Actions
- **SQL**: Standard SQL transformations
- **Python**: Custom Python transformations
- **Data Quality**: Apply expectations
- **Schema**: Column mapping and type casting
- **Temp Table**: Create temporary views

#### 💾 Write Actions
- **Streaming Table**: Live tables with change data capture
- **Materialized View**: Batch-computed views for analytics

### Project Structure

```
my_lakehouse_project/
├── lhp.yaml                   # Project configuration
├── presets/                   # Reusable configurations
│   ├── bronze_layer.yaml      # Bronze layer defaults
│   ├── silver_layer.yaml      # Silver layer defaults
│   └── gold_layer.yaml        # Gold layer defaults
├── templates/                 # Pipeline templates
│   ├── standard_ingestion.yaml
│   └── scd_type2.yaml
├── pipelines/                 # Pipeline definitions
│   ├── bronze_ingestion/
│   │   ├── customers.yaml
│   │   └── orders.yaml
│   ├── silver_transforms/
│   │   └── customer_dimension.yaml
│   └── gold_analytics/
│       └── customer_metrics.yaml
├── substitutions/             # Environment-specific values
│   ├── dev.yaml
│   ├── staging.yaml
│   └── prod.yaml
├── expectations/              # Data quality rules
└── generated/                 # Generated code
```

## 🚀 Quick Start

### Installation

```bash
pip install lakehouse-plumber
```

### Initialize a Project

```bash
lhp init my_lakehouse_project
cd my_lakehouse_project
```

### Create Your First Pipeline

Create a simple ingestion pipeline:

```yaml
# pipelines/bronze_ingestion/customers.yaml
pipeline: bronze_ingestion
flowgroup: customers
presets:
  - bronze_layer

actions:
  - name: load_customers_raw
    type: load
    source:
      type: cloudfiles
      path: "{{ landing_path }}/customers"
      format: json
      schema_evolution_mode: addNewColumns
    target: v_customers_raw
    description: "Load raw customer data from landing zone"

  - name: write_customers_bronze
    type: write
    source: v_customers_raw
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ bronze_schema }}"
      table: "customers"
      table_properties:
        delta.enableChangeDataFeed: "true"
        quality: "bronze"
    description: "Write customers to bronze layer"
```

### Configure Environment

```yaml
# substitutions/dev.yaml
catalog: dev_catalog
bronze_schema: bronze
silver_schema: silver
gold_schema: gold
landing_path: /mnt/dev/landing
checkpoint_path: /mnt/dev/checkpoints

secrets:
  default_scope: dev-secrets
  scopes:
    database: dev-db-secrets
    storage: dev-storage-secrets
```

### Validate and Generate

```bash
# Validate configuration
lhp validate --env dev

# Generate pipeline code
lhp generate --env dev

# View generated code
ls generated/
```

## 📋 CLI Commands

### Project Management
- `lhp init <project_name>` - Initialize new project
- `lhp validate --env <env>` - Validate pipeline configurations
- `lhp generate --env <env>` - Generate pipeline code
- `lhp info` - Show project information and statistics

### Discovery and Inspection
- `lhp list-presets` - List available presets
- `lhp list-templates` - List available templates
- `lhp show <flowgroup> --env <env>` - Show resolved configuration
- `lhp stats` - Show project statistics

### State Management
- `lhp generate --cleanup` - Clean up orphaned generated files
- `lhp state --env <env>` - Show generation state
- `lhp state --cleanup --env <env>` - Clean up orphaned files

### IntelliSense Setup
- `lhp setup-intellisense` - Set up VS Code IntelliSense support
- `lhp setup-intellisense --check` - Check prerequisites
- `lhp setup-intellisense --status` - Show current setup status
- `lhp setup-intellisense --verify` - Verify setup is working
- `lhp setup-intellisense --conflicts` - Show extension conflict analysis
- `lhp setup-intellisense --cleanup` - Remove IntelliSense setup

## 🧠 VS Code IntelliSense Support

LakehousePlumber provides comprehensive VS Code IntelliSense support with auto-completion, validation, and documentation for all YAML configuration files.

### ✨ Features

- **Smart Auto-completion**: Context-aware suggestions for all configuration options
- **Real-time Validation**: Immediate feedback on configuration errors
- **Inline Documentation**: Hover hints and descriptions for all fields
- **Schema Validation**: Ensures your YAML files follow the correct structure
- **Error Detection**: Highlights syntax and semantic errors as you type

### 🔧 Setup

#### Prerequisites
- VS Code installed and accessible via command line
- LakehousePlumber installed (`pip install lakehouse-plumber`)

#### Quick Setup
```bash
# Check if your system is ready
lhp setup-intellisense --check

# Set up IntelliSense (one-time setup)
lhp setup-intellisense

# Restart VS Code to activate schema associations
```

#### Verify Setup
```bash
# Check if setup is working
lhp setup-intellisense --verify

# View current status
lhp setup-intellisense --status
```

### 🎯 What Gets IntelliSense Support

- **Pipeline Configurations** (`pipelines/**/*.yaml`) - Full pipeline schema with actions, sources, and targets
- **Templates** (`templates/**/*.yaml`) - Template definitions with parameter validation
- **Presets** (`presets/**/*.yaml`) - Preset configuration schema
- **Substitutions** (`substitutions/**/*.yaml`) - Environment-specific value validation
- **Project Configuration** (`lhp.yaml`) - Main project settings

### 📝 Usage

Once set up, open any Lakehouse Plumber YAML file in VS Code and enjoy:

1. **Auto-completion**: Press `Ctrl+Space` to see available options
2. **Documentation**: Hover over any field to see descriptions
3. **Validation**: Red underlines indicate errors with helpful messages
4. **Structure**: IntelliSense guides you through the correct YAML structure

Example of IntelliSense in action:
```yaml
# Type "actions:" and get auto-completion for action types
actions:
  - name: load_data
    type: # ← IntelliSense suggests: load, transform, write
    source:
      type: # ← IntelliSense suggests: cloudfiles, delta, sql, jdbc, python
      path: # ← Documentation shows path requirements
```

### 🛠️ Troubleshooting

#### Extension Conflicts
Some YAML extensions may conflict with LakehousePlumber schemas:
```bash
# Check for conflicts
lhp setup-intellisense --conflicts

# View detailed conflict analysis
lhp setup-intellisense --conflicts
```

#### Setup Issues
```bash
# Force setup even if prerequisites aren't met
lhp setup-intellisense --force

# Clean up and start fresh
lhp setup-intellisense --cleanup
lhp setup-intellisense
```

#### Common Issues

**IntelliSense not working after setup:**
1. Restart VS Code completely
2. Verify setup: `lhp setup-intellisense --verify`
3. Check for extension conflicts: `lhp setup-intellisense --conflicts`

**Schema associations missing:**
1. Check status: `lhp setup-intellisense --status`
2. Re-run setup: `lhp setup-intellisense --force`

**Red Hat YAML extension conflicts:**
- The Red Hat YAML extension is detected but usually works well alongside LakehousePlumber schemas
- If issues persist, you can temporarily disable it or adjust its settings



## 🎨 Advanced Features

### Presets

Create reusable configurations:

```yaml
# presets/bronze_layer.yaml
name: bronze_layer
version: "1.0"
description: "Standard bronze layer configuration"

defaults:
  operational_metadata: true
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

### Templates

Create parameterized pipeline templates:

```yaml
# templates/standard_ingestion.yaml
name: standard_ingestion
version: "1.0"
description: "Standard data ingestion template"

parameters:
  - name: source_path
    type: string
    required: true
  - name: table_name
    type: string
    required: true
  - name: file_format
    type: string
    default: "json"

actions:
  - name: "load_{{ table_name }}_raw"
    type: load
    source:
      type: cloudfiles
      path: "{{ source_path }}"
      format: "{{ file_format }}"
    target: "v_{{ table_name }}_raw"
    
  - name: "write_{{ table_name }}_bronze"
    type: write
    source: "v_{{ table_name }}_raw"
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ bronze_schema }}"
      table: "{{ table_name }}"
```

### Data Quality

Integrate expectations:

```yaml
# expectations/customer_quality.yaml
expectations:
  - name: valid_customer_key
    constraint: "customer_key IS NOT NULL"
    on_violation: "fail"
  - name: valid_email
    constraint: "email RLIKE '^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"
    on_violation: "drop"
```

```yaml
# Pipeline with data quality
- name: validate_customers
  type: transform
  transform_type: data_quality
  source: v_customers_raw
  target: v_customers_validated
  expectations_file: "expectations/customer_quality.yaml"
```

### SCD Type 2

Implement Slowly Changing Dimensions:

```yaml
- name: customer_dimension_scd2
  type: transform
  transform_type: python
  source: v_customers_validated
  target: v_customers_scd2
  python_source: |
    def scd2_merge(df):
        return df.withColumn("__start_date", current_date()) \
                 .withColumn("__end_date", lit(None)) \
                 .withColumn("__is_current", lit(True))
```

## 🏗️ Table Creation and Append Flow API

LakehousePlumber uses the Databricks Append Flow API to efficiently handle multiple data streams writing to the same streaming table. This approach prevents table recreation conflicts and enables high-performance, concurrent data ingestion.

### Core Concepts

#### Table Creation Control

Every write action must specify whether it creates the table or appends to an existing one:

```yaml
- name: write_orders_primary
  type: write
  source: v_orders_cleaned_primary
  write_target:
    type: streaming_table
    database: "{catalog}.{bronze_schema}"
    table: orders
    create_table: true  # ← This action creates the table
    table_properties:
      delta.enableChangeDataFeed: "true"
      quality: "bronze"

- name: write_orders_secondary  
  type: write
  source: v_orders_cleaned_secondary
  write_target:
    type: streaming_table
    database: "{catalog}.{bronze_schema}"
    table: orders
    create_table: false  # ← This action appends to existing table
```

#### Generated DLT Code

The above configuration generates optimized DLT code:

```python
# Table is created once
dlt.create_streaming_table(
    name="catalog.bronze.orders",
    comment="Streaming table: orders",
    table_properties={
        "delta.enableChangeDataFeed": "true",
        "quality": "bronze"
    }
)

# Multiple append flows target the same table
@dlt.append_flow(
    target="catalog.bronze.orders",
    name="f_orders_primary",
    comment="Append flow to catalog.bronze.orders from v_orders_cleaned_primary"
)
def f_orders_primary():
    return spark.readStream.table("v_orders_cleaned_primary")

@dlt.append_flow(
    target="catalog.bronze.orders", 
    name="f_orders_secondary",
    comment="Append flow to catalog.bronze.orders from v_orders_cleaned_secondary"
)
def f_orders_secondary():
    return spark.readStream.table("v_orders_cleaned_secondary")
```

### Validation Rules

LakehousePlumber enforces strict validation rules to prevent conflicts:

#### Rule 1: Exactly One Creator Per Table
Each streaming table must have exactly one action with `create_table: true` across the entire pipeline.

```yaml
# ✅ VALID: One creator, multiple appenders
- name: write_lineitem_au
  write_target:
    table: lineitem
    create_table: true   # ← Creates table

- name: write_lineitem_nz  
  write_target:
    table: lineitem
    create_table: false  # ← Appends to table

- name: write_lineitem_us
  write_target:
    table: lineitem  
    create_table: false  # ← Appends to table
```

```yaml
# ❌ INVALID: Multiple creators
- name: action1
  write_target:
    table: lineitem
    create_table: true   # ← Error: Multiple creators

- name: action2
  write_target:
    table: lineitem
    create_table: true   # ← Error: Multiple creators
```

```yaml  
# ❌ INVALID: No creator
- name: action1
  write_target:
    table: lineitem
    create_table: false  # ← Error: No creator for table

- name: action2
  write_target:
    table: lineitem
    create_table: false  # ← Error: No creator for table
```

#### Rule 2: Explicit Configuration Required for Multiple Writes to Table
The `create_table` field defaults to `true`, NOT requiring explicit specification unless you want to append to an existing table:

```yaml
# ❌ Implicit (defaults to true - may cause validation errors if you want to append to an existing table)
write_target:
  table: my_table
  # create_table not specified (defaults to true)

# ✅ Explicit (recommended for single write to table, required for multiple writes to table)  
write_target_1_n_more:
  table: my_existing_table
  create_table: false
```

### Error Handling

LakehousePlumber provides clear, actionable error messages:

```bash
# No table creator
Table creation validation failed:
  - Table 'catalog.bronze.orders' has no creator. 
    One action must have 'create_table: true'. 
    Used by: orders_ingestion.write_orders_bronze

# Multiple table creators  
Table creation validation failed:
  - Table 'catalog.bronze.orders' has multiple creators: 
    orders_ingestion.write_orders_primary, orders_ingestion.write_orders_secondary. 
    Only one action can have 'create_table: true'.
```

### Advanced Use Cases

#### Multi-Region Data Ingestion

```yaml
# Pipeline ingesting from multiple regions
actions:
  - name: write_events_us_east
    type: write
    source: v_events_us_east_cleaned
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: events
      create_table: true  # Primary region creates table
      partition_columns: ["event_date", "region"]
      
  - name: write_events_us_west
    type: write  
    source: v_events_us_west_cleaned
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: events
      create_table: false  # Secondary regions append
      
  - name: write_events_eu
    type: write
    source: v_events_eu_cleaned  
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: events
      create_table: false  # Secondary regions append
```

#### Cross-Flowgroup Table Sharing

Tables can be shared across multiple flowgroups within the same pipeline:

```yaml
# flowgroup1.yaml
pipeline: bronze_facts
flowgroup: orders_processing
actions:
  - name: write_orders_online
    write_target:
      table: all_orders
      create_table: true  # This flowgroup creates the table

# flowgroup2.yaml  
pipeline: bronze_facts
flowgroup: legacy_orders
actions:
  - name: write_orders_legacy
    write_target:
      table: all_orders
      create_table: false  # This flowgroup appends to existing table
```

### Smart File Generation

LakehousePlumber includes intelligent file writing that reduces unnecessary file churn:

#### Content-Based File Writing
- Only writes files when content actually changes
- Normalizes whitespace and formatting for accurate comparison
- Reduces Git noise and CI/CD overhead

```bash
# Generation output shows statistics
✅ Generation complete: 2 files written, 8 files skipped (no changes)
```

#### Benefits
- **Faster CI/CD**: Fewer file changes mean faster builds
- **Cleaner Git History**: No unnecessary commits for unchanged files  
- **Reduced Resource Usage**: Less file I/O and processing
- **Better Developer Experience**: Clear indication of actual changes

### Migration Guide

#### From Legacy DLT Code

If you have existing DLT code with multiple `dlt.create_streaming_table()` calls:

```python
# ❌ Legacy: Multiple table creations
dlt.create_streaming_table(name="catalog.bronze.orders", ...)
dlt.create_streaming_table(name="catalog.bronze.orders", ...)  # Conflict!

@dlt.table(name="catalog.bronze.orders")
def orders_flow1():
    return spark.readStream.table("source1")
    
@dlt.table(name="catalog.bronze.orders")  
def orders_flow2():
    return spark.readStream.table("source2")
```

Update your YAML configuration:

```yaml
# ✅ New: Explicit table creation control
- name: write_orders_primary
  source: source1
  write_target:
    table: orders
    create_table: true   # Only this action creates

- name: write_orders_secondary
  source: source2  
  write_target:
    table: orders
    create_table: false  # This action appends
```

#### Backward Compatibility

Existing configurations without `create_table` flags will work but may trigger validation warnings. Update configurations gradually by adding explicit `create_table` flags.

## 🔧 Development

### Prerequisites

- Python 3.8+
- Databricks workspace with enabled
- Access to cloud storage (S3, ADLS, GCS)

### Local Development

```bash
# Clone the repository
git clone https://github.com/yourusername/lakehouse-plumber.git
cd lakehouse-plumber

# Install in development mode
pip install -e .

# Run tests
pytest tests/

# Run CLI
lhp --help
```

### Testing

LakehousePlumber includes comprehensive test coverage:

```bash
# Run all tests
pytest

# Run specific test categories
pytest tests/test_integration.py      # Integration tests
pytest tests/test_cli.py             # CLI tests
pytest tests/test_advanced_features.py  # Advanced features
pytest tests/test_performance.py     # Performance tests
```

## 📚 Examples

### Bronze Layer Ingestion

```yaml
pipeline: bronze_ingestion
flowgroup: orders
presets:
  - bronze_layer

actions:
  - name: load_orders_cloudfiles
    type: load
    source:
      type: cloudfiles
      path: "{{ landing_path }}/orders"
      format: parquet
      schema_evolution_mode: addNewColumns
    target: v_orders_raw
    operational_metadata: true
    
  - name: write_orders_bronze
    type: write
    source: v_orders_raw
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ bronze_schema }}"
      table: "orders"
      partition_columns: ["order_date"]
```

### Silver Layer Transformation

```yaml
pipeline: silver_transforms
flowgroup: customer_dimension

actions:
  - name: cleanse_customers
    type: transform
    transform_type: sql
    source: "{{ catalog }}.{{ bronze_schema }}.customers"
    target: v_customers_cleansed
    sql: |
      SELECT 
        customer_key,
        TRIM(UPPER(customer_name)) as customer_name,
        REGEXP_REPLACE(phone, '[^0-9]', '') as phone_clean,
        address,
        nation_key,
        market_segment,
        account_balance
      FROM STREAM(LIVE.customers)
      WHERE customer_key IS NOT NULL
      
  - name: apply_scd2
    type: transform
    transform_type: python
    source: v_customers_cleansed
    target: v_customers_scd2
    python_source: |
      @dlt.view
      def scd2_logic():
          return spark.readStream.table("LIVE.v_customers_cleansed")
          
  - name: write_customer_dimension
    type: write
    source: v_customers_scd2
    write_target:
      type: streaming_table
      database: "{{ catalog }}.{{ silver_schema }}"
      table: "dim_customers"
      table_properties:
        delta.enableChangeDataFeed: "true"
        quality: "silver"
```

### Gold Layer Analytics

```yaml
pipeline: gold_analytics
flowgroup: customer_metrics

actions:
  - name: customer_lifetime_value
    type: transform
    transform_type: sql
    source: 
      - "{{ catalog }}.{{ silver_schema }}.dim_customers"
      - "{{ catalog }}.{{ silver_schema }}.fact_orders"
    target: v_customer_ltv
    sql: |
      SELECT 
        c.customer_key,
        c.customer_name,
        c.market_segment,
        COUNT(o.order_key) as total_orders,
        SUM(o.total_price) as lifetime_value,
        AVG(o.total_price) as avg_order_value,
        MAX(o.order_date) as last_order_date
      FROM LIVE.dim_customers c
      LEFT JOIN LIVE.fact_orders o ON c.customer_key = o.customer_key
      WHERE c.__is_current = true
      GROUP BY c.customer_key, c.customer_name, c.market_segment
      
  - name: write_customer_metrics
    type: write
    source: v_customer_ltv
    write_target:
      type: materialized_view
      database: "{{ catalog }}.{{ gold_schema }}"
      table: "customer_metrics"
      refresh_schedule: "0 2 * * *"  # Daily at 2 AM
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Development Setup

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [Read the Docs](https://mmodarre.github.io/Lakehouse_Plumber)
- **Issues**: [GitHub Issues](https://github.com/Mmodarre/Lakehouse_Plumber/issues)
- **Discussions**: [GitHub Discussions](https://github.com/Mmodarre/Lakehouse_Plumber/discussions)

## 🙏 Acknowledgments

- Built for the Databricks ecosystem
- Inspired by modern data engineering practices
- Designed for the medallion architecture pattern

---

**Made with ❤️ for Databricks and Lakeflow Declarative Pipelines**

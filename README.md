# LakehousePlumber 🔧

<div align="center">
  <img src="lakehouse-plumber-logo.png" alt="LakehousePlumber Logo">
</div>

<div align="center">

[![PyPI version](https://badge.fury.io/py/lakehouse-plumber.svg)](https://badge.fury.io/py/lakehouse-plumber)
[![Python Support](https://img.shields.io/pypi/pyversions/lakehouse-plumber.svg)](https://pypi.org/project/lakehouse-plumber/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Lines of Code](https://img.shields.io/badge/lines%20of%20code-~8k-blue)](https://github.com/Mmodarre/Lakehouse_Plumber)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

[![Coverage](https://codecov.io/gh/Mmodarre/Lakehouse_Plumber/branch/main/graph/badge.svg)](https://codecov.io/gh/Mmodarre/Lakehouse_Plumber)
[![Documentation](https://img.shields.io/badge/docs-available-brightgreen.svg)](https://lakehouse-plumber.readthedocs.io/)
[![Databricks](https://img.shields.io/badge/Databricks-DLT-orange.svg)](https://databricks.com/product/delta-live-tables)

</div>

**Action-based Lakeflow Declaritive Pipelines (formerly DLT) pipeline generator for Databricks**

LakehousePlumber is a powerful CLI tool that generates Lakeflow Declaritive Pipelines pipelines from YAML configurations, enabling data engineers to build robust, scalable data pipelines using a declarative approach.

## 🎯 Key Features

- **Action-Based Architecture**: Define pipelines using composable load, transform, and write actions
- **Template System**: Reusable pipeline templates with parameterization
- **Environment Management**: Multi-environment support with token substitution
- **Data Quality Integration**: Built-in expectations and validation
- **Smart Generation**: Only regenerate changed files with state management
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
├── schemas/                   # Table schemas
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

- **Documentation**: [Wiki](https://github.com/yourusername/lakehouse-plumber/wiki)
- **Issues**: [GitHub Issues](https://github.com/yourusername/lakehouse-plumber/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/lakehouse-plumber/discussions)

## 🙏 Acknowledgments

- Built for the Databricks ecosystem
- Inspired by modern data engineering practices
- Designed for the medallion architecture pattern

---

**Made with ❤️ for Databricks and Lakeflow Declarative Data Pipelines** 
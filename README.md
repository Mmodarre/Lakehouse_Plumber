# TPC-H Lakehouse Sample Project

This is a sample LakehousePlumber project demonstrating a complete data lakehouse implementation using TPC-H benchmark data.

## Project Overview

This project implements a medallion architecture (Bronze → Silver → Gold) for TPC-H data with:
- **Bronze Layer**: Raw CSV ingestion with schema enforcement and operational metadata
- **Silver Layer**: Cleansed data with SCD Type 2 for dimensions
- **Gold Layer**: Business-ready aggregations and metrics

## Architecture

### Data Flow
```
CSV Files → Bronze (Raw) → Silver (Cleansed/Conformed) → Gold (Analytics)
```

### Key Features
1. **Schema-driven ingestion** - All tables use predefined schemas from `schemas/` directory
2. **Data quality checks** - Customer data validated with expectations
3. **SCD Type 2** - Customer dimension tracks historical changes
4. **Operational metadata** - All bronze tables include ingestion timestamps and source tracking
5. **Partitioned fact tables** - Orders partitioned by year/month for performance

## Project Structure

```
tpch_lakehouse/
├── presets/                    # Layer-specific configurations
│   ├── bronze_layer.yaml      # Bronze ingestion settings
│   ├── silver_layer.yaml      # Silver transformation settings
│   └── gold_layer.yaml        # Gold aggregation settings
├── templates/                  # Reusable pipeline templates
│   └── csv_ingestion.yaml     # Template for CSV file ingestion
├── schemas/                    # Table schema definitions
│   ├── customer_schema.yaml   # Customer dimension schema
│   ├── orders_schema.yaml     # Orders fact schema
│   └── tables.json           # All TPC-H table schemas
├── expectations/              # Data quality rules
│   └── customer_quality.json  # Customer data validation rules
├── substitutions/             # Environment-specific values
│   ├── dev.yaml              # Development settings
│   └── prod.yaml             # Production settings
└── pipelines/                 # Pipeline definitions
    ├── bronze_dimensions/     # Bronze dimension ingestion
    │   └── customer_ingestion.yaml
    ├── bronze_facts/          # Bronze fact ingestion
    │   └── orders_ingestion.yaml
    ├── silver_dimensions/     # Silver dimension processing
    │   └── customer_dimension.yaml
    ├── silver_facts/          # Silver fact processing
    │   └── orders_fact.yaml
    └── gold_analytics/        # Gold analytics
        └── customer_revenue.yaml
```

## Pipelines

### Bronze Layer
- **customer_ingestion**: Ingests customer CSV files with data quality validation
- **orders_ingestion**: Ingests orders CSV files

### Silver Layer
- **customer_dimension**: Implements SCD Type 2 for customer with data cleansing
- **orders_fact**: Enriches orders with derived columns and partitioning

### Gold Layer
- **customer_revenue**: Customer lifetime value and revenue analytics

## Getting Started

### Prerequisites
- Databricks workspace with DLT enabled
- CSV files in landing zone matching the TPC-H schema
- LakehousePlumber CLI installed

### Setup
1. Update environment settings in `substitutions/dev.yaml`:
   - Set your catalog name
   - Configure landing zone path
   - Update checkpoint locations

2. Validate the project:
   ```bash
   lhp validate --env dev
   ```

3. Generate pipelines:
   ```bash
   # Generate bronze pipelines
   lhp generate bronze_dimensions --env dev
   lhp generate bronze_facts --env dev
   
   # Generate silver pipelines
   lhp generate silver_dimensions --env dev
   lhp generate silver_facts --env dev
   
   # Generate gold pipelines
   lhp generate gold_analytics --env dev
   ```

4. Deploy to Databricks:
   - Upload generated Python files from `generated/` to Databricks
   - Create DLT pipelines for each layer
   - Configure pipeline dependencies (Bronze → Silver → Gold)

## Data Quality

Customer data is validated with the following rules:
- **Fail on**: Invalid customer key, empty customer name, invalid nation key
- **Drop on**: Invalid phone format
- **Warn on**: Unusual account balance, invalid market segment

## SCD Type 2 Implementation

The customer dimension tracks changes to:
- Customer name
- Address
- Phone number
- Account balance
- Market segment

Historical records are preserved with:
- `__start_date` and `__end_date` for record validity
- `__is_current` flag for easy filtering
- `__version` for tracking change count

## Performance Optimizations

1. **Partitioning**: Orders fact table partitioned by year/month
2. **Z-Ordering**: Customer dimension z-ordered by customer key
3. **Auto-optimization**: Enabled for all streaming tables
4. **Data skipping**: Extended to 32 columns for gold layer analytics

## Extending the Project

### Adding New Tables
1. Add schema definition to `schemas/`
2. Create ingestion pipeline in `pipelines/bronze_*/`
3. Add cleansing logic in `pipelines/silver_*/`
4. Update analytics in `pipelines/gold_*/`

### Adding Data Quality Rules
1. Create expectations file in `expectations/`
2. Reference in pipeline using `expectations_file` parameter

### Creating New Aggregations
1. Add new flowgroup to `pipelines/gold_analytics/`
2. Use materialized views for large aggregations
3. Consider partitioning for time-based metrics

## Best Practices

1. **Always use schemas** - Define schemas for all tables
2. **Apply data quality early** - Validate in bronze layer
3. **Track lineage** - Use operational metadata
4. **Version control** - Commit all YAML configurations
5. **Test in dev** - Validate before production deployment

## Troubleshooting

### Common Issues
1. **Schema mismatch**: Ensure CSV headers match schema definitions
2. **Missing files**: Check landing zone paths in substitutions
3. **SCD conflicts**: Verify primary keys are unique
4. **Performance**: Monitor partition pruning in fact tables

### Debug Commands
```bash
# Show resolved configuration
lhp show <flowgroup> --env dev

# List all pipelines
lhp info

# View pipeline statistics
lhp stats
```

## License
This is a sample project for demonstration purposes.

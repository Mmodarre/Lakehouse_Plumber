# LakehousePlumber VS Code YAML Support

This directory contains JSON schemas that provide VS Code with autocomplete, validation, and IntelliSense support for LakehousePlumber YAML files.

## Setup

### Prerequisites
1. Install the **YAML** extension by Red Hat in VS Code
2. Ensure you have the schemas and `.vscode/settings.json` files in your project

### Configuration

The `.vscode/settings.json` file automatically configures VS Code to use the appropriate schema for each YAML file type:

- **Pipeline files** (`pipelines/**/*.yaml`) → `flowgroup.schema.json`
- **Template files** (`templates/**/*.yaml`) → `template.schema.json`
- **Preset files** (`presets/**/*.yaml`) → `preset.schema.json`
- **Project config** (`lhp.yaml`) → `project.schema.json`
- **Substitution files** (`substitutions/**/*.yaml`) → `substitution.schema.json`

## Features

### 1. Autocomplete
- Type assistance for all configuration properties
- Enum value suggestions for action types, transform types, etc.
- Template parameter suggestions

### 2. Validation
- Real-time validation of YAML structure
- Required field validation
- Type checking (string, number, boolean, array)
- Enum value validation

### 3. Hover Documentation
- Descriptions for all properties
- Usage examples and context

### 4. IntelliSense
- Smart suggestions based on context
- Property descriptions and examples
- Template parameter hints

## Schema Files

### flowgroup.schema.json
Schema for pipeline configuration files with:
- Pipeline and flowgroup metadata
- Action definitions (load, transform, write)
- Template usage with parameters
- Preset references
- Operational metadata configuration

### template.schema.json
Schema for template files with:
- Template metadata and parameters
- Parameterized actions using `{{ parameter_name }}`
- Parameter validation (required, type, default values)

### preset.schema.json
Schema for preset files with:
- Preset metadata and inheritance
- Default configurations for all action types
- Operational metadata defaults

### project.schema.json
Schema for project configuration (lhp.yaml) with:
- Project metadata
- Include patterns for selective file processing
- Operational metadata column definitions
- Metadata presets

### substitution.schema.json
Schema for environment substitution files with:
- Environment-specific configuration structure
- Common substitution tokens
- Secret configuration
- Database and storage configuration
- Custom token definitions

## Usage Examples

### Project Configuration with Include Patterns
```yaml
# lhp.yaml
name: my_lakehouse_project
version: "1.0"
description: "Data lakehouse with selective processing"

# Include patterns for selective file processing
include:
  - "bronze_*.yaml"           # All bronze layer files
  - "silver/**/*.yaml"        # All files in silver subdirectories
  - "gold/dimension_*.yaml"   # Only dimension files in gold
  - "!**/temp_*.yaml"        # Exclude temporary files

operational_metadata:
  columns:
    load_timestamp:
      expression: "current_timestamp()"
      description: "When the record was loaded"
    file_name:
      expression: "input_file_name()"
      description: "Source file name"
```

### Pipeline Configuration with CDC
```yaml
pipeline: silver_layer
flowgroup: customer_updates

actions:
  - name: load_customer_changes
    type: load
    source:
      type: delta
      table: "bronze.customer_cdc"
      read_change_feed: true
    target: v_customer_changes

  - name: apply_customer_cdc
    type: write
    source: v_customer_changes
    write_target:
      type: streaming_table
      database: "silver"
      table: "customers"
      mode: "cdc"
      cdc_config:
        keys: ["customer_id"]
        sequence_by: "timestamp"
        stored_as_scd_type: "2"
        track_history_column_list: ["name", "email", "status"]
```

### CloudFiles with Advanced Options
```yaml
pipeline: bronze_layer
flowgroup: json_ingestion

actions:
  - name: load_json_data
    type: load
    source:
      type: cloudfiles
      path: "/mnt/landing/json_data"
      format: json
      schema_evolution_mode: "addNewColumns"
      rescue_data_column: "_rescued_data"
      max_files_per_trigger: 100
      reader_options:
        multiline: true
        allowComments: true
      format_options:
        timestampFormat: "yyyy-MM-dd HH:mm:ss"
        dateFormat: "yyyy-MM-dd"
    target: v_raw_json_data
```

### Template Configuration
```yaml
name: my_template
version: "1.0"
description: "Template for data ingestion"

parameters:
  - name: table_name
    required: true
    description: "Name of the table"
  - name: file_format
    required: false
    default: "json"
    description: "File format"

actions:
  - name: load_{{ table_name }}
    type: load
    source:
      type: cloudfiles
      format: "{{ file_format }}"
    target: v_{{ table_name }}_raw
```

### Substitution Configuration
```yaml
# substitutions/dev.yaml
dev:
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
      
  database_config:
    host: dev-db.company.com
    port: 5432
    name: lakehouse_dev
```

## Troubleshooting

### Schema Not Working
1. Ensure the YAML extension is installed and enabled
2. Check that `.vscode/settings.json` exists in your project root
3. Verify file paths in the schema associations match your project structure
4. Reload VS Code window (Ctrl+Shift+P → "Developer: Reload Window")

### Validation Errors
- Check that required fields are present
- Verify enum values match the schema
- Ensure proper YAML syntax (indentation, quotes, etc.)

### Custom Extensions
If you have custom action types or properties, you can:
1. Modify the schema files to include your custom definitions
2. Use `additionalProperties: true` for flexible configurations
3. Submit a pull request to include common extensions

## Contributing

To improve the schemas:
1. Update the relevant `.schema.json` file
2. Test with actual YAML files
3. Update this README with new features
4. Submit a pull request

The schemas are generated based on the Pydantic models in `src/lhp/models/config.py`. 
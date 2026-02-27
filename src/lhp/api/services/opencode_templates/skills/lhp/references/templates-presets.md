# Templates & Presets Reference

## Table of Contents
- [Templates](#templates)
- [Presets](#presets)
- [Integration](#integration)

---

## Templates

Templates are reusable action patterns with parameter substitution. Stored in `templates/` directory.

### Template Structure

```yaml
name: my_template
version: "1.0"
description: "Template description"

presets:                          # Optional
  - bronze_layer

parameters:
  - name: table_name
    type: string                  # string, object, array, boolean, number
    required: true
    description: "Target table name"
  - name: table_properties
    type: object
    required: false
    default: {}
  - name: partition_columns
    type: array
    required: false
    default: []
  - name: enable_cdc
    type: boolean
    required: false
    default: true
  - name: max_files
    type: number
    required: false
    default: 1000

actions:
  - name: "load_{{ table_name }}_data"
    type: load
    source:
      type: cloudfiles
      path: "{landing_volume}/{{ table_name }}/*.csv"
    target: "v_{{ table_name }}_raw"

  - name: "write_{{ table_name }}_bronze"
    type: write
    source: "v_{{ table_name }}_raw"
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: "{{ table_name }}"
      table_properties: "{{ table_properties }}"
      partition_columns: "{{ partition_columns }}"
```

### Using Templates in FlowGroups

```yaml
pipeline: raw_ingestions
flowgroup: customer_ingestion

use_template: my_template
template_parameters:
  table_name: customer
  table_properties:               # Natural YAML (not JSON strings)
    delta.enableChangeDataFeed: true
    delta.autoOptimize.optimizeWrite: true
  partition_columns:
    - "region"
    - "year"
  enable_cdc: true
  max_files: 500
```

### Parameter Types

| Type | FlowGroup Syntax | Notes |
|------|------------------|-------|
| `string` | `param: "value"` | Names, paths, identifiers |
| `object` | Natural YAML dict | Deep structures preserved |
| `array` | Natural YAML list | Lists of values |
| `boolean` | `true`/`false` | Conditional controls |
| `number` | `42` or `3.14` | Integer or float |

### Template Expressions (Jinja2)

**Basic substitution:**
```yaml
name: "process_{{ table_name }}_data"
```

**Conditional logic:**
```yaml
source: "{% if enable_dqe %}v_{{ table_name }}_validated{% else %}v_{{ table_name }}_raw{% endif %}"
```

**String filters:**
```yaml
name: "{{ table_name | lower }}_processing"
```

### Practical Examples

**CSV Ingestion Template:**
```yaml
name: csv_ingestion_template
version: "1.0"
presets:
  - bronze_layer

parameters:
  - name: table_name
    type: string
    required: true
  - name: landing_folder
    type: string
    required: true
  - name: cluster_columns
    type: array
    required: false
    default: []

actions:
  - name: "load_{{ table_name }}_csv"
    type: load
    readMode: stream
    operational_metadata: ["_source_file_path", "_processing_timestamp"]
    source:
      type: cloudfiles
      path: "{landing_volume}/{{ landing_folder }}/*.csv"
      format: csv
      options:
        cloudFiles.format: csv
        header: true
        cloudFiles.schemaHints: "schemas/{{ table_name }}_schema.yaml"
    target: "v_{{ table_name }}_cloudfiles"

  - name: "write_{{ table_name }}_bronze"
    type: write
    source: "v_{{ table_name }}_cloudfiles"
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: "{{ table_name }}"
      cluster_columns: "{{ cluster_columns }}"
```

**SCD Type 2 Template:**
```yaml
name: scd_type2_template
version: "1.0"

parameters:
  - name: table_name
    type: string
    required: true
  - name: source_table
    type: string
    required: true
  - name: primary_keys
    type: array
    required: true
  - name: sequence_column
    type: string
    required: true
  - name: track_history_column_list
    type: array
    required: false
    default: []

actions:
  - name: "load_{{ table_name }}_changes"
    type: load
    readMode: stream
    source:
      type: delta
      database: "{catalog}.{bronze_schema}"
      table: "{{ source_table }}"
      options:
        readChangeFeed: "true"
    target: "v_{{ table_name }}_changes"

  - name: "write_{{ table_name }}_dimension"
    type: write
    source: "v_{{ table_name }}_changes"
    write_target:
      type: streaming_table
      database: "{catalog}.{silver_schema}"
      table: "dim_{{ table_name }}"
      mode: cdc
      cdc_config:
        keys: "{{ primary_keys }}"
        sequence_by: "{{ sequence_column }}"
        scd_type: 2
        track_history_column_list: "{{ track_history_column_list }}"
      table_properties:
        delta.enableChangeDataFeed: true
```

---

## Presets

Presets provide reusable configuration defaults via **implicit type-based matching**. Stored in `presets/` directory.

### Preset Structure

```yaml
name: bronze_layer
version: "1.0"
description: "Standard bronze layer defaults"
extends: base_config              # Optional: inherit from another preset

defaults:
  load_actions:
    cloudfiles:                   # Matches source.type == "cloudfiles"
      options:
        cloudFiles.rescuedDataColumn: "_rescued_data"
        ignoreCorruptFiles: "true"
        cloudFiles.maxFilesPerTrigger: 200

  write_actions:
    streaming_table:              # Matches write_target.type == "streaming_table"
      table_properties:
        delta.enableChangeDataFeed: "true"
        delta.autoOptimize.optimizeWrite: "true"
        quality: "bronze"
```

### How Matching Works

- `load_actions.{source_type}` -> matches `source.type == {source_type}`
- `write_actions.{target_type}` -> matches `write_target.type == {target_type}`
- No conditional logic; use separate presets for different scenarios

### Merge Behavior

- **Nested dicts**: Deep merged (preset + explicit combined)
- **Lists**: Replaced entirely (explicit wins)
- **Scalars**: Explicit wins on conflict

### Precedence (highest to lowest)

1. Flowgroup explicit config
2. Flowgroup preset
3. Template explicit config
4. Template preset

### Usage

**In FlowGroup:**
```yaml
presets:
  - cloudfiles_defaults
  - bronze_layer
```

**In Template:**
```yaml
presets:
  - bronze_layer
```

### Common Patterns

**CloudFiles error handling:**
```yaml
defaults:
  load_actions:
    cloudfiles:
      options:
        ignoreCorruptFiles: "true"
        ignoreMissingFiles: "true"
        cloudFiles.rescuedDataColumn: "_rescued_data"
```

**Bronze table standards:**
```yaml
defaults:
  write_actions:
    streaming_table:
      table_properties:
        delta.enableRowTracking: "true"
        delta.enableChangeDataFeed: "true"
        delta.autoOptimize.optimizeWrite: "true"
        quality: "bronze"
```

**Important:** CloudFiles options must be nested under `options:` key:
```yaml
# CORRECT
load_actions:
  cloudfiles:
    options:
      cloudFiles.rescuedDataColumn: "_rescued_data"

# WRONG
load_actions:
  cloudfiles:
    cloudFiles.rescuedDataColumn: "_rescued_data"
```

---

## Integration

Templates and presets combine for maximum reusability:

```yaml
# Template references presets
name: bronze_ingestion_template
presets:
  - bronze_layer_defaults

# FlowGroup can add more presets on top
pipeline: data_pipeline
flowgroup: customer_ingestion
presets:
  - extra_quality_checks       # Added on top of template presets

use_template: bronze_ingestion_template
template_parameters:
  table_name: customer
```

Preset values from both template and flowgroup levels are merged together, with flowgroup-level taking precedence on conflicts.

# Transform Actions Reference

Transform actions manipulate data between views — SQL queries, Python functions, schema mapping, data quality, and temp tables.

## Common Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | **req** | Unique name within the FlowGroup |
| `type` | **req** | Must be `transform` |
| `transform_type` | **req** | `sql`, `python`, `data_quality`, `schema`, `temp_table` |
| `source` | **req** | Input view name (string or list for python multi-source) |
| `target` | **req** | Output view name |
| `readMode` | opt | `stream` (default) or `batch` |
| `operational_metadata` | opt | Metadata columns to add |
| `description` | opt | Documentation for the action |

---

## SQL Transform

```yaml
# Inline SQL
- name: cleanse_data
  type: transform
  transform_type: sql             # req
  source: v_raw_data              # req
  target: v_cleaned_data          # req
  sql: |                          # req (one of sql or sql_path)
    SELECT col1, UPPER(col2) as col2
    FROM stream(v_raw_data)

# External SQL file
- name: enrich_data
  type: transform
  transform_type: sql
  source: v_raw_data
  target: v_enriched
  sql_path: "sql/enrich_query.sql"  # req (one of sql or sql_path)
```

**CRITICAL:** Use `stream(view_name)` in SQL for streaming sources. Without it, the query processes in batch mode.

**Key:** Substitution variables work in both inline SQL and external files (`{token}`, `${secret:scope/key}`). Files are processed for substitutions before execution.

---

## Python Transform

```yaml
# Single source
- name: enrich_data
  type: transform
  transform_type: python          # req
  source: v_raw_data              # req — string for single source
  module_path: "transformations/enrich.py"  # req
  function_name: "enrich_data"    # req
  parameters:                     # opt
    batch_size: 1000
  target: v_enriched_data
  readMode: batch

# Multiple sources
- name: combine_data
  type: transform
  transform_type: python
  source: ["v_customers", "v_orders"]  # list for multiple sources
  module_path: "analytics/combine.py"
  function_name: "combine_sources"
  target: v_combined
  readMode: batch
```

**Function signatures:**
- **Single source**: `def func(df: DataFrame, spark, parameters: dict) -> DataFrame`
- **Multiple sources**: `def func(dataframes: List[DataFrame], spark, parameters: dict) -> DataFrame`
- **No source** (generator): `def func(spark, parameters: dict) -> DataFrame`

**File management:**
- Python files are auto-copied to `generated/<pipeline>/custom_python_functions/`
- Imports generated as `from custom_python_functions.module import function`
- Copied files include "DO NOT EDIT" warning headers
- Substitution variables supported in Python files
- Always edit originals, never the copied files

---

## Data Quality (Expectations)

```yaml
- name: quality_check
  type: transform
  transform_type: data_quality    # req
  source: v_cleaned_data          # req
  target: v_validated_data        # req
  readMode: stream                # must be stream
  expectations_file: "expectations/table_quality.json"  # req
```

**Expectations JSON format:**
```json
{
  "expectations": [
    {"name": "not_null_id", "expression": "id IS NOT NULL", "failureAction": "fail"},
    {"name": "valid_email", "expression": "email LIKE '%@%'", "failureAction": "warn"},
    {"name": "no_outliers", "expression": "amount < 100000", "failureAction": "drop"}
  ]
}
```

**failureAction values:**
- `fail` — stop pipeline on violation
- `warn` — log warning, continue processing
- `drop` — remove violating records, continue

---

## Schema Transform

```yaml
# External schema file (recommended)
- name: standardize_schema
  type: transform
  transform_type: schema          # req
  source: v_raw_data              # req — must be simple string, NOT dict
  target: v_standardized          # req
  schema_file: "schemas/bronze/table_transform.yaml"  # req (one of schema_file or schema_inline)
  enforcement: strict             # opt (default: permissive)
  readMode: stream                # opt (default: stream)

# Inline schema
- name: standardize_inline
  type: transform
  transform_type: schema
  source: v_raw_data
  target: v_standardized
  schema_inline: |                # req (one of schema_file or schema_inline)
    c_custkey -> customer_id: BIGINT
    c_name -> customer_name
    account_balance: DECIMAL(18,2)
  enforcement: strict
```

**Arrow format syntax:**
```yaml
columns:
  - "old_col -> new_col: BIGINT"   # Rename and cast
  - "old_col -> new_col"           # Rename only
  - "col: DECIMAL(18,2)"          # Cast only
  - "col"                         # Pass-through (strict mode: explicitly keep)
```

**Enforcement modes:**
- **strict** — only defined columns kept; all unmapped columns dropped
- **permissive** (default) — defined columns transformed, all others pass through unchanged

**Key:** Source must be a simple string (view name), not a dict. The old nested format with `source.view` is no longer supported.

---

## Temp Table

```yaml
# Simple passthrough
- name: create_temp
  type: transform
  transform_type: temp_table      # req
  source: v_processed             # req
  target: temp_intermediate       # req
  readMode: stream

# With SQL transformation
- name: create_temp_agg
  type: transform
  transform_type: temp_table
  source: v_raw_data
  target: temp_daily_summary
  readMode: stream
  sql: |                          # opt — inline SQL
    SELECT
      DATE(order_date) as date,
      COUNT(*) as order_count
    FROM stream({source})
    GROUP BY DATE(order_date)
```

**Key:** Use `stream({source})` in SQL when `readMode: stream`. Temp tables are automatically cleaned up when the pipeline completes. Useful for complex multi-step transforms where intermediate materialization improves performance.

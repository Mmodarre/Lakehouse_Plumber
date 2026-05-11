# CDC Patterns and SCD2 Implementation

Best practices for implementing Change Data Capture (CDC) and Slowly Changing Dimensions (SCD) in LHP — covering Delta CDF, PostgreSQL WAL, and snapshot CDC patterns.

## General CDC Patterns

### Delta Change Data Feed (CDF)

The most common CDC pattern — read changes from a CDF-enabled Delta table:

```yaml
- name: load_changes
  type: load
  readMode: stream
  source:
    type: delta
    database: "{catalog}.{bronze_schema}"
    table: customer
    options:
      readChangeFeed: "true"
  target: v_customer_changes
```

Then write as SCD Type 1 or 2:
```yaml
- name: write_dimension
  type: write
  source: v_customer_changes
  write_target:
    type: streaming_table
    database: "{catalog}.{silver_schema}"
    table: dim_customer
    mode: cdc
    cdc_config:
      keys: [customer_id]
      sequence_by: _commit_timestamp
      scd_type: 2
```

### Source-Agnostic Design Decisions

These principles apply regardless of CDC source (Delta CDF, PostgreSQL WAL, Kafka, etc.):

1. **`sequence_by`** — use business timestamps when available (`modified_at`, `created_at`), fall back to system timestamps (`_commit_timestamp`, LSN)
2. **`except_column_list`** — exclude technical columns (e.g., `_sequence_timestamp`) from SCD2 history tracking
3. **`* except` in transforms** — future-proof column selection that auto-includes new columns
4. **`operational_metadata`** at action level, not write level

## PostgreSQL WAL CDC Sources

### Understanding WAL Metadata Columns

PostgreSQL CDC sources include WAL (Write-Ahead Log) metadata columns:

```
__START_AT: STRUCT<
  __cdc_internal_value: STRING,      # PostgreSQL LSN (Log Sequence Number)
  __cdc_timestamp_value: TIMESTAMP   # Often NULL
>
__END_AT: STRUCT<...>                # Similar structure, NULL for current records
```

**Key Points:**
- `__cdc_internal_value` contains the PostgreSQL LSN (e.g., "00000067/FE0034E8-0000000000000000000")
- `__cdc_timestamp_value` could be NULL - check!
- These columns are stored as JSON strings that need parsing
- **Exclude these columns from business layers** - they're ingestion metadata only

### Best Practice: Use Business Timestamps for SCD2

Instead of relying on WAL metadata, use business audit columns for sequencing IF available and reliable:

```yaml
# Use COALESCE to handle records without modifications
sequence_by: COALESCE(CAST(modified_at AS TIMESTAMP), CAST(created_at AS TIMESTAMP))
```

## SCD2 FlowGroup Pattern

### Complete Example: Invoice SCD2 from PostgreSQL CDC

```yaml
pipeline: billing_bronze
flowgroup: invoice_scd2

variables:
  source_table: brickwell_health.billing_ingestion_schema.invoice
  target_table: invoice

actions:
  # Load from CDC source
  - name: load_invoice_cdc
    type: load
    operational_metadata: [_processing_timestamp]  # Add timestamp only, no file metadata
    readMode: stream
    source:
      type: delta
      table: "%{source_table}"
    target: v_invoice_raw

  # Transform: Exclude CDC metadata, add sequence timestamp
  - name: add_sequence_timestamp
    type: transform
    transform_type: sql
    operational_metadata: [_processing_timestamp]
    source: v_invoice_raw
    target: v_invoice_with_sequence
    sql: |
      SELECT
        -- Exclude CDC metadata and audit columns using * except
        -- This pattern auto-includes new columns added to source
        * except (
          __START_AT,           -- PostgreSQL WAL metadata
          __END_AT,             -- PostgreSQL WAL metadata
          created_at,           -- Available in ingestion layer if needed
          modified_at,          -- Used only for sequencing
          created_by,           -- Available in ingestion layer if needed
          modified_by           -- Available in ingestion layer if needed
        ),
        -- Add sequence timestamp for SCD2 ordering
        COALESCE(
          CAST(modified_at AS TIMESTAMP),
          CAST(created_at AS TIMESTAMP)
        ) AS _sequence_timestamp
      FROM STREAM(v_invoice_raw)

  # Write as SCD Type 2
  - name: write_invoice_scd2
    type: write
    source: v_invoice_with_sequence
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: "%{target_table}"
      mode: cdc
      cdc_config:
        keys:
          - invoice_number        # Natural business key
        sequence_by: _sequence_timestamp
        except_column_list:
          - _sequence_timestamp   # Exclude technical column from history tracking
        scd_type: 2
```

## Key Design Decisions Explained

### 1. Use except_column_list for Technical Columns

**Pattern:**
```yaml
cdc_config:
  except_column_list:
    - _sequence_timestamp
```

**Rationale:**
- `_sequence_timestamp` is a technical column for SCD2 ordering only
- Should NOT be tracked in `__START_AT`, `__END_AT`, or `__CURRENT` columns
- SCD2 will use it for ordering but won't track its changes

### 2. Operational Metadata Placement

**Pattern:**
```yaml
# At action level, not write level
- name: load_data
  type: load
  operational_metadata: [_processing_timestamp]  # ✅ Correct

- name: transform_data
  type: transform
  operational_metadata: [_processing_timestamp]  # ✅ Correct

- name: write_data
  type: write
  operational_metadata: [_processing_timestamp]  # ✅ Correct
```

**Rules:**
- Apply `operational_metadata` at **action level** (load, transform)
- **Do not** add at write level - it's redundant
- For **file sources** (cloudfiles): include file metadata
  ```yaml
  operational_metadata: [_source_file_path, _source_file_name, _processing_timestamp]
  ```
- For **non-file sources** (delta, sql, jdbc): only processing timestamp
  ```yaml
  operational_metadata: [_processing_timestamp]
  ```

## Common Patterns

### Pattern 1: SCD2 with Business Audit Columns

Use when source has `created_at`, `modified_at` columns (any CDC source):

```yaml
sql: |
  SELECT
    * except (__START_AT, __END_AT, created_at, modified_at, created_by, modified_by),
    COALESCE(
      CAST(modified_at AS TIMESTAMP),
      CAST(created_at AS TIMESTAMP)
    ) AS _sequence_timestamp
  FROM STREAM(v_raw)
```

### Pattern 2: SCD2 from PostgreSQL CDC without Audit Columns

Use when source only has CDC metadata:

```yaml
sql: |
  SELECT
    * except (__START_AT, __END_AT),
    -- Parse LSN for sequencing (less ideal but works)
    get_json_object(__START_AT, '$.__cdc_internal_value') AS _sequence_lsn
  FROM STREAM(v_raw)

# In cdc_config:
sequence_by: _sequence_lsn  # String-based LSN ordering
```

### Pattern 3: Future-Proof Column Selection (Any CDC Source)

Always use `* except` pattern for automatic inclusion of new columns:

```yaml
# ✅ Good - new columns automatically included
* except (metadata_col1, metadata_col2)

# ❌ Avoid - requires manual updates when columns added
col1, col2, col3, ...
```

## Troubleshooting

### Issue: "sequence_by column not found"
**Cause:** Sequence column excluded or not created
**Solution:** Ensure sequence column is created in transform and not in `except_column_list`

### Issue: "Technical column appearing in history"
**Cause:** Missing `except_column_list` in cdc_config
**Solution:** Add technical columns to `except_column_list`:
```yaml
except_column_list:
  - _sequence_timestamp
  - _processing_timestamp
```

### Issue: "Too many columns in SCD2 table"
**Cause:** Not excluding CDC metadata from source
**Solution:** Use `* except (__START_AT, __END_AT, ...)` in transform

## Summary Checklist

When implementing SCD2 (any CDC source):

- [ ] Choose `sequence_by` column: business timestamp (preferred) or system timestamp
- [ ] Use `* except` pattern in transforms for future-proof column selection
- [ ] Exclude CDC metadata columns in transform (e.g., `__START_AT`, `__END_AT` for PostgreSQL)
- [ ] Create `_sequence_timestamp` if using derived sequencing: `COALESCE(modified_at, created_at)`
- [ ] Add technical columns to `except_column_list` in cdc_config (e.g., `[_sequence_timestamp]`)
- [ ] Set appropriate natural business keys in `keys` field
- [ ] Apply `operational_metadata` at action level only
- [ ] Use `readChangeFeed: "true"` for Delta CDF sources

**PostgreSQL-specific additions:**
- [ ] Exclude `__START_AT` and `__END_AT` WAL metadata in transforms
- [ ] Exclude audit columns (`created_at`, `modified_at`, etc.) if not needed in target

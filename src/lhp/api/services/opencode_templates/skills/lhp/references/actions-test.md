# Test Actions Reference

Test actions validate data pipelines using DLT expectations in temporary tables.

**CLI flag required:** `--include-tests` (tests are skipped by default for faster builds)

```bash
lhp generate -e dev --include-tests
```

## Common Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | **req** | Unique name within the FlowGroup |
| `type` | **req** | Must be `test` |
| `test_type` | **req** | One of the 9 types below |
| `source` | **req** | Source table/view (list of 2 for row_count) |
| `on_violation` | opt | `fail` (default) or `warn` |
| `target` | opt | Table name (default: `tmp_test_<action_name>`) |
| `description` | opt | Documentation for the action |

---

## Test Types

### row_count

Compare record counts between two sources with optional tolerance.

```yaml
- name: test_raw_to_bronze_count
  type: test
  test_type: row_count
  source: [raw.orders, bronze.orders]   # req — list of exactly 2 sources
  tolerance: 0                          # opt (default: 0)
  on_violation: fail
  description: "Ensure no data loss from raw to bronze"
```

### uniqueness

Validate unique constraints on one or more columns. Optional `filter` for SCD2 active rows.

```yaml
# Global uniqueness
- name: test_order_id_unique
  type: test
  test_type: uniqueness
  source: silver.orders
  columns: [order_id]                   # req — list of column names
  on_violation: fail

# SCD2: only one active record per key
- name: test_customer_active_unique
  type: test
  test_type: uniqueness
  source: silver.customer_dim
  columns: [customer_id]
  filter: "__END_AT IS NULL"            # opt — SQL WHERE clause
  on_violation: fail
```

### referential_integrity

Check foreign key relationships between tables.

```yaml
- name: test_orders_customer_fk
  type: test
  test_type: referential_integrity
  source: silver.fact_orders
  reference: silver.dim_customer        # req — reference table
  source_columns: [customer_id]         # req — FK columns in source
  reference_columns: [customer_id]      # req — PK columns in reference
  on_violation: fail
```

### completeness

Ensure specific columns are not null. Generator selects only required columns for efficiency.

```yaml
- name: test_customer_required_fields
  type: test
  test_type: completeness
  source: silver.dim_customer
  required_columns: [customer_key, customer_id, name, nation_id]  # req
  on_violation: fail
```

### range

Validate a column falls within min/max bounds. Generator selects only the tested column.

```yaml
- name: test_order_date_range
  type: test
  test_type: range
  source: silver.orders
  column: order_date                    # req
  min_value: '2020-01-01'              # opt
  max_value: 'current_date()'          # opt
  on_violation: fail
```

### schema_match

Compare schemas between two tables via `information_schema.columns`.

```yaml
- name: test_orders_schema_match
  type: test
  test_type: schema_match
  source: silver.fact_orders
  reference: gold.fact_orders_expected  # req — reference table
  on_violation: fail
```

### all_lookups_found

Validate that dimension lookups succeed (surrogate keys present after joins).

```yaml
- name: test_order_date_lookup
  type: test
  test_type: all_lookups_found
  source: silver.fact_orders
  lookup_table: silver.dim_date         # req
  lookup_columns: [order_date]          # req — join columns in source
  lookup_result_columns: [date_key]     # req — expected columns from lookup
  on_violation: fail
```

### custom_sql

Custom SQL query with explicit expectations.

```yaml
- name: test_revenue_reconciliation
  type: test
  test_type: custom_sql
  source: gold.monthly_revenue
  sql: |                                # req — custom SQL
    SELECT month, gold_revenue, silver_revenue,
           (ABS(gold_revenue - silver_revenue) / silver_revenue) * 100 as pct_difference
    FROM ...
  expectations:                         # opt — list of expectations
    - name: revenue_matches
      expression: "pct_difference < 0.5"
      on_violation: fail
```

### custom_expectations

Attach arbitrary expectations to an existing table/view without custom SQL.

```yaml
- name: test_orders_business_rules
  type: test
  test_type: custom_expectations
  source: silver.fact_orders
  expectations:                         # req — list of expectations
    - name: positive_amount
      expression: "total_price > 0"
      on_violation: fail
    - name: reasonable_discount
      expression: "discount_percent <= 50"
      on_violation: warn
```

---

## Type-Specific Fields Summary

| Test Type | Required Fields | Optional Fields |
|-----------|----------------|-----------------|
| `row_count` | `source` (list of 2) | `tolerance` (int, default 0) |
| `uniqueness` | `columns` (list) | `filter` (SQL WHERE) |
| `referential_integrity` | `reference`, `source_columns`, `reference_columns` | — |
| `completeness` | `required_columns` (list) | — |
| `range` | `column` | `min_value`, `max_value` |
| `schema_match` | `reference` | — |
| `all_lookups_found` | `lookup_table`, `lookup_columns`, `lookup_result_columns` | — |
| `custom_sql` | `sql` | `expectations` (list) |
| `custom_expectations` | `expectations` (list) | — |

---

## Best Practices

- Use `on_violation: fail` for hard constraints, `warn` for observability
- Use `filter` for SCD2 active rows in uniqueness tests (e.g., `"__END_AT IS NULL"`)
- Keep SQL minimal — project only required columns
- Group expectations by severity for consolidated DLT UI reporting

# Builder YAML Generation — Implementation Status

## What Was Done

Overhauled the builder's YAML generation layer so 12 of 15 action types produce
complete, spec-compliant YAML through dedicated forms instead of falling back to
raw YAML editing.

### Infrastructure

| File | Change |
|------|--------|
| `hooks/useYAMLGenerator.ts` | Added `stripEmpty()` utility (recursive empty-value pruning). Added `getMultiSourceFromEdges()` for multi-source transforms. All 12 mappers refactored/created here. Exported `stripEmpty` for potential reuse. |
| `config/forms/shared/SqlOrFileToggle.tsx` | New shared component — toggle between inline SQL/schema textarea and external file path input. Used by MV Write, SQL Load, and ready for any future form needing the pattern. |

### Fixed Existing Forms

| Action | Key | Files | What Changed |
|--------|-----|-------|--------------|
| **CloudFiles Load** | `load:cloudfiles` | `CloudFilesLoadForm.tsx`, mapper | Added `operational_metadata` (ArrayInput), `inferColumnTypes` (Switch), `rescuedDataColumn` (Input). Mapper uses `stripEmpty`. |
| **SQL Transform** | `transform:sql_transform` | mapper only | Mapper refactored to `stripEmpty`. Form was already complete. |
| **Streaming Table Write** | `write:streaming_table` | `StreamingTableWriteForm.tsx`, mapper | Added: `description`, `readMode`, `create_table` (Switch), `table_schema` (textarea), `row_filter`, `spark_conf` (KeyValueInput), `once` (Switch), `snapshot_cdc` mode, full CDC sub-fields (`ignore_null_updates`, `apply_as_deletes`, `apply_as_truncates`, `track_history_column_list`, `track_history_except_column_list`, `except_column_list`). |
| **Materialized View Write** | `write:materialized_view` | `MaterializedViewWriteForm.tsx`, mapper | Added: `partition_columns`, `table_schema`, `row_filter`, `description`. SQL config keys renamed to `wt_sql`/`wt_sql_file` to avoid collision with action-level fields. Source auto-omitted when write_target has SQL. Now uses `SqlOrFileToggle`. |

### New Forms Created

| Action | Key | Form File | Mapper | Notes |
|--------|-----|-----------|--------|-------|
| **Delta Load** | `load:delta` | `DeltaLoadForm.tsx` | `mapDeltaLoad` | Full spec: database, table, where_clause, select_columns, all Delta reader options (readChangeFeed, startingVersion, startingTimestamp, versionAsOf, timestampAsOf, ignoreDeletes, skipChangeCommits, maxFilesPerTrigger). |
| **SQL Load** | `load:sql` | `SQLLoadForm.tsx` | `mapSQLLoad` | Uses `SqlOrFileToggle` for inline SQL vs file path. Default read mode: batch. |
| **Python Load** | `load:python` | `PythonLoadForm.tsx` | `mapPythonLoad` | module_path, function_name (default "get_df"), parameters (KeyValueInput), operational_metadata. |
| **Custom DataSource Load** | `load:custom_datasource` | `CustomDataSourceLoadForm.tsx` | `mapCustomDataSourceLoad` | module_path, custom_datasource_class, options (KeyValueInput), operational_metadata. |
| **Python Transform** | `transform:python_transform` | `PythonTransformForm.tsx` | `mapPythonTransform` | Multi-source support (checkbox list from upstream nodes), module_path, function_name, parameters, operational_metadata. |
| **Data Quality Transform** | `transform:data_quality` | `DataQualityTransformForm.tsx` | `mapDataQualityTransform` | expectations_file (required), read mode (default stream). |
| **Temp Table Transform** | `transform:temp_table` | `TempTableTransformForm.tsx` | `mapTempTableTransform` | Optional SQL, read mode. |
| **Schema Transform** | `transform:schema` | `SchemaTransformForm.tsx` | `mapSchemaTransform` | Inline/file toggle for schema, enforcement (strict/permissive), read mode. |

### Routing & Catalog Updates

| File | Change |
|------|--------|
| `config/ActionFormRouter.tsx` | 10 new `case` branches added (was 4, now 12). Only JDBC, Kafka, Sink fall through to `GenericYAMLActionForm`. |
| `hooks/useActionCatalog.ts` | `hasMVPForm` flipped to `true` for all 10 newly implemented types. Only JDBC, Kafka, Sink remain `false`. |

---

## Remaining Action Types (3)

These still fall back to `GenericYAMLActionForm` (raw YAML textarea).

### 1. JDBC Load (`load:jdbc`)

**Complexity: Medium.** Two mutually exclusive modes (query-based vs table-based), credentials via
secret substitution, multiple required fields.

**YAML structure:**
```yaml
- name: load_postgres
  type: load
  readMode: batch
  source:
    type: jdbc
    url: "jdbc:postgresql://host:5432/db"
    driver: "org.postgresql.Driver"
    user: "${secret:scope/username}"
    password: "${secret:scope/password}"
    query: "SELECT * FROM ..."     # OR table: "products" — mutually exclusive
  target: v_external_data
```

**Form fields needed:**
- `url` (required, text) — JDBC connection URL
- `driver` (required, select or text) — common drivers: PostgreSQL, MySQL, SQL Server, Oracle
- `user` (required, text) — supports `${secret:...}` substitution
- `password` (required, text) — supports `${secret:...}` substitution
- Mode toggle: Query vs Table (mutually exclusive)
  - `query` (textarea, mono) — SQL query
  - `table` (text) — table name
- Target view, read mode (default batch), description

**Mapper:** `mapJDBCLoad()` — build source with type `jdbc`, conditionally include `query` or `table`.

### 2. Kafka Load (`load:kafka`)

**Complexity: High.** Multiple subscription modes, auth patterns (AWS MSK IAM, Azure Event Hubs
OAuth, SASL/PLAIN), many Kafka-specific options.

**YAML structure:**
```yaml
- name: load_kafka_events
  type: load
  readMode: stream
  source:
    type: kafka
    bootstrap_servers: "broker1:9092,broker2:9092"
    subscribe: "topic1,topic2"       # OR subscribePattern OR assign — one required
    options:
      startingOffsets: "latest"
      failOnDataLoss: false
      kafka.security.protocol: "SASL_SSL"
  target: v_kafka_raw
```

**Form fields needed:**
- `bootstrap_servers` (required, text)
- Subscription mode toggle (exactly one required):
  - `subscribe` (text) — comma-separated topic list
  - `subscribePattern` (text) — Java regex
  - `assign` (textarea, JSON) — specific partitions
- Target view, description
- Advanced: `startingOffsets` (select: latest/earliest/JSON), `failOnDataLoss` (switch), security options (KeyValueInput), other Kafka options
- Consider an auth preset selector (AWS MSK IAM, Azure Event Hubs, plain SASL) that pre-fills common security option blocks

**Mapper:** `mapKafkaLoad()` — build source with type `kafka`, merge subscription + options.

### 3. Sink Write (`write:sink`)

**Complexity: High.** Four distinct sink subtypes (delta, kafka, custom, foreachbatch), each with
different required fields and YAML structure. This is effectively 4 mini-forms behind a sink_type selector.

**YAML structure (varies by sink_type):**
```yaml
# Delta Sink
write_target:
  type: sink
  sink_type: delta
  sink_name: analytics_export
  options:
    tableName: "catalog.schema.table"   # OR path — mutually exclusive

# Kafka Sink
write_target:
  type: sink
  sink_type: kafka
  sink_name: order_events
  bootstrap_servers: "broker:9092"
  topic: "output-topic"
  options: { ... }

# Custom Sink
write_target:
  type: sink
  sink_type: custom
  sink_name: crm_api
  module_path: "sinks/api_sink.py"
  custom_sink_class: "APIDataSource"
  options: { ... }

# ForEachBatch Sink
write_target:
  type: sink
  sink_type: foreachbatch
  sink_name: customer_merge
  module_path: "batch_handlers/merge.py"
```

**Form fields needed:**
- Source (upstream select)
- `sink_type` selector (delta / kafka / custom / foreachbatch)
- `sink_name` (required, text)
- `comment` (text)
- Conditional fields per sink_type:
  - **delta:** `tableName` vs `path` toggle, `mergeSchema`
  - **kafka:** `bootstrap_servers`, `topic`, security options
  - **custom:** `module_path`, `custom_sink_class`, options
  - **foreachbatch:** `module_path`

**Mapper:** `mapSinkWrite()` — dispatch on `config.sink_type` to build the appropriate write_target.

---

## Also Not in Builder (by design)

### Test Actions (`type: test`)

Test actions are a separate action type with 9 test subtypes (row_count, not_null, unique,
referential_integrity, etc.). They are not currently part of the builder canvas at all —
the `ActionType` union in `types/builder.ts` does not include `'test'`, and no catalog entries
exist for them.

Adding test support would require:
1. Extending `ActionType` to include `'test'`
2. Adding `ActionSubtype` entries for each test type
3. Adding catalog entries in `useActionCatalog.ts`
4. Building forms + mappers for 9 test types
5. Canvas UX decisions (tests connect differently — they often reference two sources)

---

## Files to Read Before Starting on Remaining Types

### Spec / Reference (the source of truth for YAML structure)

| File | Content |
|------|---------|
| `src/lhp/api/services/opencode_templates/skills/lhp/references/actions-load.md` | Full JDBC (line 127) and Kafka (line 161) load specs with all field docs and auth patterns |
| `src/lhp/api/services/opencode_templates/skills/lhp/references/actions-write.md` | Full Sink spec (line 198) — all 4 sink subtypes with YAML examples |
| `src/lhp/api/services/opencode_templates/skills/lhp/references/actions-test.md` | Test actions spec (9 test types) — only needed if adding test support |

### Validators (know what the backend requires/rejects)

| File | Content |
|------|---------|
| `src/lhp/core/validators/load_validator.py` | Validation rules for JDBC and Kafka loads (required fields, mutual exclusions) |
| `src/lhp/core/validators/write_validator.py` | Validation rules for sink writes |
| `src/lhp/utils/kafka_validator.py` | Kafka-specific validation (subscription modes, security protocol checks) |

### Generators (know how the YAML gets turned into Python)

| File | Content |
|------|---------|
| `src/lhp/generators/load/jdbc.py` | JDBC code generator — shows which fields are actually used |
| `src/lhp/generators/load/kafka.py` | Kafka code generator — shows option handling |
| `src/lhp/generators/write/sink.py` | Sink dispatcher (routes to subtype generators) |
| `src/lhp/generators/write/sinks/delta_sink.py` | Delta sink generator |
| `src/lhp/generators/write/sinks/kafka_sink.py` | Kafka sink generator |
| `src/lhp/generators/write/sinks/custom_sink.py` | Custom sink generator |
| `src/lhp/generators/write/sinks/foreachbatch_sink.py` | ForEachBatch sink generator |

### Schema (JSON Schema validation — catches issues before generator runs)

| File | Content |
|------|---------|
| `src/lhp/schemas/flowgroup.schema.json` | JSON Schema for flowgroup YAML — definitive field reference with types and enums |

### Builder code (understand existing patterns)

| File | Content |
|------|---------|
| `hooks/useYAMLGenerator.ts` | All existing mappers + `stripEmpty` helper. Add new mappers here. |
| `config/ActionFormRouter.tsx` | Switch routing — add new `case` entries. |
| `hooks/useActionCatalog.ts` | Flip `hasMVPForm: true` when done. |
| `config/forms/shared/` | Reusable components: `FormField`, `FormSection`, `KeyValueInput`, `ArrayInput`, `SqlOrFileToggle`. |
| `config/forms/StreamingTableWriteForm.tsx` | Best example of a complex form (CDC sub-fields, advanced options, switches). |
| `config/forms/DeltaLoadForm.tsx` | Good example of a load form with many optional reader options. |
| `types/builder.ts` | TypeScript types — `ActionSubtype` already includes `jdbc`, `kafka`, `sink`. |

### Models (Pydantic — Python-side field definitions)

| File | Content |
|------|---------|
| `src/lhp/models/config.py` | `LoadSourceType`, `WriteTargetType` enums, full Pydantic model with all fields and defaults |

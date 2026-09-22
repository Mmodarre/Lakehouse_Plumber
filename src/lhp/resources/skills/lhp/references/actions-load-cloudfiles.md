# Load — cloudfiles (Auto Loader)

`type: load` with `source.type: cloudfiles`. Streams files into a temporary view via Databricks Auto Loader. Handler: `CloudFilesLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `path` | string | — | File path or glob. |
| `format` | string | `json` | Mandatory; feeds `cloudFiles.format`. |
| `readMode` | string | `stream` | Must be `stream` (`batch` rejected). |
| `schema` | string / `{file: ...}` | — | Schema-file path. Mutually exclusive with `cloudFiles.schemaHints` and legacy `schema_file`; disables schema evolution when set. |
| `schema_file` | string | — | Back-compat schema path. |
| `options` | dict | — | `cloudFiles.*` options (must be a dict). |
| `reader_options` | dict | — | Merged verbatim into the reader. |
| `format_options` | dict | — | Prefixed with `{format}.`. |

## Minimal YAML

```yaml
- name: load_orders
  type: load
  source:
    type: cloudfiles
    path: "${landing_volume}/orders/*.json"
    format: json
    readMode: stream
  target: v_orders_raw
```

## Key rules

- `cloudFiles.format` is auto-injected from `source.format` if omitted.
- `schemaHints` accepts inline DDL (`"id BIGINT, name STRING"`), a YAML file (`schemas/x.yaml`, supports `nullable: false` → `NOT NULL`), or a `.ddl`/`.sql` file (auto-detected).
- `_metadata.*` columns (`file_path`, `file_size`, `file_modification_time`) are only available in views, not downstream transforms.
- Common `options`: `cloudFiles.schemaEvolutionMode`, `cloudFiles.rescuedDataColumn`, `cloudFiles.maxFilesPerTrigger`, `cloudFiles.maxBytesPerTrigger`, `cloudFiles.inferColumnTypes`, `cloudFiles.includeExistingFiles`, `cloudFiles.useIncrementalListing`, `cloudFiles.useNotifications`.
- All options mirror Databricks Auto Loader options.

## Path filtering

- **`pathGlobFilter`** (under `options`) narrows which files are read; it matches the **basename only**, e.g. `pathGlobFilter: "*.parquet"`.
- Globs in `path`: `*`, `?`, char classes `[a-z]`, brace alternatives `{a,b,c}`. Use `[^x]` for negation — Spark does **not** support the Unix `[!x]`. Avoid the `**` globstar (undocumented for Auto Loader).
- `cloudFiles.useStrictGlobber: "true"` (DBR 12.2+) gives predictable Spark-standard globbing; the default globber is more permissive and `*` can cross directory boundaries.
- For filtering globs can't express, post-filter in a SQL transform on `_metadata.file_path`, e.g. `WHERE NOT _metadata.file_path RLIKE '.*/exclude_[^/]+/.*'`.

## Legacy scalar options

These `source:` keys are compatibility aliases. Prefer the corresponding
`cloudFiles.*` options for new configurations; the generator rejects a legacy
key and its mapped option when both are supplied.

| Legacy key | Current LHP option mapping | Purpose |
|------------|----------------------------|---------|
| `schema_location` | `cloudFiles.schemaLocation` | Stores inferred schema state; it is not a schema-definition file. |
| `schema_infer_column_types` | `cloudFiles.inferColumnTypes` | Infers column types during schema discovery. |
| `max_files_per_trigger` | `cloudFiles.maxFilesPerTrigger` | Limits the number of files in a micro-batch. |
| `schema_evolution_mode` | `cloudFiles.schemaEvolutionMode` | Chooses handling for newly discovered columns. |
| `rescue_data_column` | `cloudFiles.rescueDataColumn` | Current legacy mapping in LHP. For the documented Auto Loader option, use `options.cloudFiles.rescuedDataColumn` instead and inspect generated settings when migrating. |

`source.schema` is a schema-file path, including when represented as a string.
Inline DDL belongs in `source.options.cloudFiles.schemaHints`. The schema editor
must distinguish this from Delta's `source.schema`, which names a Unity Catalog
schema. Leaving `source.schema` unset does not select inference if the legacy
`schema_file` is still supplied.

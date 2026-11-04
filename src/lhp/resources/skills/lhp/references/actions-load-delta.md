# Load — delta

`type: load` with `source.type: delta`. Reads a Delta table into a temporary view. Handler: `DeltaLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `table` | string | — | Table name. |
| `catalog` | string | — | Three-part reference when combined with `schema`. |
| `schema` | string | — | Schema name. |
| `readMode` | string | `batch` | `batch` or `stream`. |
| `options` | dict | — | Delta reader options; values must be non-empty. |
| `where_clause` | list | `[]` | Filter clauses. |
| `select_columns` | list | — | Projection. |

## Minimal YAML

```yaml
- name: load_customers
  type: load
  source:
    type: delta
    catalog: "${catalog}"
    schema: "${bronze_schema}"
    table: customers
    readMode: batch
  target: v_customers
```

## Key rules

- Use `catalog` + `schema` + `table` for the three-part name.
- CDC: `options.readChangeFeed: "true"` requires `readMode: stream`; pair with `startingVersion` / `startingTimestamp`.
- Time travel (batch): `versionAsOf` or `timestampAsOf`.
- Other options: `ignoreDeletes`, `skipChangeCommits`, `maxFilesPerTrigger`.
- The view a delta load creates is a batch view by default and a streaming view with `readMode: stream`; later actions in the flowgroup must read it the same way (`LHP-VAL-070`). A `data_quality` transform with no `readMode` inherits it.

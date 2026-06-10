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

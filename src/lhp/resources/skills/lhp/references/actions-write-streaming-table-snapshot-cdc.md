# Write — streaming_table (snapshot CDC)

`type: write`, `write_target.type: streaming_table` with `mode: snapshot_cdc`. Full-snapshot CDC via `create_auto_cdc_from_snapshot_flow()`. Requires `snapshot_cdc_config`. `create_table` is forced `true` in this mode. Handler: `StreamingTableWriteGenerator` (validated by `SnapshotCDCValidator`).

## Options (under `write_target.snapshot_cdc_config:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | one-of | Exactly one of `source` / `source_function`. |
| `source_function` | dict | one-of | `{file, function, parameters?}`; `file` and `function` required. |
| `keys` | list[string] | required | Non-empty. |
| `stored_as_scd_type` | int | — | `1` or `2`. |
| `track_history_column_list` | list[string] | — | Mutually exclusive with `track_history_except_column_list`. |
| `track_history_except_column_list` | list[string] | — | Mutually exclusive with `track_history_column_list`. |

## Minimal YAML

```yaml
- name: write_customer_snapshot
  type: write
  write_target:
    type: streaming_table
    mode: snapshot_cdc
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: customer_dim
    snapshot_cdc_config:
      source: "${catalog}.${bronze_schema}.customer_snapshot"
      keys: ["customer_id"]
      stored_as_scd_type: 2
```

## `source_function` form

```yaml
snapshot_cdc_config:
  source_function:
    file: "snapshots/customer.py"        # relative to project root
    function: next_snapshot
    parameters:                          # optional; passed to the function
      catalog: "${catalog}"
      schema: "${bronze_schema}"
  keys: ["customer_id"]
  stored_as_scd_type: 2
```

The function is emitted as the `source` argument of `create_auto_cdc_from_snapshot_flow(...)`. With `parameters`, each entry is bound as a **keyword argument** via `functools.partial` — e.g. `source=partial(next_snapshot, catalog="prod", schema="bronze")` — so the function must declare them as keyword-only args (after `*`). Substitution tokens in parameter values are resolved at generate time, before binding. A common signature returns a snapshot for a given version: `def next_snapshot(latest_version, *, catalog, schema) -> tuple[DataFrame, int] | None` (return `None` to stop).

## Key rules

- `source` and `source_function` are mutually exclusive.
- With `source_function`, the action is **self-contained** — no action-level `source` field needed, and it is exempt from the "must have a Load action" requirement. `source_function.file` is resolved relative to project root.
- **The snapshot function runs OUTSIDE SDP's decorated context** (it is handed to `create_auto_cdc_from_snapshot_flow` as a plain callable). It therefore **cannot reference an SDP dataset** — a temp view or streaming table built by another action in the same pipeline. SDP only allows referencing SDP objects from inside decorated functions. The function must read its source **directly** (e.g. a Delta table or path via `spark.read...`). Consequently **no Load action is needed — or even possible — to feed it**; do not add a Load/transform view as its input.
- Same applies to a plain `source:` string — it points at an external table/path, not an in-pipeline view.
- Local helper imports (snapshot `source_function`) follow the standard rules: `LHP-VAL-023/024/025`, `LHP-IO-003`. The whole file is copied, so helper functions defined alongside `function` are preserved and callable.
- **Dependency analysis** (`lhp dag`): `source_function.parameters` are statically resolved — bound to the function's keyword-only args exactly as codegen applies them (function looked up at module top level by `function` name; `${token}` bytes preserved, never resolved at dag time) — so reads like `spark.read.table(f"{catalog}.{schema}.t")` over bound parameters resolve to edges. Helper-routed / runtime-only reads are opaque by design → advisory `LHP-DEP-002` (warning-only, never an error); declare those edges with the additive `depends_on` action field (malformed entries → `LHP-VAL-063`).

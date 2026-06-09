# Transform — schema

`type: transform` with `transform_type: schema`. Renames/casts/filters columns via a schema mapping. Fields are flat on the action. Requires exactly one of `schema_inline` or `schema_file`. Handler: `SchemaTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | View-name string only (not a dict). |
| `schema_inline` | string | — | One of `schema_inline` / `schema_file`. |
| `schema_file` | string | — | External schema file; one of `schema_inline` / `schema_file`. |
| `enforcement` | string | `permissive` | `strict` or `permissive`. |
| `readMode` | string | `stream` | Read mode. |

## Minimal YAML

```yaml
- name: enforce_schema
  type: transform
  transform_type: schema
  source: v_orders
  schema_file: "schemas/orders.yaml"
  enforcement: strict
  target: v_orders_typed
```

## Key rules

- Arrow syntax: `old_col -> new_col: BIGINT` (rename + cast), `old_col -> new_col` (rename), `col: DECIMAL(18,2)` (cast), `col` (pass-through / explicit keep in strict mode).
- `$` is allowed in any name that references a source column — the rename source (left of `->`), a cast-only `col: TYPE`, and a pass-through `col` — leading, internal, or trailing. Example: `"$revenue -> revenue: DECIMAL(18,2)"`. A rename target (right of `->`) must be a clean identifier (letters/digits/underscores only); a `$` there (e.g. `a -> $b`) is a syntax error → `LHP-VAL-011`.
- `strict` — only defined columns kept, unmapped columns dropped. `permissive` (default) — defined columns transformed, all others pass through unchanged.
- `source` must be a simple string; the old nested `source.view` format is no longer supported.

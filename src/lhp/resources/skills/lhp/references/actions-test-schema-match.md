# Test — schema_match

`type: test`, `test_type: schema_match`. Compares schemas between two tables via `information_schema.columns`. Fields are flat on the action. Handler: `SchemaMatchTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Three-part name (`catalog.schema.table`) of the table to check. |
| `reference` | string | required | Three-part name (`catalog.schema.table`) of the expected-schema table. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_schema_match
  type: test
  test_type: schema_match
  source: main.sales.orders
  reference: main.sales.orders_expected
```

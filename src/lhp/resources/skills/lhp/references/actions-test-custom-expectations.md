# Test — custom_expectations

`type: test`, `test_type: custom_expectations`. Attaches arbitrary expectations to an existing table/view without custom SQL. Fields are flat on the action. Handler: `CustomExpectationsTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view to check. |
| `expectations` | list | required | List of expectation dicts; each carries its own predicate and `on_violation`. |

## Minimal YAML

```yaml
- name: orders_custom_checks
  type: test
  test_type: custom_expectations
  source: v_orders_clean
  expectations:
    - name: positive_amount
      expression: "amount > 0"
      on_violation: fail
```

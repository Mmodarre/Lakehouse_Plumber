# Transform — python

`type: transform` with `transform_type: python`. Fields are flat on the action. Handler: `PythonTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `module_path` | string | required | Path to a `.py` file (relative to project root). |
| `function_name` | string | required | Function to call. |
| `parameters` | dict | `{}` | Passed to the function. |
| `source` | string / list | required | Input view(s); `None` is rejected. |
| `readMode` | string | `batch` | Read mode. |

## Minimal YAML

```yaml
- name: transform_enrich
  type: transform
  transform_type: python
  source: v_orders
  module_path: "transforms/enrich.py"
  function_name: enrich
  parameters:
    lookup: regions
  target: v_orders_enriched
```

## Key rules

- Function signatures: single source `def func(df, spark, parameters: dict) -> DataFrame`; multiple sources (`source` is a list) `def func(dataframes: List[DataFrame], spark, parameters: dict) -> DataFrame`; no source (generator) `def func(spark, parameters: dict) -> DataFrame`.
- Files auto-copied to `generated/<pipeline>/custom_python_functions/`; imports emitted as `from custom_python_functions.module import function`; copies carry a "DO NOT EDIT" header. Always edit originals.
- Local helper imports: transitive closure copied, sub-package structure preserved; import root must NOT be a package → `LHP-VAL-023`; `import helpers.x` → `LHP-VAL-024`; missing helper → `LHP-VAL-025`; broken sibling → `LHP-IO-003`. Relative imports preserved; absolute-local imports prefix-rewritten.

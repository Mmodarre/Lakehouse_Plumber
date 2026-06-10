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
- **The whole file is copied, not just the entry function.** Define helper functions alongside `function_name` in the same `.py` and call them from the entry function — they are preserved verbatim. Prefer factoring procedural logic into helpers the entry function calls over one giant function.
- Files auto-copied to `generated/<pipeline>/custom_python_functions/`; imports emitted as `from custom_python_functions.module import function`; copies carry a "DO NOT EDIT" header. Always edit originals.
- Local helper imports: transitive closure copied, sub-package structure preserved; import root must NOT be a package → `LHP-VAL-023`; `import helpers.x` → `LHP-VAL-024`; missing helper → `LHP-VAL-025`; broken sibling → `LHP-IO-003`. Relative imports preserved; absolute-local imports prefix-rewritten.

## Dependency analysis

- `lhp deps` statically extracts table reads from the copied Python: `spark.table(...)`, `spark.read.table(...)` / `spark.readStream.table(...)`, `spark.read.format("delta"|"iceberg"|"hive"|"unity_catalog").table(...)`/`.load(...)` (incl. `readStream`), `spark.sql("...")`, and statically-resolvable names (literals, module constants, well-known-placeholder f-strings, `+` concatenation, `"{}.{}".format(...)`). `cloudFiles` (Auto Loader) and `custom_datasource` reads stay external roots.
- For names the parser cannot resolve (function params, return values, non-static operands), declare edges explicitly with `depends_on` on the action: a list of upstream table refs (`catalog.schema.table` / `schema.table`). It is **additive** — entries add edges on top of whatever is parsed; matching is case-insensitive and does NOT resolve `${tokens}`. Malformed entries → `LHP-VAL-063`.

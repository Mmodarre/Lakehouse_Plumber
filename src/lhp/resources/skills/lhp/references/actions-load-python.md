# Load — python

`type: load` with `source.type: python`. Calls a Python function that returns a DataFrame. Handler: `PythonLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `module_path` | string | required | Must end in `.py`; relative to project root. |
| `function_name` | string | `get_df` | Function to call. |
| `parameters` | dict | `{}` | Passed to the function. |

## Minimal YAML

```yaml
- name: load_external
  type: load
  source:
    type: python
    module_path: "loaders/external.py"
    function_name: get_df
    parameters:
      region: us
  target: v_external
```

## Key rules

- Function signature: `def func(spark, parameters: dict) -> DataFrame`.
- Files are copied to `generated/` with substitution processing; edit the originals, never the copies.
- Local helper imports: the transitive closure is copied into `custom_python_functions/`, sub-package structure preserved. Import root must NOT be a package (no top-level `__init__.py`) → `LHP-VAL-023`. `import helpers.x` (plain dotted local) → `LHP-VAL-024`; missing helper → `LHP-VAL-025`; broken sibling in a copied package → `LHP-IO-003`.
- Choose `python` for one-shot DataFrame logic (batch); choose `custom_datasource` when you need Spark-managed streaming offsets.

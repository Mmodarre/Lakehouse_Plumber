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
- **The whole file is copied, not just the entry function.** Define additional helper functions (and module-level constants/imports) alongside `function_name` in the same `.py` and call them from the entry function — they are preserved verbatim. Keep `function_name` as the single entry point; factor everything else into helpers it calls.
- Files are copied to `generated/` with substitution processing; edit the originals, never the copies.
- Local helper imports: the transitive closure is copied into `custom_python_functions/`, sub-package structure preserved. Import root must NOT be a package (no top-level `__init__.py`) → `LHP-VAL-023`. `import helpers.x` (plain dotted local) → `LHP-VAL-024`; missing helper → `LHP-VAL-025`; broken sibling in a copied package → `LHP-IO-003`.
- Choose `python` for one-shot DataFrame logic (batch); choose `custom_datasource` when you need Spark-managed streaming offsets.
- **Dependency analysis** (`lhp dag`) statically extracts table reads from the copied Python (`spark.table`, `spark.read`/`readStream.table`, `spark.read.format("delta"|"iceberg"|"hive"|"unity_catalog").table`/`.load`, `spark.sql`, statically-resolvable names incl. string methods, f-strings over bound values, and static-list `for`-loop unrolling). YAML `source.parameters` is statically resolved — the dict is bound as the entry function's 2nd positional arg exactly as codegen passes it (`fn(spark, parameters)`; `function_name` defaults to `get_df`), so `parameters["k"]` / `parameters.get("k", default)` reads resolve with `${token}` bytes preserved. Helper-routed / runtime-only reads are opaque by design → advisory `LHP-DEP-002` (warning-only, never an error); declare those edges with the additive `depends_on` action field (list of `catalog.schema.table` / `schema.table` refs; malformed → `LHP-VAL-063`).

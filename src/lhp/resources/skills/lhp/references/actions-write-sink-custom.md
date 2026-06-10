# Write — sink (custom)

`type: write`, `write_target.type: sink` with `sink_type: custom`. Writes through a user-supplied PySpark `DataSink`. Handler: `CustomSinkWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `module_path` | string | required | `.py` DataSink module. |
| `custom_sink_class` | string | required | Class name. |
| `sink_name` | string | — | Unique identifier. |
| `options` | dict | `{}` | Passed to the sink. |
| `comment` | string | derived | — |

## Minimal YAML

```yaml
- name: write_to_custom_sink
  type: write
  source: v_seed_rows
  write_target:
    type: sink
    sink_type: custom
    sink_name: backed_sink
    module_path: "py_functions/custom_sink.py"
    custom_sink_class: "MyCustomSink"
    options:
      output_path: "/tmp/custom_sink_output"
```

## Key rules

- Class implements the `DataSource` interface with a `name()` classmethod and a `writer()` returning a `DataSink`; the `DataSink` implements `write(iterator)`.
- Local helper imports follow the standard rules (`LHP-VAL-023/024/025`, `LHP-IO-003`).

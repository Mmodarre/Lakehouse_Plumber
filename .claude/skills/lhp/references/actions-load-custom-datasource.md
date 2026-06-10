# Load — custom_datasource (PySpark)

`type: load` with `source.type: custom_datasource`. Reads through a user-supplied PySpark `DataSource` class — use when you need Spark-managed streaming offsets. Handler: `CustomDataSourceLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `module_path` | string | required | Path to the `.py` DataSource module. |
| `custom_datasource_class` | string | required | Class name. |
| `options` | dict | `{}` | Passed to the reader. |
| `readMode` | string | `stream` | Read mode. |

## Minimal YAML

```yaml
- name: load_custom
  type: load
  source:
    type: custom_datasource
    module_path: "sources/my_source.py"
    custom_datasource_class: MyDataSource
    options:
      endpoint: "https://api.example.com"
  target: v_custom
```

## Key rules

- Class must implement the `DataSource` interface; its `name()` classmethod return value is used for `.format()`, not the class name. Streaming sources implement `DataSourceStreamReader` (`initialOffset`, `latestOffset`, `partitions`, `read`).
- Source is auto-copied to the generated pipeline; import management handled. Supports substitution variables (`${token}`, `${secret:scope/key}`).
- Local helper imports follow the same rules as `type: python` (`LHP-VAL-023/024/025`, `LHP-IO-003`).

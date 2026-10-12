# Publish test results

Test actions can feed a custom reporting provider. This is separate from defining
expectations: read the relevant test-action reference first. Use `on_violation:
warn` on reported tests; a failing flow may stop before its metrics are available.

In `lhp.yaml`, configure a project-relative Python module:

```yaml
test_reporting:
  module_path: providers/audit_delta.py
  function_name: publish_results
  config_file: providers/audit_config.yaml
```

The optional config file contains a mapping, for example:

```yaml
audit_table: "${catalog}.${bronze_schema}.test_audit_log"
```

The provider contract is `publish_results(results, config, context, spark)`. Each
result includes `test_id`, `status`, and record counts; context includes pipeline
identity, update identity and terminal state. Return published/failed counts.
For example, this provider appends a small audit record for each test:

```python
def publish_results(results, config, context, spark):
    if not results:
        return {"published": 0, "failed": 0}
    rows = [{
        "test_id": str(result["test_id"]),
        "status": str(result["status"]),
        "update_id": str(context.get("update_id", "")),
    } for result in results]
    (spark.createDataFrame(rows).write.format("delta")
     .mode("append").saveAsTable(config["audit_table"]))
    return {"published": len(rows), "failed": 0}
```

Assign a stable `test_id` to each reported test, then validate and generate with
`--include-tests` (and `-pc config/pipeline_config.yaml` for a bundle project).
Inspect the generated event hook and provider files. Publishing happens during
pipeline execution, not local validation or generation. An append-only example
does not guarantee idempotence across retries; use update/test identity to dedupe
if the destination requires it. Confirm destination access before executing.

The provider can write Delta, call an API or publish elsewhere, but external systems
need their own connectivity, credentials and provider implementation. Do not
invent provider options for a service merely because it accepts HTTP requests.

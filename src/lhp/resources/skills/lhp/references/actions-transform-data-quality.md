# Transform — data_quality (expectations)

`type: transform` with `transform_type: data_quality`. Applies expectation rules. Fields are flat on the action. Handler: `DataQualityTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `expectations_file` | string | required | YAML rules file. |
| `mode` | string | `dqe` | `dqe` or `quarantine` (`DQMode`). |
| `quarantine` | object | — | Required when `mode: quarantine`; fields `dlq_table` and `source_table` (both required). |
| `readMode` | string | inherited (`dqe`) / `stream` (`quarantine`) | `stream` or `batch` in `dqe` mode; `quarantine` accepts only `stream` (`LHP-VAL-068` otherwise). With no `readMode`, a `dqe` action reads the way its in-flowgroup source view was made (see Key rules). |

## Minimal YAML

```yaml
- name: dq_orders
  type: transform
  transform_type: data_quality
  source: v_orders
  expectations_file: "expectations/orders.yaml"
  mode: dqe
  target: v_orders_validated
```

## Key rules

- `expectations_file` is a **YAML** rules file. Each rule has a per-rule `action`: `warn` (default), `drop`, or `fail` (list-format files use `failureAction` or `type` instead; see [errors.md](errors.md) VAL-006).
- `mode: dqe` applies expectations inline; `mode: quarantine` routes violations to a DLQ — requires a `quarantine` block with `dlq_table` and `source_table`.
- **Quarantine coerces every expectation to `drop`** regardless of each rule's configured `action`; LHP emits a validation warning for any `warn`/`fail` rule so the coercion is visible. Don't rely on per-rule `warn`/`fail` in quarantine mode.
- Both user-managed tables must exist before runtime. The supported inbox contract
  requires CDF and row tracking; the outbox requires row tracking. Both tables need
  deletion vectors (and are clustered by `_dlq_source_table`) when the DLQ is shared. See
  [quarantine.md](quarantine.md) for DDL, correction, recycling and full-refresh conditions.
- The DLQ outbox table is derived as `<dlq_table>_outbox`. **Recycling** quarantined-then-fixed rows requires Change Data Feed on the DLQ table — set `delta.enableChangeDataFeed = 'true'` on it yourself (LHP does not set it).
- **Read-mode inheritance (`dqe`, no `readMode`):** the action reads its source view's kind when a load/transform in the same flowgroup produces it. Producer kinds: `delta` load batch by default, `jdbc` batch, `cloudfiles`/`kafka` stream, `sql` stream iff the SQL reads `STREAM(...)`, `schema`/`python`/`data_quality` transforms take the kind of their own read. An external source, or one of unknown kind (a `python` load, a `temp_table`), defaults to `stream` — set `readMode: batch` on the DQ for a batch `python` loader. An explicit `readMode` wins.
- **Read-mode lineage (`LHP-VAL-070`):** every in-flowgroup read of a view must match the view's kind: `readStream`/`STREAM(v)` of a batch view fails, a batch read (incl. SQL without `STREAM()`) of a streaming view fails, and a standard streaming-table append flow with `readMode: batch` over a batch view fails unless `once: true`. The error names the `readMode` fix. Test actions always read with batch SQL — point them at tables or batch views.
- **Fusion onto a materialized view:** a batch `dqe` DQ whose only reader is a pass-through MV is fused into it (the MV carries the rules and operational-metadata columns and reads the DQ's source view). See [actions-write-materialized-view.md](actions-write-materialized-view.md).
- Rules are validated at `lhp validate`, inline `expectations` included: no constraint `VAL-001`, unknown action/type `VAL-006`, non-text field or non-mapping rule `VAL-008`. `expectations_file`/`quarantine`/`mode` on a non-`data_quality` action is `LHP-CFG-073`. `lhp validate` shows only the wrapper `LHP-VAL-007` for most of these; run `lhp validate --show-details` for the specific code.

# Quarantine and recycling

A streaming `data_quality` transform with `mode: quarantine` emits a clean path,
a rejected-row sink, a recycle sink and a final union of clean and revalidated
recycled rows. Read [the action fields](actions-transform-data-quality.md).

```yaml
# Action fragment inside a flowgroup; source and downstream write are required.
- name: quarantine_orders
  type: transform
  transform_type: data_quality
  source: v_orders
  target: v_orders_validated
  readMode: stream
  expectations_file: expectations/orders.yaml
  mode: quarantine
  quarantine:
    dlq_table: "${catalog}.${bronze_schema}.orders_dlq"
    source_table: "${catalog}.${bronze_schema}.orders"
```

`source_table` identifies the logical source/output in the ledger and filters
recycling; use consistent fully qualified names. The outbox is always named
`<dlq_table>_outbox`. All expectations are coerced to drop for the clean path;
warn/fail rules produce a validation warning. At least one rule is required.

## External table prerequisites

Create both tables before runtime; LHP generates consumers, not their DDL. Replace
the example identifiers with the configured fully qualified names. The supported
contract uses CDF and row tracking on the inbox and row tracking on the outbox.

```sql
CREATE TABLE IF NOT EXISTS example_catalog.ops.orders_dlq (
  _dlq_sk STRING NOT NULL,
  _dlq_source_table STRING NOT NULL,
  _dlq_status STRING NOT NULL,
  _dlq_timestamp TIMESTAMP NOT NULL,
  _dlq_failed_rules ARRAY<STRUCT<name: STRING, rule: STRING>>,
  _dlq_rescued_data STRING,
  _row_data VARIANT NOT NULL
) TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableRowTracking' = 'true'
);
CREATE TABLE IF NOT EXISTS example_catalog.ops.orders_dlq_outbox (
  _dlq_sk STRING NOT NULL,
  _dlq_source_table STRING NOT NULL,
  _row_data VARIANT NOT NULL,
  _dlq_recycled_at TIMESTAMP NOT NULL
) TBLPROPERTIES ('delta.enableRowTracking' = 'true');
```

Use triggered, non-continuous pipeline execution. Required runtime functions
include VARIANT reconstruction and foreach-batch sinks. Verify runtime support,
permissions and source availability before execution.

## Correction and replay

The inbox MERGE uses `_dlq_sk` (derived from source identity and row JSON) and inserts
only unmatched rows. An operator corrects `_row_data` and marks `_dlq_status` as
`fixed`; LHP does not automatically repair business data. The CDF reader selects
fixed inserts/update postimages, keeps the latest commit per key and insert-merges
it into the outbox. The outbox is re-read, reconstructed to the current schema and
checked again before joining the clean output. Recycling can require a subsequent
triggered update; inspect actual results rather than assuming immediate arrival.

Because both ledgers are external and insert-deduplicated, retained corrected
records can be replayed when pipeline state is rebuilt after a full refresh.
Preserve the inbox/outbox and replayable source history for this behaviour.
Dropping the external tables, changing row identity or changing source contents
can change recovery outcomes. An outbox row already inserted for a key is not
updated by another edit to that same inbox key. This design is not an unconditional
exactly-once guarantee or automatic repair of arbitrary source/rule changes.

Generation verifies the declared structure and shows this mechanism in Python;
a real full-refresh recovery claim requires a runtime exercise with actual data.

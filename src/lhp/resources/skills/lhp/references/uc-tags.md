# Unity Catalog tags

For a table-creating `streaming_table` or `materialized_view` write, set
`write_target.tags` for table tags or `write_target.tags_file` for a shared
schema/tag file containing table and column tags. Temporary tables and sinks are
excluded. Key-only values may be empty strings or null.

```yaml
# Write-target fragment
write_target:
  type: streaming_table
  catalog: "${catalog}"
  schema: "${silver_schema}"
  table: orders
  tags:
    team: platform
    classification: internal
```

LHP emits `_uc_tagging_hook.py` per pipeline, using the Entity Tag Assignments API
when tables exist during runtime. Generation does not apply tags. The optional
`lhp.yaml` `uc_tagging` block defaults to enabled and additive; setting
`remove_undeclared_tags: true` reconciles managed entities to their declared keys,
including deletion of undeclared tags. Use that setting deliberately.

`tag_update_concurrency` defaults to 16 (range 1–20).
`max_allowable_consecutive_failures` defaults to null (no failure limit) and may be
an integer >= 0. The hook applies supported table/column assignments and reports
failures as event-log warnings; a successful pipeline update does not prove tags
were written. Runtime identity needs the relevant tag and catalog/schema grants.

Use the installed schema and actual target file for combined schema/tag formats;
do not guess column nesting. For a first example the inline table mapping above
is sufficient. Keep runtime verification separate from local validation.

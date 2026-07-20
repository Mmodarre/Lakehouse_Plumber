// ── write / materialized_view — @dp.materialized_view ────────
//
// Discriminator: `write_target.type` = 'materialized_view' (the sub-type this
// spec is keyed on). A materialized view needs a target table plus a query
// source — action-level `source`, `write_target.sql`, or `write_target.sql_path`
// — collapsed into a single 3-way `oneOfToggle`. Switching branches PRUNES the
// other two keys (OneOfToggle → applyDiscriminatorChange), so the file only ever
// holds one query source; the requiredOneOf/mutuallyExclusive rules re-surface on
// the toggle (computeIssues toggle-ownership, Task 4.2b).
//
// The validator only enforces requiredOneOf (at least one) — supplying more than
// one is accepted with generator precedence sql > sql_path > source
// (generators/write/materialized_view.py:89-96,110-112). The toggle keeps the
// file to exactly one; the mutuallyExclusive rule is a soft guard for a legacy
// file that already holds several. `create_table` is intentionally OMITTED —
// the MV generator never reads it (accepted-but-ignored no-op). Table options
// mirror streaming_table (via the shared DltTableOptionsValidator), plus refresh_*.
//
// Field provenance:
//   write_target.catalog      required   validators/action/write.py:118-119
//   write_target.schema       required   validators/action/write.py:120-121
//   write_target.table        required   validators/action/write.py:122-123
//   source ⊕ sql ⊕ sql_path   requiredOneOf  validators/action/write.py:157-164 (source is action-level)
//   source (string|list)                 validators/action/write.py:165-168
//   write_target.sql                     generators/write/materialized_view.py:90-91
//   write_target.sql_path                generators/write/materialized_view.py:92-96
//   write_target.comment                 generators/write/materialized_view.py:134
//   write_target.table_schema            generators/write/materialized_view.py:60; validators/compatibility/dlt_table_options.py:94
//   write_target.row_filter              generators/write/materialized_view.py:81; dlt_table_options.py:101
//   write_target.temporary               generators/write/materialized_view.py:82; dlt_table_options.py:106
//   write_target.refresh_schedule        generators/write/materialized_view.py:83,135
//   write_target.refresh_policy          generators/write/materialized_view.py:136; dlt_table_options.py:153-158 (auto|incremental|incremental_strict|full; no default)
//   write_target.partition_columns       generators/write/materialized_view.py:130; dlt_table_options.py:116
//   write_target.cluster_columns         generators/write/materialized_view.py:131; dlt_table_options.py:127 (XOR cluster_by_auto)
//   write_target.cluster_by_auto         generators/write/materialized_view.py:132; dlt_table_options.py:138,143-145
//   write_target.path                    generators/write/materialized_view.py:133
//   write_target.table_properties        generators/write/materialized_view.py:58; dlt_table_options.py:42
//   write_target.tags                    validators/compatibility/dlt_table_options.py:79; references/actions-write-materialized-view.md:11
//   write_target.tags_file               validators/compatibility/dlt_table_options.py:82-88 (unified schemas/ file; XOR tags)
//   write_target.spark_conf              generators/write/materialized_view.py:59; dlt_table_options.py:26

import type { ActionSubTypeSpec } from './types'
import { schemaStub, schemaSuggestPath } from './schemaFile'

const WT = ['write_target'] as const

export const writeMaterializedViewSpec: ActionSubTypeSpec = {
  kind: 'write',
  subType: 'materialized_view',
  title: 'Materialized view',
  summary: 'Recomputed table from a query — a source view, inline SQL, or a SQL file.',
  groups: [
    {
      title: 'Target table',
      fields: [
        {
          path: [...WT, 'catalog'],
          label: 'Catalog',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}',
        },
        {
          path: [...WT, 'schema'],
          label: 'Schema',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${gold_schema}',
        },
        {
          path: [...WT, 'table'],
          label: 'Table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'customer_summary',
        },
      ],
    },
    {
      title: 'Query source',
      description: 'Set exactly one of source view / inline SQL / SQL file.',
      fields: [
        {
          // Synthetic path — the branches own the real source / sql / sql_path
          // keys (source is action-level; sql/sql_path live under write_target).
          // Switching prunes the other two; the field's own path is unused.
          path: ['__query_source'],
          label: 'Query source',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'source',
                label: 'Source view(s)',
                path: ['source'],
                backing: 'text',
                placeholder: 'v_customer_orders',
              },
              {
                value: 'sql',
                label: 'Inline SQL',
                path: [...WT, 'sql'],
                backing: 'inline',
                language: 'sql',
                placeholder:
                  'SELECT customer_id, COUNT(*) AS orders FROM v_orders GROUP BY customer_id',
              },
              {
                value: 'sql_path',
                label: 'SQL file',
                path: [...WT, 'sql_path'],
                backing: 'file',
                accept: ['.sql'],
                placeholder: 'sql/customer_summary.sql',
              },
            ],
          },
        },
      ],
    },
    {
      title: 'Table options',
      advanced: true,
      fields: [
        { path: [...WT, 'comment'], label: 'Comment', widget: 'text' },
        {
          path: [...WT, 'table_schema'],
          label: 'Table schema',
          widget: 'textarea',
          monospace: true,
        },
        { path: [...WT, 'row_filter'], label: 'Row filter', widget: 'text', monospace: true },
        {
          path: [...WT, 'temporary'],
          label: 'Temporary',
          widget: 'bool',
          defaultValue: false,
        },
        {
          path: [...WT, 'refresh_schedule'],
          label: 'Refresh schedule',
          widget: 'text',
          monospace: true,
        },
        {
          path: [...WT, 'refresh_policy'],
          label: 'Refresh policy',
          widget: 'enum',
          options: ['auto', 'incremental', 'incremental_strict', 'full'],
          unsetLabel: 'Not set (default)',
        },
        {
          path: [...WT, 'partition_columns'],
          label: 'Partition columns',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: [...WT, 'cluster_columns'],
          label: 'Cluster columns',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: [...WT, 'cluster_by_auto'],
          label: 'Auto clustering',
          widget: 'bool',
          defaultValue: false,
        },
        { path: [...WT, 'path'], label: 'Table path', widget: 'text', monospace: true },
        {
          path: [...WT, 'table_properties'],
          label: 'Table properties',
          widget: 'keyValue',
        },
        {
          path: [...WT, 'tags'],
          label: 'Tags',
          widget: 'keyValue',
        },
        {
          // Points at the unified schemas/ file (strict table/name + optional
          // tags/columns YAML; the single source of column-level tags). Mutually
          // exclusive with inline `tags` (dlt_table_options.py:82-88 → LHP-CFG);
          // the soft mutuallyExclusive rule below surfaces a both-set hint. The
          // New affordance proposes schemas/<table>.yaml and seeds a skeleton.
          path: [...WT, 'tags_file'],
          label: 'Tags file',
          widget: 'text',
          monospace: true,
          fileRef: { accept: ['.yaml', '.yml'], stub: schemaStub, suggestPath: schemaSuggestPath },
          placeholder: 'schemas/customer_summary.yaml',
        },
        { path: [...WT, 'spark_conf'], label: 'Spark conf', widget: 'keyValue' },
      ],
    },
  ],
  rules: [
    {
      kind: 'requiredOneOf',
      paths: [['source'], [...WT, 'sql'], [...WT, 'sql_path']],
      message: 'A materialized view needs a source view, inline SQL, or a SQL file.',
    },
    {
      kind: 'mutuallyExclusive',
      paths: [['source'], [...WT, 'sql'], [...WT, 'sql_path']],
      message: 'Set only one query source (source / inline SQL / SQL file).',
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        [...WT, 'cluster_columns'],
        [...WT, 'cluster_by_auto'],
      ],
      message: 'Set only one of cluster columns / auto clustering.',
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        [...WT, 'tags'],
        [...WT, 'tags_file'],
      ],
      message: 'Set inline tags or a tags file, not both.',
    },
  ],
}

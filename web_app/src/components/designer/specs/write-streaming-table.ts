// ── write / streaming_table — 3 modes (standard | cdc | snapshot_cdc) ─
//
// The discriminator is `write_target.mode` (default 'standard',
// generators/write/streaming_table.py:56). It switches which field GROUPS
// render. The three modes are asymmetric on purpose:
//   • cdc          → `cdc_config` with `scd_type` (1|2) + column_list/except_column_list
//   • snapshot_cdc → `snapshot_cdc_config` with `stored_as_scd_type` + source ⊕ source_function
// The bundled JSON schema is stale for cdc_config — fields below come from
// the validators + generator, not the schema.
//
// Field provenance:
//   write_target.mode                    generators/write/streaming_table.py:56; ...-standard.md:10
//   write_target.catalog     required    validators/action/write.py:118-119
//   write_target.schema      required    validators/action/write.py:120-121
//   write_target.table       required    validators/action/write.py:122-123
//   source (non-snapshot)    required    validators/action/write.py:130-134 (cdc: single string :141-149)
//   write_target.create_table            generators/write/streaming_table.py:67 (forced true in snapshot_cdc :64-65)
//   write_target.temporary               generators/write/streaming_table.py:101
//   write_target.comment                 generators/write/streaming_table.py:247
//   write_target.table_schema            generators/write/streaming_table.py:79
//   write_target.row_filter              generators/write/streaming_table.py:100
//   write_target.partition_columns       generators/write/streaming_table.py:244
//   write_target.cluster_columns         generators/write/streaming_table.py:245 (XOR cluster_by_auto)
//   write_target.cluster_by_auto         generators/write/streaming_table.py:246; ...-standard.md:24
//   write_target.path                    generators/write/streaming_table.py:248
//   write_target.table_properties        generators/write/streaming_table.py:76
//   write_target.tags                    references/actions-write-streaming-table-standard.md:18
//   write_target.spark_conf              generators/write/streaming_table.py:78
//   cdc_config.keys          required    validators/compatibility/cdc_config.py:41-43
//   cdc_config.sequence_by               validators/compatibility/cdc_config.py:60
//   cdc_config.scd_type                  validators/compatibility/_cdc_helpers.py:13-16 (1|2)
//   cdc_config.ignore_null_updates       validators/compatibility/_cdc_helpers.py:20
//   cdc_config.apply_as_deletes          validators/compatibility/_cdc_helpers.py:25
//   cdc_config.apply_as_truncates        validators/compatibility/_cdc_helpers.py:28 (not w/ scd 2 :33)
//   cdc_config.column_list               validators/compatibility/_cdc_helpers.py:74 (XOR except)
//   cdc_config.except_column_list        validators/compatibility/_cdc_helpers.py:75
//   cdc_config.track_history_column_list          _cdc_helpers.py:38 (XOR except)
//   cdc_config.track_history_except_column_list    _cdc_helpers.py:39
//   snapshot_cdc_config.source           validators/compatibility/snapshot_cdc.py:40 (XOR source_function)
//   snapshot_cdc_config.source_function.file      snapshot_cdc.py:57
//   snapshot_cdc_config.source_function.function  snapshot_cdc.py:59
//   snapshot_cdc_config.source_function.parameters snapshot_cdc.py:62
//   snapshot_cdc_config.keys      required        snapshot_cdc.py:76-78
//   snapshot_cdc_config.stored_as_scd_type        snapshot_cdc.py:95-98 (1|2)
//   snapshot_cdc_config.track_history_column_list  snapshot_cdc.py:107 (XOR except)
//   snapshot_cdc_config.track_history_except_column_list snapshot_cdc.py:108

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, isPresent, readPath } from './helpers'

const MODE: readonly [string, string, string] = ['standard', 'cdc', 'snapshot_cdc']

const mode = (raw: Record<string, unknown>): string =>
  effectiveValue(raw, ['write_target', 'mode'], 'standard')

const CDC = ['write_target', 'cdc_config'] as const
const SNAP = ['write_target', 'snapshot_cdc_config'] as const

export const writeStreamingTableSpec: ActionSubTypeSpec = {
  kind: 'write',
  subType: 'streaming_table',
  title: 'Streaming table',
  summary: 'Append, CDC (SCD 1/2), or full-snapshot CDC into a streaming table.',
  groups: [
    {
      title: 'Mode',
      fields: [
        {
          path: ['write_target', 'mode'],
          label: 'Write mode',
          widget: 'enum',
          options: MODE,
          enumDefault: 'standard',
          help: 'standard appends; cdc applies SCD 1/2; snapshot_cdc uses full snapshots.',
        },
      ],
    },
    {
      title: 'Target table',
      fields: [
        {
          path: ['write_target', 'catalog'],
          label: 'Catalog',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}',
        },
        {
          path: ['write_target', 'schema'],
          label: 'Schema',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${silver_schema}',
        },
        {
          path: ['write_target', 'table'],
          label: 'Table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'customer_dim',
        },
      ],
    },
    {
      title: 'Source',
      visibleWhen: (raw) => mode(raw) !== 'snapshot_cdc',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_customer_bronze',
          help: 'View(s) to read. CDC mode supports a single source view.',
        },
      ],
    },
    {
      title: 'CDC configuration',
      visibleWhen: (raw) => mode(raw) === 'cdc',
      fields: [
        {
          path: [...CDC, 'keys'],
          label: 'Keys',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
          help: 'Natural business keys (non-empty).',
        },
        {
          path: [...CDC, 'sequence_by'],
          label: 'Sequence by',
          widget: 'stringOrList',
          monospace: true,
          help: 'Ordering column(s) that decide the latest version.',
        },
        {
          path: [...CDC, 'scd_type'],
          label: 'SCD type',
          widget: 'enum',
          options: ['1', '2'],
          valueType: 'integer',
          unsetLabel: 'Not set',
          help: '1 overwrites; 2 tracks history.',
        },
        {
          path: [...CDC, 'ignore_null_updates'],
          label: 'Ignore null updates',
          widget: 'bool',
          defaultValue: false,
        },
        {
          path: [...CDC, 'apply_as_deletes'],
          label: 'Apply as deletes',
          widget: 'text',
          monospace: true,
          help: 'Boolean expression identifying delete rows.',
        },
        {
          path: [...CDC, 'apply_as_truncates'],
          label: 'Apply as truncates',
          widget: 'text',
          monospace: true,
          help: 'Expression for truncates. Not supported with SCD Type 2.',
        },
        {
          path: [...CDC, 'column_list'],
          label: 'Column list',
          widget: 'stringList',
          monospace: true,
          help: 'Columns to include. Exclusive with except column list.',
        },
        {
          path: [...CDC, 'except_column_list'],
          label: 'Except column list',
          widget: 'stringList',
          monospace: true,
          help: 'Columns to exclude. Exclusive with column list.',
        },
        {
          path: [...CDC, 'track_history_column_list'],
          label: 'Track history — columns',
          widget: 'stringList',
          monospace: true,
          help: 'SCD2 history columns. Exclusive with the except variant.',
        },
        {
          path: [...CDC, 'track_history_except_column_list'],
          label: 'Track history — except columns',
          widget: 'stringList',
          monospace: true,
          help: 'SCD2 history exclusions. Exclusive with the columns variant.',
        },
      ],
    },
    {
      title: 'Snapshot CDC configuration',
      description: 'Set exactly one of source / source function.',
      visibleWhen: (raw) => mode(raw) === 'snapshot_cdc',
      fields: [
        {
          path: [...SNAP, 'source'],
          label: 'Source',
          widget: 'text',
          monospace: true,
          placeholder: '${catalog}.${bronze_schema}.customer_snapshot',
          help: 'External table/path holding the current snapshot.',
        },
        {
          path: [...SNAP, 'source_function', 'file'],
          label: 'Source function — file',
          widget: 'text',
          monospace: true,
          placeholder: 'snapshots/customer.py',
          help: 'Python file (relative to project root) providing the snapshot function.',
        },
        {
          path: [...SNAP, 'source_function', 'function'],
          label: 'Source function — name',
          widget: 'text',
          monospace: true,
          placeholder: 'next_snapshot',
        },
        {
          path: [...SNAP, 'source_function', 'parameters'],
          label: 'Source function — parameters',
          widget: 'keyValue',
          help: 'Bound as keyword arguments via functools.partial.',
        },
        {
          path: [...SNAP, 'keys'],
          label: 'Keys',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
        },
        {
          path: [...SNAP, 'stored_as_scd_type'],
          label: 'Stored as SCD type',
          widget: 'enum',
          options: ['1', '2'],
          valueType: 'integer',
          unsetLabel: 'Not set',
        },
        {
          path: [...SNAP, 'track_history_column_list'],
          label: 'Track history — columns',
          widget: 'stringList',
          monospace: true,
          help: 'Exclusive with the except variant.',
        },
        {
          path: [...SNAP, 'track_history_except_column_list'],
          label: 'Track history — except columns',
          widget: 'stringList',
          monospace: true,
          help: 'Exclusive with the columns variant.',
        },
      ],
    },
    {
      title: 'Table options',
      fields: [
        {
          path: ['write_target', 'create_table'],
          label: 'Create table',
          widget: 'bool',
          defaultValue: true,
          help: 'Forced true in snapshot_cdc mode.',
        },
        {
          path: ['write_target', 'temporary'],
          label: 'Temporary',
          widget: 'bool',
          defaultValue: false,
        },
        { path: ['write_target', 'comment'], label: 'Comment', widget: 'text' },
        {
          path: ['write_target', 'table_schema'],
          label: 'Table schema',
          widget: 'textarea',
          monospace: true,
          help: 'Inline DDL or a schema-file path.',
        },
        { path: ['write_target', 'row_filter'], label: 'Row filter', widget: 'text', monospace: true },
        {
          path: ['write_target', 'partition_columns'],
          label: 'Partition columns',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: ['write_target', 'cluster_columns'],
          label: 'Cluster columns',
          widget: 'stringList',
          monospace: true,
          help: 'Liquid clustering columns. Exclusive with auto clustering.',
        },
        {
          path: ['write_target', 'cluster_by_auto'],
          label: 'Auto clustering',
          widget: 'bool',
          defaultValue: false,
          help: 'Auto liquid clustering. Exclusive with cluster columns.',
        },
        { path: ['write_target', 'path'], label: 'Table path', widget: 'text', monospace: true },
        {
          path: ['write_target', 'table_properties'],
          label: 'Table properties',
          widget: 'keyValue',
          help: 'delta.* and other table properties.',
        },
        {
          path: ['write_target', 'tags'],
          label: 'Tags',
          widget: 'keyValue',
          help: 'UC tags; applied during the run via the tagging hook.',
        },
        { path: ['write_target', 'spark_conf'], label: 'Spark conf', widget: 'keyValue' },
      ],
    },
  ],
  rules: [
    {
      kind: 'mutuallyExclusive',
      paths: [
        ['write_target', 'cluster_columns'],
        ['write_target', 'cluster_by_auto'],
      ],
      message: 'Set only one of cluster columns / auto clustering.',
    },
    // cdc mode -----------------------------------------------------------
    {
      kind: 'mutuallyExclusive',
      paths: [
        [...CDC, 'column_list'],
        [...CDC, 'except_column_list'],
      ],
      message: 'Set only one of column list / except column list.',
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        [...CDC, 'track_history_column_list'],
        [...CDC, 'track_history_except_column_list'],
      ],
      message: 'Set only one track-history variant.',
    },
    {
      kind: 'custom',
      paths: [[...CDC, 'apply_as_truncates']],
      check: (raw) =>
        isPresent(readPath(raw, [...CDC, 'apply_as_truncates'])) &&
        readPath(raw, [...CDC, 'scd_type']) === 2
          ? 'apply_as_truncates is not supported with SCD Type 2.'
          : null,
    },
    {
      kind: 'custom',
      paths: [['source']],
      check: (raw) =>
        mode(raw) === 'cdc' && Array.isArray(raw.source) && raw.source.length > 1
          ? 'CDC mode supports a single source view.'
          : null,
    },
    // snapshot_cdc mode --------------------------------------------------
    {
      kind: 'xor',
      paths: [
        [...SNAP, 'source'],
        [...SNAP, 'source_function'],
      ],
      message: 'Set exactly one of source / source function.',
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        [...SNAP, 'track_history_column_list'],
        [...SNAP, 'track_history_except_column_list'],
      ],
      message: 'Set only one track-history variant.',
    },
  ],
}

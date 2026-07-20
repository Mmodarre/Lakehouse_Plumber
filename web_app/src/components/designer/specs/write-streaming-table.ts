// ── write / streaming_table — 3 modes (standard | cdc | snapshot_cdc) ─
//
// The discriminator is `write_target.mode` (default 'standard',
// generators/write/streaming_table.py:56-58). It switches which field GROUPS
// render AND, via `branchPaths` (Task 0.5), PRUNES the now-inactive mode's
// exclusive keys on switch — so the create/edit flow can never leave a stale
// cdc_config under mode=standard (the compatibility validators reject that).
// The three modes are asymmetric on purpose:
//   • standard     → append; action-level `source` (shared with cdc)
//   • cdc          → `cdc_config` with `scd_type` (1|2) + column_list/except
//   • snapshot_cdc → `snapshot_cdc_config` with `stored_as_scd_type` +
//                    source ⊕ source_function; NO action-level `source`
// The bundled JSON schema is STALE for cdc_config (flowgroup.schema.json:697-763:
// mode enum omits 'standard'; a spurious cdc `stored_as_scd_type`; scd types
// typed as string) — fields below come from the validators + generator + the
// Jinja template, not the schema.
//
// scd_type coercion (Task 0.2): the scd_type / stored_as_scd_type enums are
// `valueType:'integer'`, so EnumSelect commits the NUMBER 2 (not "2"). A legacy
// hand-edited file may still hold the string "2", so every scd-type predicate
// compares via `String(...) === '2'` to handle both.
//
// Field provenance (validators live under core/validators/):
//   write_target.mode                    generators/write/streaming_table.py:56-58 (standard|cdc|snapshot_cdc, default standard)
//   write_target.catalog     required    core/validators/action/write.py:118-119
//   write_target.schema      required    core/validators/action/write.py:120-121
//   write_target.table       required    core/validators/action/write.py:122-123
//   source (non-snapshot)    required    core/validators/action/write.py:130-134 (cdc: single view)
//   once                                 models/_action.py:107 (Optional[bool]); streaming_table.py:174,259
//   write_target.create_table            streaming_table.py:64-67 (forced true in snapshot_cdc; default true otherwise); models/_action.py:25
//   write_target.temporary               streaming_table.py:101; models/_action.py:35
//   write_target.comment                 streaming_table.py:247; models/_action.py:26
//   write_target.table_schema            streaming_table.py:79-98 (inline DDL OR file path; is_file_path → .yaml/.yml/.json/.sql)
//   write_target.row_filter              streaming_table.py:100; models/_action.py:34
//   write_target.partition_columns       streaming_table.py:244; models/_action.py:29
//   write_target.cluster_columns         streaming_table.py:245 (XOR cluster_by_auto)
//   write_target.cluster_by_auto         streaming_table.py:246; models/_action.py:31
//   write_target.path                    streaming_table.py:248; models/_action.py:36
//   write_target.table_properties        streaming_table.py:76-77; models/_action.py:27
//   write_target.tags                    _field_catalog.py:71 (WriteTarget field; not emitted by the ST template)
//   write_target.tags_file               core/validators/compatibility/dlt_table_options.py:82-88 (external UC tags sidecar; XOR tags)
//   write_target.spark_conf              streaming_table.py:78; models/_action.py:32
//   cdc_config.keys          required    core/validators/compatibility/cdc_config.py:41-52 (list)
//   cdc_config.sequence_by               cdc_config.py:60-76 (string or list)
//   cdc_config.scd_type                  core/validators/compatibility/_cdc_helpers.py:13-16 (int 1|2, default 1)
//   cdc_config.ignore_null_updates       _cdc_helpers.py:18-21 (bool)
//   cdc_config.apply_as_deletes          _cdc_helpers.py:23-26 (SQL expression STRING)
//   cdc_config.apply_as_truncates        _cdc_helpers.py:28-36 (STRING; REJECTED when scd_type==2)
//   cdc_config.column_list               _cdc_helpers.py:82-89 (XOR except_column_list, :74-80)
//   cdc_config.except_column_list        _cdc_helpers.py:91-98
//   cdc_config.track_history_column_list          _cdc_helpers.py:41-66 (XOR except); template :58-59 emits ONLY when scd_type==2 → SCD-2-only
//   cdc_config.track_history_except_column_list    _cdc_helpers.py:41-66
//   snapshot_cdc_config.source           core/validators/compatibility/snapshot_cdc.py:40-50 (XOR source_function)
//   snapshot_cdc_config.source_function.file      snapshot_cdc.py:57-58 (required)
//   snapshot_cdc_config.source_function.function  snapshot_cdc.py:59-60 (required)
//   snapshot_cdc_config.source_function.parameters snapshot_cdc.py:62-67 (dict)
//   snapshot_cdc_config.keys      required        snapshot_cdc.py:76-86 (list)
//   snapshot_cdc_config.stored_as_scd_type        snapshot_cdc.py:95-98 (int 1|2, default 1; DIFFERENT key from cdc's scd_type)
//   snapshot_cdc_config.track_history_column_list  snapshot_cdc.py:102-138 (XOR except); template :121-124 emits regardless of SCD type → NOT gated (asymmetry vs cdc)
//   snapshot_cdc_config.track_history_except_column_list snapshot_cdc.py:102-138

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, isPresent, readPath } from './helpers'
import { ucTagsStub, ucTagsSuggestPath } from './ucTagsFile'

const MODE: readonly [string, string, string] = ['standard', 'cdc', 'snapshot_cdc']

const mode = (raw: Record<string, unknown>): string =>
  effectiveValue(raw, ['write_target', 'mode'], 'standard')

const CDC = ['write_target', 'cdc_config'] as const
const SNAP = ['write_target', 'snapshot_cdc_config'] as const

// EnumSelect(valueType integer) writes scd_type as the number 2; a legacy file
// may hold the string "2". Compare via String() so both read as SCD Type 2.
const isCdcScd2 = (raw: Record<string, unknown>): boolean =>
  String(readPath(raw, [...CDC, 'scd_type'])) === '2'

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
          display: 'segmented',
          options: MODE,
          enumDefault: 'standard',
          // Owned key-blocks per mode (Task 0.5 prune-on-switch). `source` is
          // SHARED by standard+cdc (survives a switch between them); switching to
          // snapshot_cdc prunes action-level `source` + `cdc_config`; switching
          // off cdc prunes `cdc_config`. The discriminator key itself is NEVER
          // listed under a branch, and no branch owns an ancestor of another's.
          branchPaths: {
            standard: [['source']],
            cdc: [['source'], [...CDC]],
            snapshot_cdc: [[...SNAP]],
          },
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
      // Hidden in snapshot_cdc (that mode's source lives in snapshot_cdc_config);
      // the mode branchPaths prune removes the key on switch.
      visibleWhen: (raw) => mode(raw) !== 'snapshot_cdc',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_customer_bronze',
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
        },
        {
          path: [...CDC, 'sequence_by'],
          label: 'Sequence by',
          widget: 'stringOrList',
          monospace: true,
        },
        {
          path: [...CDC, 'scd_type'],
          label: 'SCD type',
          widget: 'enum',
          display: 'segmented',
          options: ['1', '2'],
          valueType: 'integer',
          enumDefault: '1',
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
          placeholder: 'operation = "DELETE"',
        },
        {
          path: [...CDC, 'apply_as_truncates'],
          label: 'Apply as truncates',
          widget: 'text',
          monospace: true,
          placeholder: 'operation = "TRUNCATE"',
          // SCD-1-only: the validator rejects apply_as_truncates with SCD Type 2
          // (_cdc_helpers.py:32-36). Disabled — not hidden — under SCD 2 so the
          // constraint is visible; the "why" hint is the custom rule below.
          disabledWhen: isCdcScd2,
        },
        {
          path: [...CDC, 'column_list'],
          label: 'Column list',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: [...CDC, 'except_column_list'],
          label: 'Except column list',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: [...CDC, 'track_history_column_list'],
          label: 'Track history — columns',
          widget: 'stringList',
          monospace: true,
          // SCD-2-only: the template emits these only when scd_type==2
          // (streaming_table.py.j2:58-59) — silently dropped for SCD 1.
          visibleWhen: isCdcScd2,
        },
        {
          path: [...CDC, 'track_history_except_column_list'],
          label: 'Track history — except columns',
          widget: 'stringList',
          monospace: true,
          visibleWhen: isCdcScd2,
        },
      ],
    },
    {
      title: 'Snapshot CDC configuration',
      description: 'Set exactly one of source / source function.',
      visibleWhen: (raw) => mode(raw) === 'snapshot_cdc',
      fields: [
        {
          // Synthetic path — the branches own the real source / source_function
          // keys. `source` is a plain table ref; `source_function` is an OBJECT
          // (file/function/parameters) rendered via the `'fields'` backing.
          path: [...SNAP, '__source'],
          label: 'Snapshot source',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'source',
                label: 'Source table',
                path: [...SNAP, 'source'],
                backing: 'text',
                placeholder: '${catalog}.${bronze_schema}.customer_snapshot',
              },
              {
                value: 'source_function',
                label: 'Source function',
                path: [...SNAP, 'source_function'],
                backing: 'fields',
                fields: [
                  {
                    path: [...SNAP, 'source_function', 'file'],
                    label: 'Function file',
                    widget: 'text',
                    monospace: true,
                    required: true,
                    fileRef: { accept: ['.py'] },
                    placeholder: 'snapshots/customer.py',
                  },
                  {
                    path: [...SNAP, 'source_function', 'function'],
                    label: 'Function name',
                    widget: 'text',
                    monospace: true,
                    required: true,
                    placeholder: 'next_snapshot',
                  },
                  {
                    path: [...SNAP, 'source_function', 'parameters'],
                    label: 'Parameters',
                    widget: 'keyValue',
                  },
                ],
              },
            ],
          },
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
          label: 'SCD type',
          widget: 'enum',
          display: 'segmented',
          options: ['1', '2'],
          valueType: 'integer',
          enumDefault: '1',
        },
        // Snapshot track-history is valid REGARDLESS of SCD type (asymmetry vs
        // cdc — template :121-124 has no scd-type gate); shown unconditionally.
        {
          path: [...SNAP, 'track_history_column_list'],
          label: 'Track history — columns',
          widget: 'stringList',
          monospace: true,
        },
        {
          path: [...SNAP, 'track_history_except_column_list'],
          label: 'Track history — except columns',
          widget: 'stringList',
          monospace: true,
        },
      ],
    },
    {
      title: 'Table options',
      advanced: true,
      fields: [
        {
          path: ['write_target', 'create_table'],
          label: 'Create table',
          widget: 'bool',
          defaultValue: true,
          // Snapshot CDC always materializes the table (streaming_table.py:64-65
          // forces create_table=True); render checked-and-disabled there. The
          // explanatory hint is the custom rule below.
          disabledWhen: (raw) => mode(raw) === 'snapshot_cdc',
        },
        {
          path: ['once'],
          label: 'Run once (backfill)',
          widget: 'bool',
          defaultValue: false,
        },
        {
          path: ['write_target', 'temporary'],
          label: 'Temporary',
          widget: 'bool',
          defaultValue: false,
        },
        { path: ['write_target', 'comment'], label: 'Comment', widget: 'text' },
        {
          // Synthetic path — both branches own the single `table_schema` key
          // (inline DDL OR a file path, auto-detected by the generator). The
          // toggle is a pure affordance switch; the shared key survives it.
          path: ['write_target', '__table_schema'],
          label: 'Table schema',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'inline',
                label: 'Inline DDL',
                path: ['write_target', 'table_schema'],
                backing: 'inline',
                language: 'sql',
                placeholder: 'customer_id BIGINT, name STRING',
              },
              {
                value: 'file',
                label: 'From file',
                path: ['write_target', 'table_schema'],
                backing: 'file',
                accept: ['.yaml', '.yml', '.json', '.sql'],
                placeholder: 'schemas/customer_dim.yaml',
              },
            ],
          },
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
        },
        {
          path: ['write_target', 'cluster_by_auto'],
          label: 'Auto clustering',
          widget: 'bool',
          defaultValue: false,
        },
        { path: ['write_target', 'path'], label: 'Table path', widget: 'text', monospace: true },
        {
          path: ['write_target', 'table_properties'],
          label: 'Table properties',
          widget: 'keyValue',
        },
        {
          path: ['write_target', 'tags'],
          label: 'Tags',
          widget: 'keyValue',
        },
        {
          // External UC tags sidecar (strict table/name + optional version/tags/columns YAML;
          // the single source of column-level tags). Mutually exclusive with
          // inline `tags` (dlt_table_options.py:82-88 → LHP-CFG); the soft
          // mutuallyExclusive rule below surfaces a both-set hint. The New
          // affordance proposes uc_tags/<table>.yaml and seeds a skeleton.
          path: ['write_target', 'tags_file'],
          label: 'Tags file',
          widget: 'text',
          monospace: true,
          fileRef: { accept: ['.yaml', '.yml'], stub: ucTagsStub, suggestPath: ucTagsSuggestPath },
          placeholder: 'uc_tags/customer_dim.yaml',
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
    {
      kind: 'custom',
      paths: [['write_target', 'create_table']],
      check: (raw) =>
        mode(raw) === 'snapshot_cdc'
          ? 'Snapshot CDC always creates the table — create_table is forced on.'
          : null,
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        ['write_target', 'tags'],
        ['write_target', 'tags_file'],
      ],
      message: 'Set inline tags or a tags file, not both.',
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
        isPresent(readPath(raw, [...CDC, 'apply_as_truncates'])) && isCdcScd2(raw)
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

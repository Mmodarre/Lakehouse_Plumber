// ── write / sink — dp.create_sink (delta | kafka | custom | foreachbatch) ──
//
// Discriminator: `write_target.sink_type` (REQUIRED — no default, explicit empty
// state; never defaults to delta). Rendered as a `segmented` enum carrying
// `branchPaths`, so a switch both re-renders the branch GROUPS and PRUNES the
// prior variant's exclusive keys (Task 0.5), the same pattern as
// streaming_table's `mode`. Two keys are SHARED and survive the relevant switch:
//   • `options`     — delta + kafka (survives delta↔kafka; pruned entering
//                     custom/foreachbatch, which do not use it)
//   • `module_path` — custom + foreachbatch (survives custom↔foreachbatch)
// `sink_name` + `comment` are common to all four and live in the core group, so
// they are never branch-pruned. Event Hubs is NOT a separate sink_type: it is a
// kafka sink whose `options` set `kafka.sasl.mechanism: OAUTHBEARER`
// (generators/write/sinks/kafka_sink.py:36; references/actions-write-sink-eventhubs.md:3).
//
// Field provenance:
//   write_target.sink_type      required   validators/action/_write_sinks.py:18-19 (delta|kafka|custom|foreachbatch)
//   write_target.sink_name      required   validators/action/_write_sinks.py:22-23
//   source                      required   validators/action/_write_sinks.py:25-28 (foreachbatch: single string :122-124)
//   write_target.comment                   generators/write/sinks/delta_sink.py:20-24 (kafka:43-53, custom:162-166, foreachbatch:118-122)
//   -- delta --
//   write_target.options        required   validators/action/_write_sinks.py:51-56 (tableName XOR path :57-68; 3-part tableName :70-74); generators/write/sinks/delta_sink.py:16
//   -- kafka --
//   write_target.bootstrap_servers required validators/action/_write_sinks.py:85-86; generators/write/sinks/kafka_sink.py:20
//   write_target.topic          required   validators/action/_write_sinks.py:88-89; generators/write/sinks/kafka_sink.py:21
//   write_target.options                   generators/write/sinks/kafka_sink.py:29-33 (KafkaOptionsValidator; OAUTHBEARER => Event Hubs :36)
//   -- custom --
//   write_target.module_path        required validators/action/_write_sinks.py:109-110; generators/write/sinks/custom_sink.py:68
//   write_target.custom_sink_class  required validators/action/_write_sinks.py:112-113; generators/write/sinks/custom_sink.py:69
//   -- foreachbatch --
//   write_target.module_path ⊕ batch_handler  requiredOneOf/xor  validators/action/_write_sinks.py:127-137
//   write_target.batch_handler             generators/write/sinks/foreachbatch_sink.py:31,61-62

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, isPresent, isSubstitutionToken, readPath } from './helpers'

const WT = ['write_target'] as const
const SINK_TYPES: readonly string[] = ['delta', 'kafka', 'custom', 'foreachbatch']

const sinkType = (raw: Record<string, unknown>): string => effectiveValue(raw, [...WT, 'sink_type'], '')

export const writeSinkSpec: ActionSubTypeSpec = {
  kind: 'write',
  subType: 'sink',
  title: 'Sink',
  summary: 'Stream a view to an external target: Delta, Kafka / Event Hubs, custom, or foreachBatch.',
  groups: [
    {
      title: 'Sink',
      fields: [
        {
          path: [...WT, 'sink_type'],
          label: 'Sink type',
          widget: 'enum',
          display: 'segmented',
          options: SINK_TYPES,
          required: true,
          // No enumDefault: the control starts with NOTHING selected — the user
          // must pick a sink type. Owned key-blocks per branch (Task 0.5 prune-on-
          // switch). `options` is SHARED by delta+kafka (survives delta↔kafka;
          // pruned when entering custom/foreachbatch); `module_path` is SHARED by
          // custom+foreachbatch (survives that switch). The discriminator key
          // itself is NEVER listed under a branch.
          branchPaths: {
            delta: [[...WT, 'options']],
            kafka: [[...WT, 'bootstrap_servers'], [...WT, 'topic'], [...WT, 'options']],
            custom: [[...WT, 'module_path'], [...WT, 'custom_sink_class']],
            foreachbatch: [[...WT, 'module_path'], [...WT, 'batch_handler']],
          },
        },
        {
          path: [...WT, 'sink_name'],
          label: 'Sink name',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'analytics_delta_export',
        },
        { path: [...WT, 'comment'], label: 'Comment', widget: 'text' },
      ],
    },
    {
      title: 'Source',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_metrics',
        },
      ],
    },
    {
      title: 'Delta sink',
      description:
        'Delta sinks target a table via options.tableName/path (no catalog/schema/table). Set exactly one of tableName / path.',
      visibleWhen: (raw) => sinkType(raw) === 'delta',
      fields: [
        {
          path: [...WT, 'options'],
          label: 'Options',
          widget: 'keyValue',
        },
      ],
    },
    {
      title: 'Kafka sink',
      description:
        'Event Hubs is a Kafka sink — set kafka.sasl.mechanism: OAUTHBEARER in options, not a separate sink type.',
      visibleWhen: (raw) => sinkType(raw) === 'kafka',
      fields: [
        {
          path: [...WT, 'bootstrap_servers'],
          label: 'Bootstrap servers',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${kafka_bootstrap_servers}',
        },
        {
          path: [...WT, 'topic'],
          label: 'Topic',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'acme.orders.fulfillment',
        },
        {
          path: [...WT, 'options'],
          label: 'Options',
          widget: 'keyValue',
        },
      ],
    },
    {
      title: 'Custom sink',
      visibleWhen: (raw) => sinkType(raw) === 'custom',
      fields: [
        {
          path: [...WT, 'module_path'],
          label: 'Module path',
          widget: 'text',
          monospace: true,
          required: true,
          fileRef: { accept: ['.py'] },
          placeholder: 'sinks/my_sink.py',
        },
        {
          path: [...WT, 'custom_sink_class'],
          label: 'Custom sink class',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'MyCustomDataSink',
        },
      ],
    },
    {
      title: 'ForEachBatch sink',
      description: 'Set exactly one of module path / inline batch handler.',
      visibleWhen: (raw) => sinkType(raw) === 'foreachbatch',
      fields: [
        {
          // Synthetic path — the branches own the real module_path / batch_handler
          // keys. module_path is a file ref (.py); batch_handler is an inline
          // Python body. Switching prunes the inactive branch's key.
          path: [...WT, '__handler'],
          label: 'Batch handler',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'module_path',
                label: 'Module path',
                path: [...WT, 'module_path'],
                backing: 'file',
                accept: ['.py'],
                placeholder: 'batch_handlers/my_handler.py',
              },
              {
                value: 'batch_handler',
                label: 'Inline handler',
                path: [...WT, 'batch_handler'],
                backing: 'inline',
                language: 'python',
                placeholder: 'def handle(batch_df, batch_id):\n    ...',
              },
            ],
          },
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'custom',
      paths: [[...WT, 'options']],
      check: (raw) => {
        if (sinkType(raw) !== 'delta') return null
        const hasTable = isPresent(readPath(raw, [...WT, 'options', 'tableName']))
        const hasPath = isPresent(readPath(raw, [...WT, 'options', 'path']))
        if (hasTable && hasPath) return 'Delta options cannot set both tableName and path.'
        if (!hasTable && !hasPath) return 'Delta options need either tableName or path.'
        return null
      },
    },
    {
      kind: 'custom',
      paths: [[...WT, 'options']],
      check: (raw) => {
        if (sinkType(raw) !== 'delta') return null
        const tableName = readPath(raw, [...WT, 'options', 'tableName'])
        if (typeof tableName !== 'string' || !isPresent(tableName)) return null
        if (isSubstitutionToken(tableName)) return null
        return tableName.split('.').length === 3
          ? null
          : 'Delta options.tableName should be a 3-part name (catalog.schema.table).'
      },
    },
    {
      kind: 'custom',
      paths: [
        [...WT, 'module_path'],
        [...WT, 'batch_handler'],
      ],
      check: (raw) => {
        if (sinkType(raw) !== 'foreachbatch') return null
        const hasModule = isPresent(readPath(raw, [...WT, 'module_path']))
        const hasHandler = isPresent(readPath(raw, [...WT, 'batch_handler']))
        return hasModule === hasHandler
          ? 'ForEachBatch needs exactly one of module path / batch handler.'
          : null
      },
    },
    {
      kind: 'custom',
      paths: [['source']],
      check: (raw) =>
        sinkType(raw) === 'foreachbatch' && Array.isArray(raw.source)
          ? 'ForEachBatch supports a single source view (string), not a list.'
          : null,
    },
  ],
}

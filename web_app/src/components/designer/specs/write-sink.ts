// ── write / sink — dp.create_sink (delta | kafka | custom | foreachbatch) ──
//
// Discriminator: `write_target.sink_type` (REQUIRED — no default). It switches
// which field GROUPS render, the same pattern as streaming_table's `mode`.
// Event Hubs is NOT a separate sink_type: it is a kafka sink whose `options`
// set `kafka.sasl.mechanism: OAUTHBEARER` (generators/write/sinks/kafka_sink.py:36;
// references/actions-write-sink-eventhubs.md:3 — "no EH-only YAML keys exist").
//
// Field provenance:
//   write_target.sink_type      required   validators/action/_write_sinks.py:18-19 (delta|kafka|custom|foreachbatch)
//   write_target.sink_name      required   validators/action/_write_sinks.py:22-23
//   source                      required   validators/action/_write_sinks.py:25-28 (foreachbatch: single string :122-125)
//   write_target.comment                   generators/write/sinks/delta_sink.py:20-24 (kafka:43-53, custom:162-166, foreachbatch:118-122)
//   -- delta --
//   write_target.options        required   validators/action/_write_sinks.py:51-56 (tableName XOR path :58-68); generators/write/sinks/delta_sink.py:16
//   -- kafka --
//   write_target.bootstrap_servers required validators/action/_write_sinks.py:85-86; generators/write/sinks/kafka_sink.py:20
//   write_target.topic          required   validators/action/_write_sinks.py:88-89; generators/write/sinks/kafka_sink.py:21
//   write_target.options                   generators/write/sinks/kafka_sink.py:29-33 (KafkaOptionsValidator; OAUTHBEARER => Event Hubs :36)
//   -- custom --
//   write_target.module_path        required validators/action/_write_sinks.py:109-110; generators/write/sinks/custom_sink.py:68
//   write_target.custom_sink_class  required validators/action/_write_sinks.py:112-113; generators/write/sinks/custom_sink.py:69
//   write_target.options                   generators/write/sinks/custom_sink.py:71
//   -- foreachbatch --
//   write_target.module_path ⊕ batch_handler  validators/action/_write_sinks.py:127-137
//   write_target.batch_handler             generators/write/sinks/foreachbatch_sink.py:31,61-62

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, isPresent, readPath } from './helpers'

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
          options: SINK_TYPES,
          required: true,
          help: 'delta / kafka / custom / foreachbatch. Event Hubs = kafka + OAUTHBEARER options.',
        },
        {
          path: [...WT, 'sink_name'],
          label: 'Sink name',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'analytics_delta_export',
          help: 'Unique name for the created sink.',
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
          help: 'View(s) to read. foreachBatch supports a single view only.',
        },
      ],
    },
    {
      title: 'Delta sink',
      description: 'options must include exactly one of tableName / path.',
      visibleWhen: (raw) => sinkType(raw) === 'delta',
      fields: [
        {
          path: [...WT, 'options'],
          label: 'Options',
          widget: 'keyValue',
          help: 'tableName (three-part) or path, plus checkpointLocation, mergeSchema, …',
        },
      ],
    },
    {
      title: 'Kafka sink',
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
          help: 'kafka.* options. Set kafka.sasl.mechanism: OAUTHBEARER for Event Hubs.',
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
          placeholder: 'sinks/my_sink.py',
          help: 'Python file with the custom DataSink class.',
        },
        {
          path: [...WT, 'custom_sink_class'],
          label: 'Custom sink class',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'MyCustomDataSink',
        },
        {
          path: [...WT, 'options'],
          label: 'Options',
          widget: 'keyValue',
          help: 'Passed through to the sink.',
        },
      ],
    },
    {
      title: 'ForEachBatch sink',
      description: 'Set exactly one of module path / inline batch handler.',
      visibleWhen: (raw) => sinkType(raw) === 'foreachbatch',
      fields: [
        {
          path: [...WT, 'module_path'],
          label: 'Module path',
          widget: 'text',
          monospace: true,
          placeholder: 'batch_handlers/my_handler.py',
          help: 'File whose body is the batch handler.',
        },
        {
          path: [...WT, 'batch_handler'],
          label: 'Batch handler',
          widget: 'textarea',
          monospace: true,
          help: 'Inline function body; receives the micro-batch DataFrame as df.',
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

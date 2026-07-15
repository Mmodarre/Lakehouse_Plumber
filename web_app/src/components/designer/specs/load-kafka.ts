// ── load / kafka — stream from Kafka into a temp view ────────
//
// Kafka LOAD uses bootstrap_servers + a subscription method + options; there is
// NO `topic` field on the load side (topic lives on the write/sink). Event Hubs
// / MSK-IAM auth is expressed entirely as `options` entries (OAUTHBEARER etc.).
//   source.bootstrap_servers  required  validators/action/load.py:121-122; generators/load/kafka.py:40-55
//   source.subscribe                    validators/action/load.py:124-140 (exactly one of the three)
//   source.subscribePattern             validators/action/load.py:124-140
//   source.assign                       validators/action/load.py:124-140
//   source.options                      generators/load/kafka.py:77-93 (extra kafka.* options)
//   readMode                            generators/load/kafka.py:31-38 (must be stream)
//   target                    required  validators/action/load.py:17-18
// Rules: exactly one of subscribe / subscribePattern / assign (load.py:133-140;
//        kafka.py:114-154); readMode must be stream (kafka.py:32-38).

import type { ActionSubTypeSpec } from './types'
import { readPath } from './helpers'

export const loadKafkaSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'kafka',
  title: 'Kafka',
  summary: 'Stream from Apache Kafka (or a Kafka-compatible endpoint) into a temporary view.',
  groups: [
    {
      title: 'Connection',
      fields: [
        {
          path: ['source', 'bootstrap_servers'],
          label: 'Bootstrap servers',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'broker1:9092,broker2:9092',
        },
      ],
    },
    {
      title: 'Subscription',
      description: 'Set exactly one of subscribe / subscribe pattern / assign.',
      fields: [
        {
          path: ['source', 'subscribe'],
          label: 'Subscribe',
          widget: 'text',
          monospace: true,
          placeholder: 'orders',
        },
        {
          path: ['source', 'subscribePattern'],
          label: 'Subscribe pattern',
          widget: 'text',
          monospace: true,
          placeholder: 'topic-.*',
        },
        {
          path: ['source', 'assign'],
          label: 'Assign',
          widget: 'text',
          monospace: true,
          placeholder: '{"topic": [0, 1]}',
        },
      ],
    },
    {
      title: 'Read',
      fields: [
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['stream'],
          enumDefault: 'stream',
        },
      ],
    },
    {
      title: 'Advanced',
      advanced: true,
      fields: [
        {
          path: ['source', 'options'],
          label: 'Kafka options',
          widget: 'keyValue',
        },
      ],
    },
    {
      title: 'Target',
      fields: [
        {
          path: ['target'],
          label: 'Target view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_events',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'xor',
      paths: [
        ['source', 'subscribe'],
        ['source', 'subscribePattern'],
        ['source', 'assign'],
      ],
      message: 'Set exactly one of subscribe / subscribe pattern / assign.',
    },
    {
      kind: 'custom',
      paths: [['readMode']],
      check: (raw) => {
        const mode = readPath(raw, ['readMode'])
        return typeof mode === 'string' && mode !== 'stream'
          ? 'Kafka requires readMode: stream (batch is rejected).'
          : null
      },
    },
  ],
}

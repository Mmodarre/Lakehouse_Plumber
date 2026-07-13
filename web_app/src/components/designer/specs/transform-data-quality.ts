// ── transform / data_quality — apply expectation rules ──────
//
// Fields are flat on the action. Expectations live in an EXTERNAL file
// (`expectations_file`) which the generator hard-requires (generators/transform/
// data_quality.py:37-51). Each rule carries its own action (warn/drop/fail)
// INSIDE that file — there is no per-action `on_violation` and no inline list-
// of-objects, so this spec needs NO `objectList` widget (see report note).
//   source            required  validators/action/_dq_transform.py:22-23
//   expectations_file required  generators/transform/data_quality.py:37-51;
//                               references/actions-transform-data-quality.md:9
//   mode                        models/_enums.py:53-57 (DQMode dqe|quarantine);
//                               validators/action/_dq_transform.py:25-30 (default dqe)
//   readMode                    generators/transform/data_quality.py:28-35 (must be stream)
//   quarantine.dlq_table    req-when-quarantine  _dq_transform.py:41-44; models/_quarantine.py:9
//   quarantine.source_table req-when-quarantine  _dq_transform.py:41-44; models/_quarantine.py:10
//   target            required  validators/action/transform.py:25-26

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, readPath } from './helpers'

const mode = (raw: Record<string, unknown>): string => effectiveValue(raw, ['mode'], 'dqe')

export const transformDataQualitySpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'data_quality',
  title: 'Data quality',
  summary: 'Apply expectation rules from an external file (DQE inline, or quarantine to a DLQ).',
  groups: [
    {
      title: 'Expectations',
      fields: [
        {
          path: ['source'],
          label: 'Source view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
        },
        {
          path: ['expectations_file'],
          label: 'Expectations file',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'expectations/orders.yaml',
        },
        {
          path: ['mode'],
          label: 'Mode',
          widget: 'enum',
          options: ['dqe', 'quarantine'],
          enumDefault: 'dqe',
        },
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
      title: 'Quarantine',
      description: 'Required when mode is quarantine.',
      visibleWhen: (raw) => mode(raw) === 'quarantine',
      fields: [
        {
          path: ['quarantine', 'dlq_table'],
          label: 'DLQ table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${schema}.orders_dlq',
        },
        {
          path: ['quarantine', 'source_table'],
          label: 'Source table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${schema}.orders',
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
          placeholder: 'v_orders_validated',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'custom',
      paths: [['readMode']],
      check: (raw) => {
        const rm = readPath(raw, ['readMode'])
        return typeof rm === 'string' && rm !== 'stream'
          ? 'Data quality requires readMode: stream (batch is rejected).'
          : null
      },
    },
  ],
}

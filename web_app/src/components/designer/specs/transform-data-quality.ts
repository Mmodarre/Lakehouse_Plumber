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
//
// `mode` is a `display: 'segmented'` discriminator carrying `branchPaths`
// (Task 0.5): switching OFF quarantine PRUNES the whole `quarantine` subtree —
// the headline bug fix (_dq_transform.py:65-71 rejects a stale quarantine block
// under mode=dqe). `readMode` is NOT an editable field: it must be stream and
// defaults to stream at generate time, so the form leaves it unset and surfaces
// the constraint as an informational group description instead of a pointless
// one-option select (see report — a field-less "Advanced" group cannot render,
// so the note lives on the always-visible "Expectation rules" group). The
// readMode-must-be-stream cross-field RULE is KEPT (Phase-4 preamble: keep every
// existing cross-field rule) — it reads raw `['readMode']` and soft-flags a
// hand-edited `readMode: batch`, independent of there being no editable field.

import type { ActionSubTypeSpec } from './types'
import { effectiveValue, isSubstitutionToken, readPath } from './helpers'

const mode = (raw: Record<string, unknown>): string => effectiveValue(raw, ['mode'], 'dqe')

// A soft, non-blocking mirror of _name_checks.require_three_part_name
// (catalog.schema.table → exactly two dots). Skips substitution tokens: a
// `${catalog}.${schema}.t` resolves at generate time, so it is never flagged.
const notThreePartName = (value: unknown): boolean =>
  typeof value === 'string' &&
  value !== '' &&
  !isSubstitutionToken(value) &&
  value.split('.').length !== 3

export const transformDataQualitySpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'data_quality',
  title: 'Data quality',
  summary: 'Apply expectation rules from an external file (DQE inline, or quarantine to a DLQ).',
  groups: [
    {
      title: 'Source',
      fields: [
        {
          path: ['source'],
          label: 'Source view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
        },
      ],
    },
    {
      title: 'Expectation rules',
      description:
        'Read mode is stream (required) — data quality runs in streaming mode only; batch is rejected. The form leaves readMode unset; it defaults to stream at generate time.',
      fields: [
        {
          path: ['expectations_file'],
          label: 'Expectations file',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'expectations/orders.yaml',
          fileRef: { accept: ['.yaml', '.yml'] },
        },
        {
          path: ['mode'],
          label: 'Mode',
          widget: 'enum',
          display: 'segmented',
          options: ['dqe', 'quarantine'],
          enumDefault: 'dqe',
          branchPaths: { dqe: [], quarantine: [['quarantine']] },
        },
      ],
    },
    {
      title: 'Quarantine',
      description:
        'Required when mode is quarantine. Quarantine coerces every expectation to drop — any fail/warn action in the expectations file is ignored.',
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
    {
      kind: 'custom',
      paths: [['quarantine', 'dlq_table']],
      check: (raw) =>
        notThreePartName(readPath(raw, ['quarantine', 'dlq_table']))
          ? 'DLQ table should be a 3-part name (catalog.schema.table).'
          : null,
    },
    {
      kind: 'custom',
      paths: [['quarantine', 'source_table']],
      check: (raw) =>
        notThreePartName(readPath(raw, ['quarantine', 'source_table']))
          ? 'Source table should be a 3-part name (catalog.schema.table).'
          : null,
    },
  ],
}

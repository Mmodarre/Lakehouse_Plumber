// ── test / uniqueness — no duplicate rows for the given columns ──
//
// Discriminator: `test_type` = 'uniqueness'.
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:37-39; generators/test/uniqueness.py:33-35
//   columns       required  validators/action/_test_requirements.py:40-43; generators/test/uniqueness.py:36
//   filter                  generators/test/uniqueness.py:39; models/_action.py:113 (optional WHERE clause — Advanced)
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                           models/_enums.py:28-32 `ViolationAction` is STALE (no `drop`) + unused — not the source of truth.
//   test_id                 models/_action.py:125 (Optional[str]); validators/field/_field_catalog.py:159.
//                           NOT in flowgroup.schema.json → help-schema-parity deferred to Task 5.1.
//   target                  models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testUniquenessSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'uniqueness',
  title: 'Uniqueness',
  summary: 'Assert no duplicate rows for a set of columns.',
  groups: [
    {
      title: 'Target under test',
      fields: [
        {
          path: ['source'],
          label: 'Source',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.fact_orders',
        },
      ],
    },
    {
      title: 'Test parameters',
      fields: [
        {
          path: ['columns'],
          label: 'Columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'order_id',
        },
      ],
    },
    {
      title: 'Violation handling',
      fields: [
        {
          path: ['on_violation'],
          label: 'On violation',
          widget: 'enum',
          options: ['fail', 'warn', 'drop'],
          enumDefault: 'fail',
        },
      ],
    },
    {
      title: 'Advanced',
      advanced: true,
      fields: [
        {
          path: ['filter'],
          label: 'Filter',
          widget: 'text',
          monospace: true,
          placeholder: 'order_status = "ACTIVE"',
        },
        { path: ['test_id'], label: 'Test ID', widget: 'text', monospace: true },
        {
          path: ['target'],
          label: 'Target',
          widget: 'text',
          monospace: true,
          placeholder: 'tmp_test_<name>',
        },
      ],
    },
  ],
}

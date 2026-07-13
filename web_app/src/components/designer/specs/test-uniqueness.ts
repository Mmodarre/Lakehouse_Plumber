// ── test / uniqueness — no duplicate rows for the given columns ──
//
// Discriminator: `test_type` = 'uniqueness'.
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:37-39; generators/test/uniqueness.py:33-35
//   columns       required  validators/action/_test_requirements.py:40-43; generators/test/uniqueness.py:36
//   filter                  generators/test/uniqueness.py:39; models/_action.py:113 (optional WHERE clause)
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'

export const testUniquenessSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'uniqueness',
  title: 'Uniqueness',
  summary: 'Assert no duplicate rows for a set of columns.',
  groups: [
    {
      title: 'Test',
      fields: [
        {
          path: ['source'],
          label: 'Source',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.fact_orders',
        },
        {
          path: ['columns'],
          label: 'Columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'order_id',
        },
        {
          path: ['filter'],
          label: 'Filter',
          widget: 'text',
          monospace: true,
        },
        {
          path: ['on_violation'],
          label: 'On violation',
          widget: 'enum',
          options: ['fail', 'warn', 'drop'],
          enumDefault: 'fail',
        },
      ],
    },
  ],
}

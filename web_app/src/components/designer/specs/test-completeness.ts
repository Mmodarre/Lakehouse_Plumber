// ── test / completeness — required columns must be non-null ──
//
// Discriminator: `test_type` = 'completeness'.
//
// Field provenance:
//   source            required  validators/action/_test_requirements.py:76-78; generators/test/completeness.py:37-39
//   required_columns  required  validators/action/_test_requirements.py:79-82; generators/test/completeness.py:24,40
//   on_violation                validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'

export const testCompletenessSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'completeness',
  title: 'Completeness',
  summary: 'Required columns must be non-null for every row.',
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
          path: ['required_columns'],
          label: 'Required columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
          help: 'Columns that must be non-null.',
        },
        {
          path: ['on_violation'],
          label: 'On violation',
          widget: 'enum',
          options: ['fail', 'warn', 'drop'],
          enumDefault: 'fail',
          help: 'Action when the check fails (default fail).',
        },
      ],
    },
  ],
}

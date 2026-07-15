// ── test / completeness — required columns must be non-null ──
//
// Discriminator: `test_type` = 'completeness'.
//
// Field provenance:
//   source            required  validators/action/_test_requirements.py:76-78; generators/test/completeness.py:37-39
//   required_columns  required  validators/action/_test_requirements.py:79-82; generators/test/completeness.py:24,40
//   on_violation                validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                               models/_enums.py:28-32 `ViolationAction` is STALE (no `drop`) + unused.
//   test_id                     models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                               → help-schema-parity deferred to Task 5.1.
//   target                      models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testCompletenessSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'completeness',
  title: 'Completeness',
  summary: 'Required columns must be non-null for every row.',
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
          path: ['required_columns'],
          label: 'Required columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
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

// ── test / row_count — compare two tables' row counts ───────
//
// Discriminator: `test_type` = 'row_count'. `source` is a list of EXACTLY two
// tables; the check passes when |count(a) − count(b)| ≤ tolerance.
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:25-35 (list of exactly 2); generators/test/row_count.py:34-36
//   tolerance               generators/test/row_count.py:24 (default 0)
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'

export const testRowCountSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'row_count',
  title: 'Row count',
  summary: 'Compare row counts between two tables within a tolerance.',
  groups: [
    {
      title: 'Test',
      fields: [
        {
          path: ['source'],
          label: 'Sources',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${raw_schema}.customers',
        },
        {
          path: ['tolerance'],
          label: 'Tolerance',
          widget: 'number',
          min: 0,
          valueType: 'integer',
          placeholder: '0',
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
  rules: [
    {
      kind: 'custom',
      paths: [['source']],
      check: (raw) =>
        Array.isArray(raw.source) && raw.source.length !== 2
          ? 'Row count compares exactly two sources.'
          : null,
    },
  ],
}

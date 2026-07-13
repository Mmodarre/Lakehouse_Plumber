// ── test / range — a column's values fall within bounds ─────
//
// Discriminator: `test_type` = 'range'. At least one of min_value / max_value
// is required. Both are `Any` on the model and SQL-quoted by the generator, so
// they render as free text (numbers, dates, or tokens all round-trip).
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:84-86; generators/test/range.py:45-48
//   column        required  validators/action/_test_requirements.py:87-88; generators/test/range.py:24,49
//   min_value ⊕ max_value  requiredOneOf  validators/action/_test_requirements.py:89-92
//   min_value               generators/test/range.py:30; models/_action.py:119 (Any)
//   max_value               generators/test/range.py:32; models/_action.py:120 (Any)
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'

export const testRangeSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'range',
  title: 'Range',
  summary: "A column's values must fall within a minimum and/or maximum bound.",
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
          path: ['column'],
          label: 'Column',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'total_price',
        },
        {
          path: ['min_value'],
          label: 'Minimum',
          widget: 'text',
          monospace: true,
          placeholder: '0',
        },
        {
          path: ['max_value'],
          label: 'Maximum',
          widget: 'text',
          monospace: true,
          placeholder: '1000000',
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
      kind: 'requiredOneOf',
      paths: [['min_value'], ['max_value']],
      message: 'Set at least one of minimum / maximum.',
    },
  ],
}

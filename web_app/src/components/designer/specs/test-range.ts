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
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                           models/_enums.py:28-32 `ViolationAction` is STALE (no `drop`) + unused.
//   test_id                 models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                           → help-schema-parity deferred to Task 5.1.
//   target                  models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testRangeSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'range',
  title: 'Range',
  summary: "A column's values must fall within a minimum and/or maximum bound.",
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
      description:
        'Minimum / maximum are free values — a number, a date, or a substitution token all round-trip (they are SQL-quoted at generate time).',
      fields: [
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
  rules: [
    {
      kind: 'requiredOneOf',
      paths: [['min_value'], ['max_value']],
      message: 'Set at least one of minimum / maximum.',
    },
  ],
}

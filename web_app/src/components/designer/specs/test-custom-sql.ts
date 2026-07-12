// ── test / custom_sql — user SQL with attached expectations ──
//
// Discriminator: `test_type` = 'custom_sql'. `sql` is required; `source` is an
// optional fallback (the template emits `spark.table(<source>)` when no sql is
// given, but the validator still requires sql). `expectations` is an OPTIONAL
// inline list of {name, expression, on_violation}; each rule carries its own
// on_violation (there is no action-level on_violation here — see report).
//
// Field provenance:
//   sql            required  validators/action/_test_requirements.py:144-147; generators/test/custom_sql.py:26
//   source                   generators/test/custom_sql.py:29-32 (fallback); validators/action/_test_requirements.py:140-143
//   expectations             generators/test/custom_sql.py:23; generators/test/_base.py:79-96 (List[Dict])
//   expectations[].name      required  generators/test/_base.py:87
//   expectations[].expression required generators/test/_base.py:88
//   expectations[].on_violation        generators/test/_base.py:89 (fail|drop|warn, default fail)

import type { ActionSubTypeSpec } from './types'

export const testCustomSqlSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'custom_sql',
  title: 'Custom SQL',
  summary: 'Run a custom query and assert expectations on its result.',
  groups: [
    {
      title: 'Test',
      fields: [
        {
          path: ['sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          required: true,
          placeholder: 'SELECT month, pct_difference FROM ${catalog}.${gold_schema}.revenue_comparison',
          help: 'Query whose rows the expectations are evaluated against.',
        },
        {
          path: ['source'],
          label: 'Source',
          widget: 'stringOrList',
          monospace: true,
          placeholder: '${catalog}.${gold_schema}.monthly_revenue',
          help: 'Optional fallback table when no SQL result view is used.',
        },
      ],
    },
    {
      title: 'Expectations',
      fields: [
        {
          path: ['expectations'],
          label: 'Expectations',
          widget: 'objectList',
          help: 'Each rule: a name, a boolean expression, and its own violation action.',
          itemFields: [
            {
              path: ['name'],
              label: 'Name',
              widget: 'text',
              monospace: true,
              required: true,
              placeholder: 'revenue_matches',
            },
            {
              path: ['expression'],
              label: 'Expression',
              widget: 'text',
              monospace: true,
              required: true,
              placeholder: 'pct_difference < 0.5',
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
    },
  ],
}

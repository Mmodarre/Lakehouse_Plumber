// ── test / custom_sql — user SQL with attached expectations ──
//
// Discriminator: `test_type` = 'custom_sql'. `sql` is required inline code
// (codeFields routes the `sql` key to the Monaco editor); `source` is an
// optional fallback (the template emits `spark.table(<source>)` when no sql is
// given, but the validator still requires sql). `expectations` is an OPTIONAL
// inline objectList of {name, expression, on_violation}; each rule carries its
// OWN on_violation, so there is NO action-level on_violation here — the
// Expectations group description carries that note (a field-less "Violation
// handling" group cannot render — see ActionModalEditor:386).
//
// Field provenance:
//   sql            required  validators/action/_test_requirements.py:144-147; generators/test/custom_sql.py:26; codeFields.ts (inline sql)
//   source                   generators/test/custom_sql.py:29-32 (fallback); validators/action/_test_requirements.py:140-143
//   expectations             generators/test/custom_sql.py:23 (optional); generators/test/_base.py:79-96 (List[Dict])
//   expectations[].name      required  generators/test/_base.py:87 (exp["name"])
//   expectations[].expression required generators/test/_base.py:88 (exp["expression"])
//   expectations[].on_violation        generators/test/_base.py:89 (fail|warn|drop, default fail)
//   test_id                  models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                            → help-schema-parity deferred to Task 5.1.
//   target                   models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testCustomSqlSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'custom_sql',
  title: 'Custom SQL',
  summary: 'Run a custom query and assert expectations on its result.',
  groups: [
    {
      title: 'Target under test',
      fields: [
        {
          path: ['source'],
          label: 'Source',
          widget: 'stringOrList',
          monospace: true,
          placeholder: '${catalog}.${gold_schema}.monthly_revenue',
        },
      ],
    },
    {
      title: 'Test parameters',
      fields: [
        {
          path: ['sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          required: true,
          placeholder: 'SELECT month, pct_difference FROM ${catalog}.${gold_schema}.revenue_comparison',
        },
      ],
    },
    {
      title: 'Expectations',
      description: 'Optional. Each expectation sets its own on_violation (fail / warn / drop).',
      fields: [
        {
          path: ['expectations'],
          label: 'Expectations',
          widget: 'objectList',
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

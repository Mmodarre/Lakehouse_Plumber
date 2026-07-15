// ── test / custom_expectations — arbitrary expectations on a source ──
//
// Discriminator: `test_type` = 'custom_expectations'. `expectations` is a
// REQUIRED inline objectList of {name, expression, on_violation} — this is the
// spec that motivated the `objectList` widget. Each rule carries its OWN
// on_violation, so there is NO action-level on_violation here — the Expectations
// group description carries that note (a field-less "Violation handling" group
// cannot render — see ActionModalEditor:386). The blank first row is seeded by
// the create-flow skeleton (Task 5.3), not the spec (specs are declarative).
//
// Field provenance:
//   source                     required  validators/action/_test_requirements.py:149-151; generators/test/custom_expectations.py:26-29
//   expectations               required  validators/action/_test_requirements.py:152-155; generators/test/custom_expectations.py:23; generators/test/_base.py:79-96
//   expectations[].name        required  generators/test/_base.py:87 (exp["name"])
//   expectations[].expression  required  generators/test/_base.py:88 (exp["expression"])
//   expectations[].on_violation          generators/test/_base.py:89 (fail|warn|drop, default fail)
//   test_id                    models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                              → help-schema-parity deferred to Task 5.1.
//   target                     models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testCustomExpectationsSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'custom_expectations',
  title: 'Custom expectations',
  summary: 'Attach arbitrary named boolean expectations to a source table.',
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
      title: 'Expectations',
      description: 'Each expectation sets its own on_violation (fail / warn / drop).',
      fields: [
        {
          path: ['expectations'],
          label: 'Expectations',
          widget: 'objectList',
          required: true,
          itemFields: [
            {
              path: ['name'],
              label: 'Name',
              widget: 'text',
              monospace: true,
              required: true,
              placeholder: 'positive_amount',
            },
            {
              path: ['expression'],
              label: 'Expression',
              widget: 'text',
              monospace: true,
              required: true,
              placeholder: 'total_price > 0',
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

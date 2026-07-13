// ── test / custom_expectations — arbitrary expectations on a source ──
//
// Discriminator: `test_type` = 'custom_expectations'. `expectations` is a
// REQUIRED inline list of {name, expression, on_violation} — this is the spec
// that motivated the `objectList` widget. Each rule carries its own
// on_violation (there is no action-level on_violation here — see report).
//
// Field provenance:
//   source                     required  validators/action/_test_requirements.py:149-151; generators/test/custom_expectations.py:26-29
//   expectations               required  validators/action/_test_requirements.py:152-155; generators/test/custom_expectations.py:23; generators/test/_base.py:79-96 (List[Dict])
//   expectations[].name        required  generators/test/_base.py:87
//   expectations[].expression  required  generators/test/_base.py:88
//   expectations[].on_violation          generators/test/_base.py:89 (fail|drop|warn, default fail)

import type { ActionSubTypeSpec } from './types'

export const testCustomExpectationsSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'custom_expectations',
  title: 'Custom expectations',
  summary: 'Attach arbitrary named boolean expectations to a source table.',
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
      ],
    },
    {
      title: 'Expectations',
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
  ],
}

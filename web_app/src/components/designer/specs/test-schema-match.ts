// ── test / schema_match — two tables share the same schema ──
//
// Discriminator: `test_type` = 'schema_match'. Both `source` and `reference`
// must be fully-qualified three-part Unity Catalog names (the generator diffs
// their information_schema.columns).
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:94-100 (three-part name); generators/test/schema_match.py:34-36,53-95
//   reference     required  validators/action/_test_requirements.py:101-108 (three-part name); generators/test/schema_match.py:37
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'

export const testSchemaMatchSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'schema_match',
  title: 'Schema match',
  summary: "Compare two tables' schemas via information_schema.",
  groups: [
    {
      title: 'Test',
      fields: [
        {
          path: ['source'],
          label: 'Source',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.fact_orders',
          help: 'Fully-qualified three-part table name.',
        },
        {
          path: ['reference'],
          label: 'Reference',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${gold_schema}.fact_orders_expected',
          help: 'Fully-qualified three-part table name to compare against.',
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

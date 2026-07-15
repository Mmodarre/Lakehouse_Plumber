// ── test / schema_match — two tables share the same schema ──
//
// Discriminator: `test_type` = 'schema_match'. Both `source` and `reference`
// must be fully-qualified three-part Unity Catalog names (the generator diffs
// their information_schema.columns) — both are "under test" (a symmetric compare).
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:94-100 (three-part name); generators/test/schema_match.py:34-36,53-95
//   reference     required  validators/action/_test_requirements.py:101-108 (three-part name); generators/test/schema_match.py:37
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                           models/_enums.py:28-32 `ViolationAction` is STALE (no `drop`) + unused.
//   test_id                 models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                           → help-schema-parity deferred to Task 5.1.
//   target                  models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testSchemaMatchSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'schema_match',
  title: 'Schema match',
  summary: "Compare two tables' schemas via information_schema.",
  groups: [
    {
      title: 'Target under test',
      fields: [
        {
          path: ['source'],
          label: 'Source',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.fact_orders',
        },
        {
          path: ['reference'],
          label: 'Reference',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${gold_schema}.fact_orders_expected',
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

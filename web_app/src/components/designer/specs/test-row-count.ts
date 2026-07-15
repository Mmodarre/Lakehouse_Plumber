// ── test / row_count — compare two tables' row counts ───────
//
// Discriminator: `test_type` = 'row_count'. `source` is a list of EXACTLY two
// tables; the check passes when |count(a) − count(b)| ≤ tolerance. The two
// tables render through the fixed-arity `dualSource` widget (Table A / Table B),
// NOT a growable list — but the "exactly two" soft rule is kept as a hand-edit
// backstop for a legacy non-2 list.
//
// Field provenance:
//   source        required  validators/action/_test_requirements.py:25-35 (list of exactly 2); generators/test/row_count.py:34-36
//   tolerance               models/_action.py:111 (Optional[int]); generators/test/row_count.py:24 (default 0)
//   on_violation            validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                           NB: the models/_enums.py:28-32 `ViolationAction` (fail|warn only) is STALE + unused — the
//                           validator/generator literal set is the source of truth.
//   test_id                 models/_action.py:125 (Optional[str]); validators/field/_field_catalog.py:159.
//                           NOT in flowgroup.schema.json → help-schema-parity deferred to Task 5.1.
//   target                  models/_action.py:68 (Optional[str]); default tmp_test_<name> via _action.py:127-130 +
//                           generators/test/_base.py:60; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'

export const testRowCountSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'row_count',
  title: 'Row count',
  summary: 'Compare row counts between two tables within a tolerance.',
  groups: [
    {
      title: 'Target under test',
      fields: [
        {
          path: ['source'],
          label: 'Sources',
          widget: 'dualSource',
          required: true,
        },
      ],
    },
    {
      title: 'Test parameters',
      fields: [
        {
          path: ['tolerance'],
          label: 'Tolerance',
          widget: 'number',
          min: 0,
          valueType: 'integer',
          placeholder: '0',
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
      kind: 'custom',
      paths: [['source']],
      check: (raw) =>
        Array.isArray(raw.source) && raw.source.length !== 2
          ? 'Row count compares exactly two sources.'
          : null,
    },
  ],
}

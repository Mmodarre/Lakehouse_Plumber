// ── test / referential_integrity — source rows resolve in a reference ──
//
// Discriminator: `test_type` = 'referential_integrity'. `source_columns` and
// `reference_columns` are joined positionally, so they must be equal length —
// rendered as two `stringList` fields with the equal-length soft rule kept.
//
// Field provenance:
//   source            required  validators/action/_test_requirements.py:45-49; generators/test/referential_integrity.py:35-37
//   reference         required  validators/action/_test_requirements.py:50-57 (three-part name); generators/test/referential_integrity.py:38
//   source_columns    required  validators/action/_test_requirements.py:58-61
//   reference_columns required  validators/action/_test_requirements.py:62-65
//   (equal length)              validators/action/_test_requirements.py:66-74 (_name_checks.py:20-33)
//   on_violation                validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:40-45 (default fail).
//                               models/_enums.py:28-32 `ViolationAction` is STALE (no `drop`) + unused.
//   test_id                     models/_action.py:125; validators/field/_field_catalog.py:159. NOT in flowgroup.schema.json
//                               → help-schema-parity deferred to Task 5.1.
//   target                      models/_action.py:68; default tmp_test_<name> via _action.py:127-130; flowgroup.schema.json:354-357.

import type { ActionSubTypeSpec } from './types'
import { isPresent, readPath } from './helpers'

export const testReferentialIntegritySpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'referential_integrity',
  title: 'Referential integrity',
  summary: 'Every source row must resolve to a row in the reference table.',
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
      description: 'Source columns and reference columns are joined positionally — keep them the same length.',
      fields: [
        {
          path: ['reference'],
          label: 'Reference',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.dim_customer',
        },
        {
          path: ['source_columns'],
          label: 'Source columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
        },
        {
          path: ['reference_columns'],
          label: 'Reference columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'customer_id',
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
      paths: [['source_columns'], ['reference_columns']],
      check: (raw) => {
        const src = readPath(raw, ['source_columns'])
        const ref = readPath(raw, ['reference_columns'])
        return isPresent(src) &&
          isPresent(ref) &&
          Array.isArray(src) &&
          Array.isArray(ref) &&
          src.length !== ref.length
          ? 'Source columns and reference columns must be the same length.'
          : null
      },
    },
  ],
}

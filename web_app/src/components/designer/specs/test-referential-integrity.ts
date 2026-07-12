// ── test / referential_integrity — source rows resolve in a reference ──
//
// Discriminator: `test_type` = 'referential_integrity'. `source_columns` and
// `reference_columns` are joined positionally, so they must be equal length.
//
// Field provenance:
//   source            required  validators/action/_test_requirements.py:45-49; generators/test/referential_integrity.py:35-37
//   reference         required  validators/action/_test_requirements.py:50-57 (three-part name); generators/test/referential_integrity.py:38
//   source_columns    required  validators/action/_test_requirements.py:58-61
//   reference_columns required  validators/action/_test_requirements.py:62-65
//   (equal length)              validators/action/_test_requirements.py:66-74
//   on_violation                validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'
import { isPresent, readPath } from './helpers'

export const testReferentialIntegritySpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'referential_integrity',
  title: 'Referential integrity',
  summary: 'Every source row must resolve to a row in the reference table.',
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
          path: ['reference'],
          label: 'Reference',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${silver_schema}.dim_customer',
          help: 'Fully-qualified three-part table name.',
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
          help: 'Joined positionally to source columns (equal length).',
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

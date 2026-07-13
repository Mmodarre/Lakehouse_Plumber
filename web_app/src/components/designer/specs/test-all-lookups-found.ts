// ── test / all_lookups_found — every source row resolves in a lookup ──
//
// Discriminator: `test_type` = 'all_lookups_found'. `lookup_columns` and
// `lookup_result_columns` are joined positionally, so they must be equal length.
//
// Field provenance:
//   source                required  validators/action/_test_requirements.py:110-112; generators/test/all_lookups_found.py:35-37
//   lookup_table          required  validators/action/_test_requirements.py:113-120 (three-part name); generators/test/all_lookups_found.py:42
//   lookup_columns        required  validators/action/_test_requirements.py:121-124; generators/test/all_lookups_found.py:38
//   lookup_result_columns required  validators/action/_test_requirements.py:125-128; generators/test/all_lookups_found.py:39
//   (equal length)                  validators/action/_test_requirements.py:129-137
//   on_violation                    validators/action/test.py:32-36 (fail|warn|drop); generators/test/_base.py:42

import type { ActionSubTypeSpec } from './types'
import { isPresent, readPath } from './helpers'

export const testAllLookupsFoundSpec: ActionSubTypeSpec = {
  kind: 'test',
  subType: 'all_lookups_found',
  title: 'All lookups found',
  summary: 'Every source row must resolve to a row in the lookup table.',
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
          path: ['lookup_table'],
          label: 'Lookup table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}.${gold_schema}.dim_date',
        },
        {
          path: ['lookup_columns'],
          label: 'Lookup columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'order_date',
        },
        {
          path: ['lookup_result_columns'],
          label: 'Lookup result columns',
          widget: 'stringList',
          monospace: true,
          required: true,
          placeholder: 'date_key',
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
  rules: [
    {
      kind: 'custom',
      paths: [['lookup_columns'], ['lookup_result_columns']],
      check: (raw) => {
        const cols = readPath(raw, ['lookup_columns'])
        const results = readPath(raw, ['lookup_result_columns'])
        return isPresent(cols) &&
          isPresent(results) &&
          Array.isArray(cols) &&
          Array.isArray(results) &&
          cols.length !== results.length
          ? 'Lookup columns and lookup result columns must be the same length.'
          : null
      },
    },
  ],
}

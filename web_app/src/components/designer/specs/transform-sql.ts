// ── transform / sql — SQL transform ──────────────────────────
//
// Field provenance (fields are flat on the action):
//   source     required   validators/action/transform.py:72-73 (string or list of views)
//   sql                   validators/action/transform.py:70; generators/transform/sql.py:57 (inline SQL)
//   sql_path              validators/action/transform.py:70; generators/transform/sql.py:59 (external .sql file)
//   target     required   validators/action/transform.py:25-26 (output view)
// Rule: exactly one of sql / sql_path (transform.py:70 requires ≥1;
//       references/actions-transform-sql.md:3 requires exactly one).

import type { ActionSubTypeSpec } from './types'

export const transformSqlSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'sql',
  title: 'SQL transform',
  summary: 'Transform one or more views with SQL into a new view.',
  groups: [
    {
      title: 'Query',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_orders_raw',
          help: 'Input view, or a list of views. Use stream(view) in SQL for streaming reads.',
        },
        {
          path: ['sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          placeholder: 'SELECT * FROM v_orders_raw WHERE amount > 0',
          help: 'Inline SQL. Provide exactly one of SQL / SQL file.',
        },
        {
          path: ['sql_path'],
          label: 'SQL file',
          widget: 'text',
          monospace: true,
          placeholder: 'queries/transform_orders.sql',
          help: 'External SQL file. Provide exactly one of SQL / SQL file.',
        },
        {
          path: ['target'],
          label: 'Target view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
          help: 'View this transform publishes for downstream actions.',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'xor',
      paths: [['sql'], ['sql_path']],
      message: 'Provide exactly one of SQL / SQL file.',
    },
  ],
}

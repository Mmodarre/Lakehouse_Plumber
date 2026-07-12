// ── load / sql — materialize a SQL query into a temp view ────
//
// The discriminator is `source.type: sql`, so `source` is a dict carrying the
// query (a bare-string `source` has no discriminator and is a YAML-text form).
//   source.sql            generators/load/sql.py:75-76 (inline SQL)      \ exactly one
//   source.sql_path       generators/load/sql.py:77-85 (external .sql)   /
//   target       required validators/action/load.py:17-18
// Rule: exactly one of sql / sql_path (sql.py:87 requires ≥1;
//       references/actions-load-sql.md:28 requires exactly one).

import type { ActionSubTypeSpec } from './types'

export const loadSqlSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'sql',
  title: 'SQL query',
  summary: 'Materialize a SQL query into a temporary view.',
  groups: [
    {
      title: 'Query',
      description: 'Provide exactly one of SQL / SQL file.',
      fields: [
        {
          path: ['source', 'sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          placeholder: 'SELECT * FROM ${catalog}.${bronze_schema}.orders',
          help: 'Inline SQL query.',
        },
        {
          path: ['source', 'sql_path'],
          label: 'SQL file',
          widget: 'text',
          monospace: true,
          placeholder: 'queries/load_orders.sql',
          help: 'External .sql file (relative to project root).',
        },
      ],
    },
    {
      title: 'Target',
      fields: [
        {
          path: ['target'],
          label: 'Target view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
          help: 'Temporary view this load publishes for downstream actions.',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'xor',
      paths: [
        ['source', 'sql'],
        ['source', 'sql_path'],
      ],
      message: 'Provide exactly one of SQL / SQL file.',
    },
  ],
}

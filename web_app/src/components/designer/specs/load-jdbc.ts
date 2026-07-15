// ── load / jdbc — read from an external database over JDBC ───
//
//   source.url        required  validators/action/load.py:102-105
//   source.driver     required  validators/action/load.py:102-105
//   source.user       required  validators/action/load.py:102-105 (supply via ${secret:...})
//   source.password   required  validators/action/load.py:102-105 (supply via ${secret:...})
//   source.table                validators/action/load.py:107-108 (one of table/query)
//   source.query                validators/action/load.py:107-108 (one of table/query)
//   target            required  validators/action/load.py:17-18
// Rule: exactly one of table / query (load.py:107 requires ≥1;
//       references/actions-load-jdbc.md:33 — mutually exclusive).

import type { ActionSubTypeSpec } from './types'

export const loadJdbcSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'jdbc',
  title: 'JDBC',
  summary: 'Read from an external database over JDBC.',
  groups: [
    {
      title: 'Connection',
      description: 'Supply user / password as ${secret:scope/key} references — they hold token refs, never plaintext.',
      fields: [
        {
          path: ['source', 'url'],
          label: 'JDBC URL',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'jdbc:postgresql://host:5432/db',
        },
        {
          path: ['source', 'driver'],
          label: 'Driver',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'org.postgresql.Driver',
        },
        {
          path: ['source', 'user'],
          label: 'User',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${secret:db/user}',
        },
        {
          path: ['source', 'password'],
          label: 'Password',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${secret:db/password}',
        },
      ],
    },
    {
      title: 'Read',
      description: 'Provide exactly one of table / query.',
      fields: [
        {
          path: ['source', 'table'],
          label: 'Table',
          widget: 'text',
          monospace: true,
          placeholder: 'public.orders',
        },
        {
          path: ['source', 'query'],
          label: 'Query',
          widget: 'textarea',
          monospace: true,
          placeholder: 'SELECT * FROM public.orders',
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
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'xor',
      paths: [
        ['source', 'table'],
        ['source', 'query'],
      ],
      message: 'Provide exactly one of table / query.',
    },
  ],
}

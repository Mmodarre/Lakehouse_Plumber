// ── transform / sql — SQL transform ──────────────────────────
//
// Field provenance (fields are flat on the action):
//   source     required   validators/action/transform.py:72-73 (string or list of views)
//   sql                   validators/action/transform.py:70; generators/transform/sql.py:57 (inline SQL)
//   sql_path              validators/action/transform.py:70; generators/transform/sql.py:59 (external .sql file)
//   target     required   validators/action/transform.py:25-26 (output view)
// Rule: exactly one of sql / sql_path (transform.py:70 requires ≥1;
//       references/actions-transform-sql.md:3 requires exactly one).
//
// `sql` ⊕ `sql_path` render as a single `oneOfToggle` (Task 4.2a): the Inline
// SQL ⊕ From file segments each own one key, and switching PRUNES the inactive
// branch's key (via applyDiscriminatorChange, no discriminator write) — so the
// exactly-one rule the Python validator enforces is maintained structurally.
// The `xor` soft rule is KEPT as a raw-YAML backstop (Phase-4 preamble: keep
// every existing cross-field rule): it reads `['sql']`/`['sql_path']` directly,
// so a stale hand-edited YAML carrying both keys is still soft-flagged.

import type { ActionSubTypeSpec } from './types'

export const transformSqlSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'sql',
  title: 'SQL transform',
  summary: 'Transform one or more views with SQL into a new view.',
  groups: [
    {
      title: 'Query',
      description: 'Transform the source view(s). Provide inline SQL or reference a .sql file.',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_orders_raw',
        },
        {
          // Synthetic path — the branches own the real `sql` / `sql_path` keys.
          path: ['__sql_source'],
          label: 'SQL',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'inline',
                label: 'Inline SQL',
                path: ['sql'],
                backing: 'inline',
                language: 'sql',
                placeholder: 'SELECT * FROM v_orders_raw WHERE amount > 0',
              },
              {
                value: 'file',
                label: 'From file',
                path: ['sql_path'],
                backing: 'file',
                accept: ['.sql'],
                placeholder: 'queries/transform_orders.sql',
              },
            ],
          },
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
      paths: [['sql'], ['sql_path']],
      message: 'Provide exactly one of SQL / SQL file.',
    },
  ],
}

// ── transform / temp_table — materialize an intermediate table ─
//
// Fields are flat on the action.
//   source     required  validators/action/transform.py:130-134 (view-name string,
//                        or a dict with view/source: generators/transform/temp_table.py:48-52)
//   sql                  generators/transform/temp_table.py:41 (optional; the literal
//                        {source} placeholder is replaced with the source view)
//   readMode             generators/transform/temp_table.py:21 (default batch)
//   target     required  validators/action/transform.py:25-26
//
// Unlike the sql sub-type, temp_table has NO `sql_path` — the SQL is inline
// only, so it stays a plain optional code field (Monaco via the last-segment
// `sql` heuristic in codeFields.ts), NOT a `oneOfToggle`. The `{source}` token
// in the SQL is a LITERAL replaced by the source view at generate time; it is
// NOT an LHP `${}` substitution token (documented in the group description).

import type { ActionSubTypeSpec } from './types'

export const transformTempTableSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'temp_table',
  title: 'Temp table',
  summary: 'Materialize an intermediate temp table for reuse by later transforms.',
  groups: [
    {
      title: 'Table',
      description:
        'In the SQL, the literal {source} placeholder is replaced with the source view — it is not a ${} substitution token.',
      fields: [
        {
          path: ['source'],
          label: 'Source view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
        },
        {
          path: ['sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          placeholder: "SELECT * FROM {source} WHERE status = 'active'",
        },
        {
          path: ['target'],
          label: 'Target table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'staging_orders',
        },
      ],
    },
    {
      title: 'Advanced',
      advanced: true,
      fields: [
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['batch', 'stream'],
          enumDefault: 'batch',
        },
      ],
    },
  ],
}

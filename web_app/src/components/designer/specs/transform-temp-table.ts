// ── transform / temp_table — materialize an intermediate table ─
//
// Fields are flat on the action.
//   source     required  validators/action/transform.py:130-134 (view-name string,
//                        or a dict with view/source: generators/transform/temp_table.py:48-52)
//   sql                  generators/transform/temp_table.py:41 (optional; the literal
//                        {source} placeholder is replaced with the source view)
//   readMode             generators/transform/temp_table.py:21 (default batch)
//   target     required  validators/action/transform.py:25-26

import type { ActionSubTypeSpec } from './types'

export const transformTempTableSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'temp_table',
  title: 'Temp table',
  summary: 'Materialize an intermediate temp table for reuse by later transforms.',
  groups: [
    {
      title: 'Table',
      fields: [
        {
          path: ['source'],
          label: 'Source view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
          help: 'Input view to materialize.',
        },
        {
          path: ['sql'],
          label: 'SQL',
          widget: 'textarea',
          monospace: true,
          placeholder: 'SELECT * FROM {source} WHERE status = ...',
          help: 'Optional. {source} is replaced with the source view; use stream({source}) with readMode: stream.',
        },
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['batch', 'stream'],
          enumDefault: 'batch',
        },
        {
          path: ['target'],
          label: 'Target table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'staging_orders',
          help: 'Name of the intermediate temp table this transform publishes.',
        },
      ],
    },
  ],
}

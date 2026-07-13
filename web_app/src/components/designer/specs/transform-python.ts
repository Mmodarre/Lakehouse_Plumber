// ── transform / python — call a Python function on view(s) ───
//
// Fields are FLAT on the action (unlike load/python, which nests under source).
//   source        required  validators/action/transform.py:89-102 (string or list of views)
//   module_path   required  validators/action/transform.py:104-107; generators/transform/python.py:23,36
//   function_name required  validators/action/transform.py:117-120; generators/transform/python.py:24,51
//   parameters              validators/action/transform.py:122-126; generators/transform/python.py:25
//   readMode                generators/transform/python.py:77 (default batch)
//   target        required  validators/action/transform.py:25-26

import type { ActionSubTypeSpec } from './types'

export const transformPythonSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'python',
  title: 'Python transform',
  summary: 'Transform view(s) by calling a Python function.',
  groups: [
    {
      title: 'Function',
      fields: [
        {
          path: ['source'],
          label: 'Source view(s)',
          widget: 'stringOrList',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
        },
        {
          path: ['module_path'],
          label: 'Module path',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'transforms/enrich.py',
        },
        {
          path: ['function_name'],
          label: 'Function name',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'enrich',
        },
        {
          path: ['parameters'],
          label: 'Parameters',
          widget: 'keyValue',
        },
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['batch', 'stream'],
          enumDefault: 'batch',
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
          placeholder: 'v_orders_enriched',
        },
      ],
    },
  ],
}

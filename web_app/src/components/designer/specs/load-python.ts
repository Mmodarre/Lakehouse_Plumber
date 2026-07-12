// ── load / python — call a Python function returning a DataFrame ─
//
// Fields nest under `source` (unlike transform/python, which is flat).
//   source.module_path    required  validators/action/load.py:114-115
//                                   (must end .py: generators/load/python.py:69)
//   source.function_name            generators/load/python.py:46 (default 'get_df')
//   source.parameters               generators/load/python.py:47 (passed as a dict)
//   target                required  validators/action/load.py:17-18

import type { ActionSubTypeSpec } from './types'

export const loadPythonSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'python',
  title: 'Python function',
  summary: 'Call a Python function that returns a DataFrame.',
  groups: [
    {
      title: 'Function',
      fields: [
        {
          path: ['source', 'module_path'],
          label: 'Module path',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'loaders/external.py',
          help: 'Path to a .py file (relative to project root).',
        },
        {
          path: ['source', 'function_name'],
          label: 'Function name',
          widget: 'text',
          monospace: true,
          placeholder: 'get_df',
          help: 'Function to call. Defaults to get_df.',
        },
        {
          path: ['source', 'parameters'],
          label: 'Parameters',
          widget: 'keyValue',
          help: 'Passed to the function as a dict.',
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
          placeholder: 'v_external',
          help: 'Temporary view this load publishes for downstream actions.',
        },
      ],
    },
  ],
}

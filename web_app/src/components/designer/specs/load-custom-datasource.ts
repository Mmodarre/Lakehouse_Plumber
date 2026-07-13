// ── load / custom_datasource — user PySpark DataSource class ──
//
//   source.module_path              required  generators/load/custom_datasource.py:90,94-110
//   source.custom_datasource_class  required  generators/load/custom_datasource.py:91,112-128
//   source.options                            generators/load/custom_datasource.py:92 (reader options)
//   readMode                                  generators/load/custom_datasource.py:173
//                                             (reads ONLY action.readMode; default stream)
//   target                          required  validators/action/load.py:17-18
//
// custom_datasource honors readMode ONLY at the top-level action key (unlike
// delta/kafka/cloudfiles, which also read source.readMode) — see report note.

import type { ActionSubTypeSpec } from './types'

export const loadCustomDatasourceSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'custom_datasource',
  title: 'Custom data source',
  summary: 'Read through a user-supplied PySpark DataSource class.',
  groups: [
    {
      title: 'Data source',
      fields: [
        {
          path: ['source', 'module_path'],
          label: 'Module path',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'sources/my_source.py',
        },
        {
          path: ['source', 'custom_datasource_class'],
          label: 'Class name',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'MyDataSource',
        },
        {
          path: ['source', 'options'],
          label: 'Reader options',
          widget: 'keyValue',
        },
      ],
    },
    {
      title: 'Read',
      fields: [
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['batch', 'stream'],
          enumDefault: 'stream',
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
          placeholder: 'v_custom',
        },
      ],
    },
  ],
}

// ── load / cloudfiles — Databricks Auto Loader ───────────────
//
// Field provenance (validators + generator + reference MD):
//   source.path            required   validators/action/load.py:84-85
//   source.format          required   validators/action/load.py:86-87 (default 'json' generators/load/cloudfiles.py:76)
//   source.readMode        stream-only generators/load/cloudfiles.py:66-67
//   source.schema                     generators/load/cloudfiles.py:87 (XOR schema_file / schemaHints, references/actions-load-cloudfiles.md:12)
//   source.schema_file                references/actions-load-cloudfiles.md:13 (back-compat)
//   source.options                    generators/load/cloudfiles.py:101-102 (cloudFiles.*)
//   source.reader_options             generators/load/cloudfiles.py:121-122
//   source.format_options             generators/load/cloudfiles.py:123-124
//   target                 required   temp view the load publishes (references/actions-load-cloudfiles.md:26)

import type { ActionSubTypeSpec } from './types'
import { readPath } from './helpers'

export const loadCloudfilesSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'cloudfiles',
  title: 'Auto Loader (cloudFiles)',
  summary: 'Stream files from a volume/path into a temporary view.',
  groups: [
    {
      title: 'Source',
      fields: [
        {
          path: ['source', 'path'],
          label: 'Path',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${landing_volume}/orders/*.json',
        },
        {
          path: ['source', 'format'],
          label: 'Format',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'json',
        },
        {
          path: ['source', 'readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['stream'],
          enumDefault: 'stream',
        },
      ],
    },
    {
      title: 'Schema',
      description: 'Set at most one. Leave blank to let Auto Loader infer.',
      fields: [
        {
          path: ['source', 'schema'],
          label: 'Schema file',
          widget: 'text',
          monospace: true,
        },
        {
          path: ['source', 'schema_file'],
          label: 'Schema file (legacy)',
          widget: 'text',
          monospace: true,
        },
      ],
    },
    {
      title: 'Reader options',
      fields: [
        {
          path: ['source', 'options'],
          label: 'cloudFiles options',
          widget: 'keyValue',
        },
        {
          path: ['source', 'reader_options'],
          label: 'Reader options',
          widget: 'keyValue',
        },
        {
          path: ['source', 'format_options'],
          label: 'Format options',
          widget: 'keyValue',
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
          placeholder: 'v_orders_raw',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'custom',
      paths: [['source', 'readMode']],
      check: (raw) => {
        const mode = readPath(raw, ['source', 'readMode'])
        return typeof mode === 'string' && mode !== 'stream'
          ? 'Auto Loader requires readMode: stream (batch is rejected).'
          : null
      },
    },
    {
      kind: 'mutuallyExclusive',
      paths: [
        ['source', 'schema'],
        ['source', 'schema_file'],
      ],
      message: 'Set only one of schema / schema_file (also exclusive with cloudFiles.schemaHints).',
    },
  ],
}

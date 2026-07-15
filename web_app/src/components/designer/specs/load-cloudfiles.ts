// ── load / cloudfiles — Databricks Auto Loader ───────────────
//
// Field provenance (validators + generator + reference MD):
//   source.path            required   validators/action/load.py:84-85
//   source.format          required   validators/action/load.py:86-87
//                                     enum: schemas/flowgroup.schema.json:213
//                                     default 'json' generators/load/cloudfiles.py:76
//                                     (mandatory: cloudfiles.py:58,410 — the key must be present,
//                                      so no enumDefault, which would delete it)
//   source.readMode        stream-only generators/load/cloudfiles.py:66-67
//   source.schema                     generators/load/cloudfiles.py:87-99 (string → schema FILE path)
//   source.schema_file                generators/load/cloudfiles.py:129-138 (back-compat)
//   source.schema_location            cloudfiles.py:142 → cloudFiles.schemaLocation (legacy scalar)
//   source.schema_infer_column_types  cloudfiles.py:143 → cloudFiles.inferColumnTypes
//   source.max_files_per_trigger      cloudfiles.py:144 → cloudFiles.maxFilesPerTrigger
//   source.schema_evolution_mode      cloudfiles.py:145 → cloudFiles.schemaEvolutionMode
//                                     enum: schemas/flowgroup.schema.json:306
//   source.rescue_data_column         cloudfiles.py:146 → cloudFiles.rescueDataColumn
//   source.options                    generators/load/cloudfiles.py:101-102 (cloudFiles.*)
//   source.reader_options             generators/load/cloudfiles.py:121-122
//   source.format_options             generators/load/cloudfiles.py:123-124
//   target                 required   temp view the load publishes (references/actions-load-cloudfiles.md:26)
//
// Rules mirror the generator's hard errors:
//   • schema-source exclusivity (schema ⊕ schema_file ⊕ options.cloudFiles.schemaHints):
//     cloudfiles.py:344-355 (_check_conflicts).
//   • legacy scalar ⊕ cloudFiles.* twin conflict: cloudfiles.py:340-342,332-338.
//   • readMode must be stream: cloudfiles.py:66-73.

import type { ActionSubTypeSpec } from './types'
import { readPath } from './helpers'

// The 8 file formats Auto Loader accepts (schemas/flowgroup.schema.json:213),
// json first so the skeleton seeds `format: json`.
const CLOUDFILES_FORMATS = ['json', 'csv', 'parquet', 'avro', 'orc', 'text', 'binaryfile', 'xml']

// Schema files may be JSON/YAML column specs or DDL/SQL (cloudfiles.py:226-244).
const SCHEMA_FILE_ACCEPT = ['.json', '.yaml', '.yml', '.ddl', '.sql']

// Legacy scalar → cloudFiles.* option twins the generator rejects when BOTH are
// set (cloudfiles.py:332-342). Kept as data so the conflict hint stays in sync.
const LEGACY_OPTION_TWINS: [string, string][] = [
  ['schema_location', 'cloudFiles.schemaLocation'],
  ['schema_infer_column_types', 'cloudFiles.inferColumnTypes'],
  ['max_files_per_trigger', 'cloudFiles.maxFilesPerTrigger'],
  ['schema_evolution_mode', 'cloudFiles.schemaEvolutionMode'],
  ['rescue_data_column', 'cloudFiles.rescueDataColumn'],
]

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
          widget: 'enum',
          options: CLOUDFILES_FORMATS,
          required: true,
        },
      ],
    },
    {
      title: 'Read',
      fields: [
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
      description: 'Set at most one schema source. Leave blank to let Auto Loader infer.',
      fields: [
        {
          path: ['source', 'schema'],
          label: 'Schema file',
          widget: 'text',
          monospace: true,
          fileRef: { accept: SCHEMA_FILE_ACCEPT },
        },
        {
          path: ['source', 'schema_file'],
          label: 'Schema file (legacy)',
          widget: 'text',
          monospace: true,
          fileRef: { accept: SCHEMA_FILE_ACCEPT },
        },
        {
          path: ['source', 'schema_location'],
          label: 'Schema location',
          widget: 'text',
          monospace: true,
          placeholder: '${checkpoint_volume}/_schemas/orders',
        },
        {
          path: ['source', 'schema_infer_column_types'],
          label: 'Infer column types',
          widget: 'bool',
          defaultValue: false,
        },
        {
          path: ['source', 'schema_evolution_mode'],
          label: 'Schema evolution mode',
          widget: 'enum',
          options: ['addNewColumns', 'rescue', 'failOnNewColumns'],
          unsetLabel: 'Not set (default)',
        },
        {
          path: ['source', 'rescue_data_column'],
          label: 'Rescued data column',
          widget: 'text',
          monospace: true,
          placeholder: '_rescued_data',
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
      title: 'Advanced',
      advanced: true,
      fields: [
        {
          path: ['source', 'max_files_per_trigger'],
          label: 'Max files per trigger',
          widget: 'number',
          min: 1,
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
        ['source', 'options', 'cloudFiles.schemaHints'],
      ],
      message: 'Set only one schema source: schema, schema_file, or options.cloudFiles.schemaHints.',
    },
    {
      kind: 'custom',
      paths: [
        ['source', 'schema_location'],
        ['source', 'schema_infer_column_types'],
        ['source', 'max_files_per_trigger'],
        ['source', 'schema_evolution_mode'],
        ['source', 'rescue_data_column'],
        ['source', 'options'],
      ],
      check: (raw) => {
        const clashes = LEGACY_OPTION_TWINS.filter(
          ([legacy, option]) =>
            readPath(raw, ['source', legacy]) !== undefined &&
            readPath(raw, ['source', 'options', option]) !== undefined,
        ).map(([legacy, option]) => `${legacy} / ${option}`)
        return clashes.length > 0
          ? `Set the scalar or the cloudFiles option, not both: ${clashes.join(', ')}.`
          : null
      },
    },
  ],
}

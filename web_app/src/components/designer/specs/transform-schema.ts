// ── transform / schema — rename / cast / filter columns ─────
//
// Fields are flat on the action. Requires exactly one of schema_inline / schema_file.
//   source        required  validators/action/_schema_transform.py:94-106 (string view name only)
//   schema_inline           validators/action/_schema_transform.py:108-122 (inline arrow/YAML) \ exactly
//   schema_file             validators/action/_schema_transform.py:108-122 (external file)      / one
//   enforcement             validators/action/_schema_transform.py:124-129 (strict|permissive; default permissive)
//   readMode                generators/transform/schema.py:127 (default stream)
//   target        required  validators/action/transform.py:25-26
// Rule: exactly one of schema_inline / schema_file (_schema_transform.py:113-122).

import type { ActionSubTypeSpec } from './types'

export const transformSchemaSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'schema',
  title: 'Schema transform',
  summary: 'Rename, cast, and filter columns via a schema mapping.',
  groups: [
    {
      title: 'Schema',
      description: 'Provide exactly one of inline schema / schema file.',
      fields: [
        {
          path: ['source'],
          label: 'Source view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders',
          help: 'Input view name (string only; the nested source.view form is not supported).',
        },
        {
          path: ['schema_inline'],
          label: 'Inline schema',
          widget: 'textarea',
          monospace: true,
          placeholder: 'columns:\n  - "old -> new: BIGINT"',
          help: 'Inline schema (arrow or YAML format).',
        },
        {
          path: ['schema_file'],
          label: 'Schema file',
          widget: 'text',
          monospace: true,
          placeholder: 'schemas/orders.yaml',
          help: 'External schema file (relative to project root).',
        },
        {
          path: ['enforcement'],
          label: 'Enforcement',
          widget: 'enum',
          options: ['strict', 'permissive'],
          enumDefault: 'permissive',
          help: 'strict keeps only defined columns; permissive passes the rest through unchanged.',
        },
        {
          path: ['readMode'],
          label: 'Read mode',
          widget: 'enum',
          options: ['batch', 'stream'],
          enumDefault: 'stream',
        },
        {
          path: ['target'],
          label: 'Target view',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'v_orders_typed',
          help: 'View this transform publishes for downstream actions.',
        },
      ],
    },
  ],
  rules: [
    {
      kind: 'xor',
      paths: [['schema_inline'], ['schema_file']],
      message: 'Provide exactly one of inline schema / schema file.',
    },
  ],
}

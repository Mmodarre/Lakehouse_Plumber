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
//
// `schema_inline` ⊕ `schema_file` render as a single `oneOfToggle` (Task 4.2a):
// the Inline schema ⊕ From file segments each own one key, and switching PRUNES
// the inactive branch's key (applyDiscriminatorChange, no discriminator write),
// maintaining the exactly-one rule structurally. The `xor` soft rule is KEPT as
// a raw-YAML backstop (Phase-4 preamble: keep every existing cross-field rule) —
// it reads `['schema_inline']`/`['schema_file']` directly. `source` is
// string-only: the validator rejects a dict/non-string source
// (_schema_transform.py:96-106).

import type { ActionSubTypeSpec } from './types'

export const transformSchemaSpec: ActionSubTypeSpec = {
  kind: 'transform',
  subType: 'schema',
  title: 'Schema transform',
  summary: 'Rename, cast, and filter columns via a schema mapping.',
  groups: [
    {
      title: 'Schema',
      description: 'Provide inline schema or reference a schema file (.yaml/.yml).',
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
          // Synthetic path — the branches own the real schema_inline / schema_file keys.
          path: ['__schema_source'],
          label: 'Schema',
          widget: 'oneOfToggle',
          oneOf: {
            options: [
              {
                value: 'inline',
                label: 'Inline schema',
                path: ['schema_inline'],
                backing: 'inline',
                language: 'yaml',
                placeholder: 'columns:\n  - "old -> new: BIGINT"',
              },
              {
                value: 'file',
                label: 'From file',
                path: ['schema_file'],
                backing: 'file',
                accept: ['.yaml', '.yml'],
                placeholder: 'schemas/orders.yaml',
              },
            ],
          },
        },
        {
          path: ['enforcement'],
          label: 'Enforcement',
          widget: 'enum',
          options: ['strict', 'permissive'],
          enumDefault: 'permissive',
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
          placeholder: 'v_orders_typed',
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
          enumDefault: 'stream',
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

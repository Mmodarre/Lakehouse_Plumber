// ── load / delta — read a Delta table into a temp view ───────
//
// Field provenance (validator + generator + reference MD):
//   source.catalog        required   validators/action/load.py:92-93
//   source.schema         required   validators/action/load.py:94-95
//   source.table          required   validators/action/load.py:96-97
//   readMode                         generators/load/delta.py:94 (action.readMode first, else source.readMode; default batch)
//   source.options                   generators/load/delta.py:60-92 (Delta reader options)
//   source.where_clause              generators/load/delta.py:211 (list of filter predicates)
//   source.select_columns            generators/load/delta.py:212 (projection)
//   target                required   validators/action/load.py:17-18
//
// readMode is authored at the canonical top-level action key (models/_action.py:70);
// the generator reads action.readMode first — see report note on placement.

import type { ActionSubTypeSpec } from './types'

export const loadDeltaSpec: ActionSubTypeSpec = {
  kind: 'load',
  subType: 'delta',
  title: 'Delta table',
  summary: 'Read a Delta table (batch or stream) into a temporary view.',
  groups: [
    {
      title: 'Source table',
      description: 'Three-part reference: catalog.schema.table.',
      fields: [
        {
          path: ['source', 'catalog'],
          label: 'Catalog',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${catalog}',
        },
        {
          path: ['source', 'schema'],
          label: 'Schema',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: '${bronze_schema}',
        },
        {
          path: ['source', 'table'],
          label: 'Table',
          widget: 'text',
          monospace: true,
          required: true,
          placeholder: 'customers',
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
          enumDefault: 'batch',
          help: 'batch reads a snapshot; stream reads incrementally.',
        },
      ],
    },
    {
      title: 'Options',
      fields: [
        {
          path: ['source', 'options'],
          label: 'Reader options',
          widget: 'keyValue',
          help: 'Delta reader options, e.g. readChangeFeed, startingVersion, maxFilesPerTrigger.',
        },
        {
          path: ['source', 'where_clause'],
          label: 'Where clauses',
          widget: 'stringList',
          monospace: true,
          help: 'Filter predicates applied to the read.',
        },
        {
          path: ['source', 'select_columns'],
          label: 'Select columns',
          widget: 'stringList',
          monospace: true,
          help: 'Projection; leave blank for all columns.',
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
          placeholder: 'v_customers',
          help: 'Temporary view this load publishes for downstream actions.',
        },
      ],
    },
  ],
}

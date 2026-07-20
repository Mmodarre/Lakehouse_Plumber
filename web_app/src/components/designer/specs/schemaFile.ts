// ── schema file — path proposal + skeleton for the schemas/ sidecar ─
//
// One strict file under `schemas/` now serves BOTH readers: the `table_schema`
// reader consumes `name`/`type`/`nullable`/`comment`, and the `tags_file` reader
// consumes top-level `tags:` and per-column `tags:`. Single-sourced for both
// write specs (streaming_table + materialized_view) so the "New" affordance on
// `tags_file` and the `table_schema` file branch behaves identically: it proposes
// `schemas/<table>.yaml` and seeds a unified skeleton keyed to the write target's
// `table`.
//
// Two deliberate behaviors the generator forces on us:
//  • A token-bearing (`${…}`/`{{…}}`/`%{…}`) or absent `table` yields NO path
//    proposal AND NO `table:` prefill — the generator compares the file's declared
//    `table`/`name` against the SUBSTITUTED write target, so seeding a raw token
//    guarantees a CFG-068 mismatch warning (a plain comment is safe instead).
//  • The skeleton NEVER seeds `tags: {}` — a managed-empty map deletes every
//    table tag under `remove_undeclared_tags`. The `tags:` example stays
//    commented so opting one in is an explicit choice — intentional guidance,
//    not a bug.

import type { YamlPath } from '@/lib/flowgroup-doc'
import { isSubstitutionToken, readPath } from './helpers'

const TABLE_PATH: YamlPath = ['write_target', 'table']

/**
 * The write target's `table` as a clean, tokenless string, or `null`. Mirrors
 * `companionCheckablePath`'s token detection (via `isSubstitutionToken`).
 */
export function schemaFileTableName(raw: Record<string, unknown>): string | null {
  const value = readPath(raw, TABLE_PATH)
  if (typeof value !== 'string') return null
  const table = value.trim()
  if (table === '') return null
  if (isSubstitutionToken(table)) return null
  return table
}

/** Proposed file path `schemas/<table>.yaml`, or `null` when no clean table. */
export function schemaSuggestPath(raw: Record<string, unknown>): string | null {
  const table = schemaFileTableName(raw)
  return table === null ? null : `schemas/${table}.yaml`
}

/**
 * The skeleton the New/Create affordance seeds. `table:` is prefilled when the
 * write target's `table` is a clean string; otherwise a comment stands in (never
 * a raw token — see the module note). The file serves both the `table_schema`
 * and `tags_file` readers, so it declares `columns:` (name/type) with a
 * commented-out per-column `tags:` example.
 */
export function schemaStub(raw: Record<string, unknown>): string {
  const table = schemaFileTableName(raw)
  const tableLine =
    table === null ? "table:  # set to the write target's table name" : `table: ${table}`
  return `${tableLine}
columns:
  - name: example_column
    type: STRING
    # tags:
    #   pii: high
`
}
